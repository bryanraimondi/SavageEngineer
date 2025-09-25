import os
import re
import sys
import uuid
import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import fitz  # PyMuPDF
import pandas as pd

# ---------- Dash handling + token helpers ----------
DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2212"  # -, ‐, -, ‒, –, —, −
# Strip edge punctuation (including various dashes) but keep internal hyphens within codes:
_STRIP_PUNCT = re.compile(r'^[\s"\'()\[\]{}:;,.–—\-]+|[\s"\'()\[\]{}:;,.–—\-]+$')

def unify_dashes(s: str) -> str:
    """Normalize all dash-like characters to ASCII '-' and drop soft hyphen."""
    if not s:
        return s
    for ch in DASH_CHARS[1:]:
        s = s.replace(ch, "-")
    return s.replace("\u00AD", "")

def normalize_base(token: str) -> str:
    """Lowercase, trim edge punctuation, and unify dashes."""
    if not token:
        return ""
    cleaned = _STRIP_PUNCT.sub("", token)
    if not cleaned:
        return ""
    cleaned = unify_dashes(cleaned)
    return cleaned.strip().lower()

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name).strip()

def uniquify_path(path: str) -> str:
    base, ext = os.path.splitext(path)
    out = path
    i = 1
    while os.path.exists(out):
        out = f"{base} ({i}){ext}"
        i += 1
    return out

# ---------- Excel parsing ----------
def load_table_with_dynamic_header(xlsx_path, sheet_name=None):
    """
    Find the row that contains 'ECS Codes' / 'ECS Code' and treat it as the header.
    Returns a DataFrame with proper headers (or None if not found).
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, dtype=str)
    target_labels = {"ecs codes", "ecs code"}
    header_row_idx = None
    for i in range(len(df)):
        row = df.iloc[i].astype(str)
        if any(str(cell).strip().lower() in target_labels for cell in row):
            header_row_idx = i
            break
    if header_row_idx is None:
        return None
    header = df.iloc[header_row_idx].tolist()
    data = df.iloc[header_row_idx + 1:].reset_index(drop=True)
    data.columns = header
    data = data.dropna(axis=1, how='all')
    return data

def extract_ecs_codes_from_df(df):
    """Return (ecs_set_lower, original_map_lower_to_original)."""
    if df is None or df.empty:
        return set(), {}
    cols = [c for c in df.columns if str(c).strip().lower() in ("ecs codes", "ecs code")]
    if not cols:
        return set(), {}
    raw = []
    for c in cols:
        raw += df[c].dropna().astype(str).tolist()

    tokens = []
    for v in raw:
        parts = re.split(r"[,\n;/\t ]+", v)
        for p in parts:
            t = p.strip().strip('"\'' )
            if t and re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                tokens.append(t)

    ecs_set = set()
    original_map = {}
    for t in tokens:
        low = normalize_base(t)
        if low and (low not in ecs_set):
            ecs_set.add(low)
            original_map[low] = t
    return ecs_set, original_map

# ---------- PDF ops ----------
def build_ecs_compare_set(ecs_lower_set, ignore_leading_digit):
    """
    If ignore_leading_digit is True, also add versions of ECS codes without a single leading digit.
    """
    if not ignore_leading_digit:
        return set(ecs_lower_set)
    comp = set(ecs_lower_set)
    for code in ecs_lower_set:
        if code and code[0].isdigit():
            comp.add(code[1:])
    return comp

def highlight_to_temp(pdf_path, ecs_compare_set, cancel_flag, on_match,
                      ignore_leading_digit, out_dir, highlight_all_occurrences=False):
    """
    Annotates matches in a copy of the PDF saved to out_dir (only if there are matches).
    Returns:
      hits (int),
      matched_bases (set[str]),
      tmp_path (str or None),
      hit_pages_sorted (list[int])  # 0-based
      total_pages (int)
    """
    doc = fitz.open(pdf_path)
    hits = 0
    matched_bases = set()
    highlighted_bases = set()  # "first occurrence per code per PDF" guard (overridden if highlight_all_occurrences)
    hit_pages = set()

    try:
        for page in doc:
            if cancel_flag.is_set():
                break
            page_hits = 0

            # walk the words
            for (x0, y0, x1, y1, wtext, *_rest) in page.get_text("words", sort=True):
                if cancel_flag.is_set():
                    break
                tok = (wtext or "").strip()
                if not tok:
                    continue

                base = normalize_base(tok)
                if not base:
                    continue

                cmp_base = base[1:] if (ignore_leading_digit and base[0:1].isdigit()) else base

                if cmp_base and (cmp_base in ecs_compare_set) and (highlight_all_occurrences or (cmp_base not in highlighted_bases)):
                    # Prefer literal search for nicer rectangles; fallback to word box
                    rects = page.search_for(wtext) or []
                    rect = rects[0] if rects else fitz.Rect(x0, y0, x1, y1)

                    ann = page.add_highlight_annot(rect)
                    ann.update()

                    hits += 1
                    page_hits += 1
                    matched_bases.add(cmp_base)
                    highlighted_bases.add(cmp_base)
                    on_match(cmp_base, os.path.basename(pdf_path), page.number + 1)

            if page_hits > 0:
                hit_pages.add(page.number)

        if hits > 0 and not cancel_flag.is_set():
            tmp_path = os.path.join(out_dir, f"__tmp_annot_{uuid.uuid4().hex}.pdf")
            doc.save(tmp_path)
            return hits, matched_bases, tmp_path, sorted(hit_pages), doc.page_count
        else:
            return hits, matched_bases, None, [], doc.page_count
    finally:
        doc.close()

def combine_from_selection(out_path, selections, only_highlighted_pages):
    """
    selections: list of dicts:
      {
        "tmp_path": str,
        "hit_pages": list[int],  # 0-based
        "total_pages": int,
        "keep_pages": set[int] or None
      }
    If review not used, keep_pages is None.
    If only_highlighted_pages is True:
        insert only keep_pages (or hit_pages if keep_pages is None).
    Else:
        insert full annotated docs.
    """
    out = fitz.open()
    try:
        for item in selections:
            tmp = item.get("tmp_path")
            if not tmp:
                continue
            with fitz.open(tmp) as src:
                if only_highlighted_pages:
                    pages = sorted(list(item.get("keep_pages") or item.get("hit_pages") or []))
                    if not pages:
                        continue
                    # Newer PyMuPDF supports pages=[...]; older needs per-page
                    try:
                        out.insert_pdf(src, pages=pages)
                    except TypeError:
                        for p in pages:
                            out.insert_pdf(src, from_page=p, to_page=p)
                else:
                    out.insert_pdf(src)

        out_path = uniquify_path(out_path)
        out.save(out_path)
        return out_path
    finally:
        out.close()

# ---------- Review dialog ----------
class ReviewDialog(tk.Toplevel):
    def __init__(self, master, items):
        super().__init__(master)
        self.title("Review highlighted pages to keep")
        self.geometry("1100x700")
        self.minsize(980, 600)
        self.transient(master)
        self.grab_set()

        # ===== LAYOUT =====
        wrapper = ttk.Frame(self)
        wrapper.pack(fill="both", expand=True, padx=8, pady=8)

        left = ttk.Frame(wrapper)
        left.pack(side="left", fill="both", expand=True)
        right = ttk.Frame(wrapper)
        right.pack(side="right", fill="both", expand=True, padx=(8, 0))

        ttk.Label(left, text="Pages (double-click to toggle keep):").pack(anchor="w")

        self.tree = ttk.Treeview(left, columns=("keep", "file", "page"),
                                 show="headings", selectmode="browse", height=22)
        self.tree.heading("keep", text="Keep")
        self.tree.heading("file", text="File")
        self.tree.heading("page", text="Page")
        self.tree.column("keep", width=60, anchor="center")
        self.tree.column("file", width=520, anchor="w")
        self.tree.column("page", width=70, anchor="center")
        self.tree.pack(fill="both", expand=True)

        # Keep state
        self.keep_map: dict[str, set[int]] = {}
        self._row_mapping: dict[str, tuple[str, int]] = {}  # iid -> (tmp_path, page_index)

        for it in items:
            tmp = it["tmp_path"]
            disp = it["display"]
            self.keep_map[tmp] = set(it["hit_pages"])
            for p in it["hit_pages"]:
                iid = self.tree.insert("", "end", values=("[x]", disp, p + 1))
                self._row_mapping[iid] = (tmp, p)

        # ===== PREVIEW =====
        ttk.Label(right, text="Preview").pack(anchor="w")

        # Scrollable canvas
        canvas_frame = ttk.Frame(right)
        canvas_frame.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(canvas_frame, bg="#202020", highlightthickness=0)
        xscroll = ttk.Scrollbar(canvas_frame, orient="horizontal", command=self.canvas.xview)
        yscroll = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)

        self.canvas.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        canvas_frame.rowconfigure(0, weight=1)
        canvas_frame.columnconfigure(0, weight=1)

        self._preview_img = None      # keep reference to PhotoImage
        self._tmp_png_path = None     # last temp image path
        self._zoom = 1.25

        # Controls
        controls = ttk.Frame(right)
        controls.pack(fill="x", pady=(6, 0))
        ttk.Button(controls, text="Zoom -", command=lambda: self._change_zoom(-0.15)).pack(side="left")
        ttk.Button(controls, text="Zoom +", command=lambda: self._change_zoom(+0.15)).pack(side="left", padx=6)
        self.stat = ttk.Label(controls, text="—")
        self.stat.pack(side="right")

        # Buttons
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(6, 8))
        ttk.Button(btns, text="Select All", command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Clear All", command=self._clear_all).pack(side="left", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)

        # Bindings
        self.tree.bind("<Double-1>", self._toggle_keep)
        self.tree.bind("<<TreeviewSelect>>", self._preview_selected)

        # Auto-select first row to show a preview right away
        if self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._preview_selected()

        # Clean temp images on close
        self.protocol("WM_DELETE_WINDOW", self._cancel)

    # ===== Keep / selection logic =====
    def _toggle_keep(self, event=None):
        iid = self.tree.identify_row(event.y) if event else self.tree.focus()
        if not iid:
            return
        tmp, page = self._row_mapping[iid]
        if page in self.keep_map[tmp]:
            self.keep_map[tmp].remove(page)
            self.tree.set(iid, "keep", "[ ]")
        else:
            self.keep_map[tmp].add(page)
            self.tree.set(iid, "keep", "[x]")

    def _select_all(self):
        for iid, (tmp, page) in self._row_mapping.items():
            self.keep_map[tmp].add(page)
            self.tree.set(iid, "keep", "[x]")

    def _clear_all(self):
        for iid, (tmp, page) in self._row_mapping.items():
            self.keep_map[tmp].discard(page)
            self.tree.set(iid, "keep", "[ ]")

    def _ok(self):
        self.selection = self.keep_map
        self._cleanup_temp_image()
        self.destroy()

    def _cancel(self):
        self.selection = None
        self._cleanup_temp_image()
        self.destroy()

    # ===== Preview rendering =====
    def _change_zoom(self, delta):
        new_zoom = max(0.3, min(3.0, self._zoom + delta))
        if abs(new_zoom - self._zoom) > 1e-6:
            self._zoom = new_zoom
            self._preview_selected()

    def _preview_selected(self, event=None):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        tmp, page_idx = self._row_mapping[iid]
        self._render_page(tmp, page_idx)

    def _render_page(self, tmp_pdf, page_idx):
        self.stat.config(text=f"{os.path.basename(tmp_pdf)} — page {page_idx+1}")
        try:
            doc = fitz.open(tmp_pdf)
            page = doc.load_page(page_idx)
            mat = fitz.Matrix(self._zoom, self._zoom)
            # Try with annotations; fallback if older PyMuPDF lacks 'annots' parameter
            try:
                pix = page.get_pixmap(matrix=mat, alpha=False, annots=True)
            except TypeError:
                pix = page.get_pixmap(matrix=mat, alpha=False)
            # Save to a temp PNG and display
            self._cleanup_temp_image()
            fd, path = tempfile.mkstemp(suffix=".png", prefix="ecs_preview_")
            os.close(fd)
            pix.save(path)
            self._tmp_png_path = path

            img = tk.PhotoImage(file=path)
            self._preview_img = img
            self.canvas.delete("all")
            self.canvas.create_image(0, 0, anchor="nw", image=img)
            self.canvas.config(scrollregion=(0, 0, img.width(), img.height()))
        except Exception as e:
            self.canvas.delete("all")
            self.canvas.create_text(10, 10, anchor="nw", fill="white",
                                    text=f"Preview error:\n{e}")
        finally:
            try:
                doc.close()
            except Exception:
                pass

    def _cleanup_temp_image(self):
        try:
            if self._tmp_png_path and os.path.exists(self._tmp_png_path):
                os.remove(self._tmp_png_path)
        except Exception:
            pass
        finally:
            self._tmp_png_path = None


# ---------- GUI App ----------
class HighlighterApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ECS PDF Highlighter")
        self.geometry("980x720")
        self.minsize(960, 700)

        # State
        self.excel_path = tk.StringVar()
        self.week_number = tk.StringVar()
        self.building_name = tk.StringVar()
        self.output_dir = tk.StringVar()

        self.only_highlighted_var = tk.BooleanVar(value=True)
        self.review_pages_var = tk.BooleanVar(value=True)
        self.ignore_lead_digit_var = tk.BooleanVar(value=False)
        self.highlight_all_var = tk.BooleanVar(value=True)  # NEW

        self.pdf_list = []
        self.cancel_flag = threading.Event()
        self.worker_thread = None
        self.msg_queue = queue.Queue()
        self.ecs_original_map = {}
        self.row_to_fullpath = {}

        self._build_ui()
        self._poll_messages()

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        fr_top = ttk.Frame(self)
        fr_top.pack(fill="x", **pad)
        ttk.Label(fr_top, text="Week Number:").pack(side="left")
        ttk.Entry(fr_top, width=10, textvariable=self.week_number).pack(side="left", padx=8)
        ttk.Label(fr_top, text="Building Name:").pack(side="left", padx=(16, 0))
        ttk.Entry(fr_top, width=30, textvariable=self.building_name).pack(side="left", padx=8, fill="x", expand=True)

        fr_opts = ttk.Frame(self)
        fr_opts.pack(fill="x", **pad)
        ttk.Checkbutton(fr_opts, text="Only keep highlighted pages", variable=self.only_highlighted_var).pack(side="left")
        ttk.Checkbutton(fr_opts, text="Review pages before saving", variable=self.review_pages_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Ignore leading digit in PDF codes", variable=self.ignore_lead_digit_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Highlight every occurrence", variable=self.highlight_all_var).pack(side="left", padx=12)

        fr_excel = ttk.Frame(self)
        fr_excel.pack(fill="x", **pad)
        ttk.Label(fr_excel, text="Excel (ECS Codes):").pack(side="left")
        ttk.Entry(fr_excel, textvariable=self.excel_path).pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_excel, text="Browse…", command=self._pick_excel).pack(side="left")

        fr_pdfs = ttk.LabelFrame(self, text="PDFs to Process")
        fr_pdfs.pack(fill="both", expand=True, **pad)
        btns = ttk.Frame(fr_pdfs); btns.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns, text="Add PDFs…", command=self._add_pdfs).pack(side="left")
        ttk.Button(btns, text="Remove Selected", command=self._remove_selected).pack(side="left", padx=6)
        ttk.Button(btns, text="Clear List", command=self._clear_list).pack(side="left")
        self.lst_pdfs = tk.Listbox(fr_pdfs, height=7, selectmode=tk.EXTENDED)
        self.lst_pdfs.pack(fill="both", expand=True, padx=6, pady=(0,6))

        fr_out = ttk.Frame(self)
        fr_out.pack(fill="x", **pad)
        ttk.Label(fr_out, text="Output Folder:").pack(side="left")
        ttk.Entry(fr_out, textvariable=self.output_dir).pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_out, text="Select…", command=self._pick_output_dir).pack(side="left")

        fr_log = ttk.LabelFrame(self, text="Matches (ECS Code | File | Page)")
        fr_log.pack(fill="both", expand=True, **pad)
        cols = ("code", "file", "page")
        self.tree = ttk.Treeview(fr_log, columns=cols, show="headings", height=10)
        for c, w in zip(cols, (260, 540, 70)):
            self.tree.heading(c, text=c.capitalize())
            self.tree.column(c, width=w, anchor="w" if c != "page" else "center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)

        fr_prog = ttk.Frame(self)
        fr_prog.pack(fill="x", **pad)
        self.prog = ttk.Progressbar(fr_prog, orient="horizontal", mode="determinate", maximum=100)
        self.prog.pack(side="left", expand=True, fill="x")
        self.lbl_status = ttk.Label(fr_prog, text="Idle")
        self.lbl_status.pack(side="left", padx=8)

        fr_btns = ttk.Frame(self)
        fr_btns.pack(fill="x", **pad)
        ttk.Button(fr_btns, text="Start", command=self._start).pack(side="left")
        ttk.Button(fr_btns, text="Stop", command=self._stop).pack(side="left", padx=6)
        ttk.Button(fr_btns, text="Exit", command=self._exit).pack(side="right")

    # ----- UI actions -----
    def _pick_excel(self):
        path = filedialog.askopenfilename(title="Select Excel with ECS Codes",
                                          filetypes=[("Excel files", "*.xlsx *.xls")])
        if path:
            self.excel_path.set(path)

    def _add_pdfs(self):
        paths = filedialog.askopenfilenames(title="Select PDFs", filetypes=[("PDF files", "*.pdf")])
        if paths:
            for p in paths:
                if p not in self.pdf_list:
                    self.pdf_list.append(p)
                    self.lst_pdfs.insert("end", p)

    def _remove_selected(self):
        sels = list(self.lst_pdfs.curselection())[::-1]
        for i in sels:
            path = self.lst_pdfs.get(i)
            self.lst_pdfs.delete(i)
            try:
                self.pdf_list.remove(path)
            except ValueError:
                pass

    def _clear_list(self):
        self.lst_pdfs.delete(0, "end")
        self.pdf_list.clear()

    def _pick_output_dir(self):
        d = filedialog.askdirectory(title="Select Output Folder")
        if d:
            self.output_dir.set(d)

    # ----- Start/Stop/Exit -----
    def _start(self):
        if self.worker_thread and self.worker_thread.is_alive():
            return
        week = self.week_number.get().strip()
        bldg = self.building_name.get().strip()
        excel = self.excel_path.get().strip()
        if not week or not bldg or not excel or not os.path.exists(excel) or not self.pdf_list:
            messagebox.showwarning("Input", "Please fill in week, building, Excel, and PDFs.")
            return

        out_dir = self.output_dir.get().strip() or os.path.dirname(self.pdf_list[0])
        self.output_dir.set(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        self.cancel_flag.clear()
        self.prog["value"] = 0
        self.lbl_status.config(text="Starting…")

        args = (
            week, bldg, excel, list(self.pdf_list), out_dir,
            bool(self.ignore_lead_digit_var.get()),
            bool(self.highlight_all_var.get())
        )
        self.worker_thread = threading.Thread(target=self._worker, args=args, daemon=True)
        self.worker_thread.start()

    def _stop(self):
        self.cancel_flag.set()
        self.lbl_status.config(text="Stopping…")

    def _exit(self):
        self.destroy()

    # ----- Worker -----
    def _worker(self, week_number, building_name, excel_path, pdf_paths,
                out_dir, ignore_leading_digit, highlight_all_occurrences):
        def post(msg_type, payload=None):
            self.msg_queue.put((msg_type, payload))
        def on_match(base_lower, file_name, page_num):
            pretty = self.ecs_original_map.get(base_lower, base_lower)
            post("match", (pretty, file_name, page_num))
        try:
            post("status", "Reading Excel…")
            df = load_table_with_dynamic_header(excel_path, sheet_name=0)
            if df is None:
                post("error", "Could not find a header row containing 'ECS Codes' or 'ECS Code' in the Excel.")
                return
            ecs, original_map = extract_ecs_codes_from_df(df)
            if not ecs:
                post("error", "No ECS codes found under 'ECS Codes'/'ECS Code'.")
                return
            self.ecs_original_map = original_map
            ecs_compare = build_ecs_compare_set(ecs, ignore_leading_digit)

            processed = []
            overall_matched_bases = set()

            total = len(pdf_paths) if pdf_paths else 1
            for idx, pdf in enumerate(pdf_paths, start=1):
                if self.cancel_flag.is_set():
                    break
                post("status", f"Processing: {os.path.basename(pdf)}")
                hits, matched, tmp_path, hit_pages, total_pages = highlight_to_temp(
                    pdf_path=pdf,
                    ecs_compare_set=ecs_compare,
                    cancel_flag=self.cancel_flag,
                    on_match=on_match,
                    ignore_leading_digit=ignore_leading_digit,
                    out_dir=out_dir,
                    highlight_all_occurrences=highlight_all_occurrences
                )
                if hits > 0 and not self.cancel_flag.is_set() and tmp_path:
                    overall_matched_bases |= matched
                    processed.append({
                        "display": os.path.basename(pdf),
                        "src_full": pdf,
                        "tmp_path": tmp_path,
                        "hit_pages": hit_pages,
                        "total_pages": total_pages
                    })
                    post("status", f"Annotated: {os.path.basename(pdf)} (hits: {hits})")
                else:
                    post("status", f"No highlights: {os.path.basename(pdf)}")

                post("progress", int((idx / total) * 100))

            if self.cancel_flag.is_set():
                post("done", None)
                return

            # pass to finalize
            bldg_tag = sanitize_filename(building_name)
            combined_base = os.path.join(out_dir, f"{bldg_tag}_Highlighted_WK{week_number}.pdf")
            combined_out_path = uniquify_path(combined_base)

            post("review_data", {
                "processed": processed,
                "overall_matched_bases": list(overall_matched_bases),
                "ecs_compare": list(ecs_compare),
                "combined_out_path": combined_out_path,
                "building_name": building_name,
                "week_number": week_number,
                "out_dir": out_dir
            })
        except Exception as e:
            post("error", f"Unexpected error: {e}")
        finally:
            post("done", None)

    # ----- UI message pump -----
    def _poll_messages(self):
        try:
            while True:
                msg_type, payload = self.msg_queue.get_nowait()

                if msg_type == "status":
                    self.lbl_status.config(text=str(payload))

                elif msg_type == "progress":
                    try:
                        self.prog["value"] = int(payload)
                    except Exception:
                        pass

                elif msg_type == "match":
                    code, file_name, page_num = payload
                    self.tree.insert("", "end", values=(code, file_name, page_num))

                elif msg_type == "error":
                    self.lbl_status.config(text="Error")
                    messagebox.showerror("Error", str(payload))

                elif msg_type == "review_data":
                    self._finalize_and_save(payload)

                elif msg_type == "done":
                    # re-enable buttons when worker ends
                    pass

        except queue.Empty:
            pass

        # continue polling
        self.after(80, self._poll_messages)

    # ----- finalize: review pages + combine + CSV of missing -----
    def _finalize_and_save(self, bundle):
        processed = bundle["processed"]
        overall_matched_bases = set(bundle["overall_matched_bases"])
        ecs_compare = set(bundle["ecs_compare"])
        combined_out_path = bundle["combined_out_path"]
        building_name = bundle["building_name"]
        week_number = bundle["week_number"]
        out_dir = bundle["out_dir"]

        if not processed:
            messagebox.showinfo("No Highlights", "No reports had highlights; no combined file created.")
            self.lbl_status.config(text="No highlighted reports.")
            return

        # Review dialog if enabled
        if self.review_pages_var.get():
            items = [{"display": p["display"], "tmp_path": p["tmp_path"], "hit_pages": p["hit_pages"]}
                     for p in processed]
            dlg = ReviewDialog(self, items)
            self.wait_window(dlg)
            if dlg.selection is None:
                self.lbl_status.config(text="Review canceled.")
                return
            keep_map = dlg.selection
        else:
            keep_map = {p["tmp_path"]: set(p["hit_pages"]) for p in processed}

        only_highlighted = bool(self.only_highlighted_var.get())
        selections = []
        for p in processed:
            selections.append({
                "tmp_path": p["tmp_path"],
                "hit_pages": p["hit_pages"],
                "total_pages": p["total_pages"],
                "keep_pages": keep_map.get(p["tmp_path"], set(p["hit_pages"]))
            })

        # Combine and save
        try:
            final_path = combine_from_selection(
                out_path=combined_out_path,
                selections=selections,
                only_highlighted_pages=only_highlighted
            )
            if final_path:
                self.lbl_status.config(text=f"Combined saved: {os.path.basename(final_path)}")
                messagebox.showinfo("Done", f"Combined PDF saved:\n{final_path}")
        except Exception as e:
            messagebox.showerror("Combine", f"Could not save combined PDF:\n{e}")
            self.lbl_status.config(text="Combine failed.")
            return

        # CSV of not-found codes
        try:
            missing = sorted(list(ecs_compare - overall_matched_bases))
            if missing:
                bldg_tag = sanitize_filename(building_name)
                csv_path = os.path.join(out_dir, f"{bldg_tag}_NotSurveyed_WK{week_number}.csv")
                csv_path = uniquify_path(csv_path)
                pd.DataFrame({"ECS_Code_Not_Found": missing}).to_csv(csv_path, index=False)
                self.lbl_status.config(text=f"CSV saved: {os.path.basename(csv_path)}")
        except Exception as e:
            messagebox.showwarning("CSV", f"Could not save NotSurveyed CSV:\n{e}")

# ---------- main ----------
if __name__ == "__main__":
    try:
        app = HighlighterApp()
        app.mainloop()
    except Exception as e:
        try:
            messagebox.showerror("Fatal Error", str(e))
        except Exception:
            pass
        sys.exit(1)
