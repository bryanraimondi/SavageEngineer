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

# ---------- Token helpers (suffix-insensitive, punctuation-safe) ----------
_SPLIT_RE = re.compile(r"[.\-_]")  # split on first '.', '-', or '_'
_STRIP_PUNCT = re.compile(r'^[\s"\'\(\)\[\]\{\}:;,]+|[\s"\'\(\)\[\]\{\}:;,]+$')

def normalize_base(token: str) -> str:
    """Trim stray punctuation and return base before first . - or _ (lowercased)."""
    if not token:
        return ""
    cleaned = _STRIP_PUNCT.sub("", token)
    if not cleaned:
        return ""
    base = _SPLIT_RE.split(cleaned, 1)[0]
    return base.strip().lower()

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name).strip()

def uniquify_path(path: str) -> str:
    base, ext = os.path.splitext(path)
    i = 1
    out = path
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
        for cell in row:
            if str(cell).strip().lower() in target_labels:
                header_row_idx = i
                break
        if header_row_idx is not None:
            break
    if header_row_idx is None:
        return None
    header = df.iloc[header_row_idx].tolist()
    data = df.iloc[header_row_idx + 1:].reset_index(drop=True)
    data.columns = header
    data = data.dropna(axis=1, how='all')
    return data

def extract_ecs_codes_from_df(df):
    """
    Return:
      ecs_set         -> set of lowercased ECS codes
      original_map    -> dict lowercased -> exemplar (original casing from Excel)
    We split cells by common delimiters and keep tokens that contain letters+digits and no spaces.
    """
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
        low = t.lower()
        if low not in ecs_set:
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

def highlight_to_temp(pdf_path, ecs_compare_set, cancel_flag, on_match, ignore_leading_digit, out_dir):
    """
    Open a PDF, add highlights for the FIRST occurrence per ECS base (suffix-insensitive).
    - Matching uses ecs_compare_set; if ignore_leading_digit=True, a leading digit
      on the PDF token base is ignored.
    - Writes an annotated TEMP PDF (in output dir) *only if there are hits* and returns its path.
    Returns:
      hits (int),
      matched_bases (set[str])  -> always the *comparison* base used,
      tmp_path (str or None),
      hit_pages_sorted (list[int]) -> 0-based page indices with at least one hit,
      total_pages (int)
    """
    doc = fitz.open(pdf_path)
    hits = 0
    matched_bases = set()
    highlighted_bases = set()
    hit_pages = set()

    try:
        for page in doc:
            if cancel_flag.is_set():
                break
            page_hits = 0
            for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
                if cancel_flag.is_set():
                    break
                tok = (wtext or "").strip()
                if not tok:
                    continue
                base = normalize_base(tok)
                if not base:
                    continue
                # Optionally ignore a single leading digit on the PDF token base
                cmp_base = base[1:] if (ignore_leading_digit and base and base[0].isdigit()) else base
                if cmp_base and (cmp_base in ecs_compare_set) and (cmp_base not in highlighted_bases):
                    rect = fitz.Rect(x0, y0, x1, y1)
                    ann = page.add_highlight_annot(rect)
                    ann.update()
                    hits += 1
                    page_hits += 1
                    highlighted_bases.add(cmp_base)
                    matched_bases.add(cmp_base)
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
        "total_pages": int
        "keep_pages": set[int] or None  # if review selected; 0-based
      }
    If review not used, keep_pages is None.
    If only_highlighted_pages is True:
        insert only keep_pages (or hit_pages if keep_pages is None).
    Else:
        insert *all* pages of tmp_path (full annotated doc).
    """
    out = fitz.open()
    try:
        for item in selections:
            if not item["tmp_path"]:
                continue
            src = fitz.open(item["tmp_path"])
            try:
                if only_highlighted_pages:
                    pages = sorted(list(item["keep_pages"])) if item.get("keep_pages") else item["hit_pages"]
                    if pages:
                        out.insert_pdf(src, page_numbers=pages)
                else:
                    # full document (all pages), because user wants headers etc.
                    out.insert_pdf(src)
            finally:
                src.close()
        out_path = uniquify_path(out_path)
        out.save(out_path)
        return out_path
    finally:
        out.close()

# ---------- Review dialog (select pages to keep) ----------
class ReviewDialog(tk.Toplevel):
    """
    Simple modal dialog that lists (File, Page) rows for pages that had highlights.
    Double-click a row toggles keep/remove. Buttons: Select All / Clear / OK / Cancel.
    After OK, caller reads self.selection which maps tmp_path -> set(0-based page indexes to keep).
    """
    def __init__(self, master, items):
        super().__init__(master)
        self.title("Review highlighted pages to keep")
        self.geometry("720x420")
        self.resizable(True, True)
        self.transient(master)
        self.grab_set()

        # items is a list of dicts: {"display":file, "tmp_path":..., "hit_pages":[...]}
        self.tree = ttk.Treeview(self, columns=("keep", "file", "page"), show="headings", selectmode="extended")
        self.tree.heading("keep", text="Keep")
        self.tree.heading("file", text="File")
        self.tree.heading("page", text="Page")
        self.tree.column("keep", width=60, anchor="center")
        self.tree.column("file", width=480, anchor="w")
        self.tree.column("page", width=80, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)

        # keep map: tmp_path -> set(pages)
        self.keep_map = {}
        self._row_mapping = {}  # iid -> (tmp_path, page)

        for it in items:
            tmp = it["tmp_path"]
            disp = it["display"]
            self.keep_map[tmp] = set(it["hit_pages"])
            for p in it["hit_pages"]:
                iid = self.tree.insert("", "end", values=("[x]", disp, p + 1))
                self._row_mapping[iid] = (tmp, p)

        self.tree.bind("<Double-1>", self._toggle_keep)

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(0,8))
        ttk.Button(btns, text="Select All", command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Clear All", command=self._clear_all).pack(side="left", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)

        self.selection = None  # set after OK

    def _toggle_keep(self, event):
        iid = self.tree.identify_row(event.y)
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
        self.destroy()

    def _cancel(self):
        self.selection = None
        self.destroy()

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

        self.only_highlighted_var = tk.BooleanVar(value=True)  # 1) only print highlighted pages
        self.review_pages_var = tk.BooleanVar(value=True)      # 2) review pages to keep
        self.ignore_lead_digit_var = tk.BooleanVar(value=False) # 3) ignore leading digit option

        self.pdf_list = []
        self.cancel_flag = threading.Event()
        self.worker_thread = None
        self.msg_queue = queue.Queue()
        self.ecs_original_map = {}
        self.row_to_fullpath = {}  # for double-click open

        self._build_ui()
        self._poll_messages()

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        # Row 0: Week + Building
        fr_top = ttk.Frame(self)
        fr_top.pack(fill="x", **pad)
        ttk.Label(fr_top, text="Week Number (e.g., 34):").pack(side="left")
        self.ent_week = ttk.Entry(fr_top, width=10, textvariable=self.week_number)
        self.ent_week.pack(side="left", padx=8)
        ttk.Label(fr_top, text="Building Name:").pack(side="left", padx=(16, 0))
        self.ent_bldg = ttk.Entry(fr_top, width=30, textvariable=self.building_name)
        self.ent_bldg.pack(side="left", padx=8, fill="x", expand=True)

        # Row 1: Options
        fr_opts = ttk.Frame(self)
        fr_opts.pack(fill="x", **pad)
        ttk.Checkbutton(fr_opts, text="Only keep highlighted pages", variable=self.only_highlighted_var).pack(side="left")
        ttk.Checkbutton(fr_opts, text="Review pages before saving", variable=self.review_pages_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Ignore leading digit in PDF codes (e.g., 1HLX… → HLX…)", variable=self.ignore_lead_digit_var).pack(side="left", padx=12)

        # Row 2: Excel picker
        fr_excel = ttk.Frame(self)
        fr_excel.pack(fill="x", **pad)
        ttk.Label(fr_excel, text="Excel (ECS Codes):").pack(side="left")
        self.ent_excel = ttk.Entry(fr_excel, textvariable=self.excel_path)
        self.ent_excel.pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_excel, text="Browse…", command=self._pick_excel).pack(side="left")

        # Row 3: PDFs list + buttons
        fr_pdfs = ttk.LabelFrame(self, text="PDFs to Process")
        fr_pdfs.pack(fill="both", expand=True, **pad)

        btns = ttk.Frame(fr_pdfs)
        btns.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns, text="Add PDFs…", command=self._add_pdfs).pack(side="left")
        ttk.Button(btns, text="Remove Selected", command=self._remove_selected).pack(side="left", padx=6)
        ttk.Button(btns, text="Clear List", command=self._clear_list).pack(side="left")

        self.lst_pdfs = tk.Listbox(fr_pdfs, height=7, selectmode=tk.EXTENDED)
        self.lst_pdfs.pack(fill="both", expand=True, padx=6, pady=(0,6))

        # Row 4: Output folder
        fr_out = ttk.Frame(self)
        fr_out.pack(fill="x", **pad)
        ttk.Label(fr_out, text="Output Folder:").pack(side="left")
        self.ent_out = ttk.Entry(fr_out, textvariable=self.output_dir)
        self.ent_out.pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_out, text="Select…", command=self._pick_output_dir).pack(side="left")

        # Row 5: Match log (Treeview + dbl-click to open)
        fr_log = ttk.LabelFrame(self, text="Matches (ECS Code | File | Page) — double-click to open file")
        fr_log.pack(fill="both", expand=True, **pad)

        cols = ("code", "file", "page")
        self.tree = ttk.Treeview(fr_log, columns=cols, show="headings", height=10)
        for c, w in zip(cols, (260, 540, 70)):
            self.tree.heading(c, text=c.capitalize())
            self.tree.column(c, width=w, anchor="w" if c != "page" else "center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)
        self.tree.bind("<Double-1>", self._open_row_pdf)

        # Row 6: Progress + Controls
        fr_prog = ttk.Frame(self)
        fr_prog.pack(fill="x", **pad)
        self.prog = ttk.Progressbar(fr_prog, orient="horizontal", mode="determinate", maximum=100)
        self.prog.pack(side="left", expand=True, fill="x")
        self.lbl_status = ttk.Label(fr_prog, text="Idle")
        self.lbl_status.pack(side="left", padx=8)

        fr_btns = ttk.Frame(self)
        fr_btns.pack(fill="x", **pad)
        self.btn_start = ttk.Button(fr_btns, text="Start", command=self._start)
        self.btn_start.pack(side="left")
        self.btn_stop = ttk.Button(fr_btns, text="Stop", command=self._stop, state="disabled")
        self.btn_stop.pack(side="left", padx=6)
        ttk.Button(fr_btns, text="Exit", command=self._exit).pack(side="right")

    # ----- UI actions -----
    def _pick_excel(self):
        path = filedialog.askopenfilename(
            title="Select Excel with ECS Codes",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if path:
            self.excel_path.set(path)

    def _add_pdfs(self):
        paths = filedialog.askopenfilenames(
            title="Select PDFs",
            filetypes=[("PDF files", "*.pdf")]
        )
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
        if not week:
            messagebox.showwarning("Week Number", "Please enter a week number (e.g., 34).")
            return
        bldg = self.building_name.get().strip()
        if not bldg:
            messagebox.showwarning("Building Name", "Please enter the building name.")
            return
        excel = self.excel_path.get().strip()
        if not excel or not os.path.exists(excel):
            messagebox.showwarning("Excel", "Please select a valid Excel file.")
            return
        if not self.pdf_list:
            messagebox.showwarning("PDFs", "Please add at least one PDF.")
            return

        out_dir = self.output_dir.get().strip()
        if not out_dir:
            out_dir = os.path.dirname(self.pdf_list[0])
            self.output_dir.set(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        # reset UI
        self.cancel_flag.clear()
        self.prog["value"] = 0
        self.lbl_status.config(text="Starting…")
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        for i in self.tree.get_children():
            self.tree.delete(i)
        self.row_to_fullpath.clear()

        # spawn worker
        args = (
            week,
            bldg,
            excel,
            list(self.pdf_list),
            out_dir,
            bool(self.ignore_lead_digit_var.get()),
        )
        self.worker_thread = threading.Thread(target=self._worker, args=args, daemon=True)
        self.worker_thread.start()

    def _stop(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.cancel_flag.set()
            self.lbl_status.config(text="Stopping…")

    def _exit(self):
        if self.worker_thread and self.worker_thread.is_alive():
            if not messagebox.askyesno("Exit", "Processing is still running. Stop and exit?"):
                return
            self.cancel_flag.set()
        self.destroy()

    # ----- Open PDF on double-click match row -----
    def _open_row_pdf(self, event):
        iid = self.tree.identify_row(event.y)
        if not iid:
            return
        vals = self.tree.item(iid, "values")
        if not vals or len(vals) < 2:
            return
        file_display = vals[1]
        fullpath = self.row_to_fullpath.get(iid)
        try:
            os.startfile(fullpath or file_display)  # Windows
        except Exception:
            try:
                # mac / linux fallback
                if sys.platform == "darwin":
                    os.system(f'open "{fullpath or file_display}"')
                else:
                    os.system(f'xdg-open "{fullpath or file_display}"')
            except Exception as e:
                messagebox.showerror("Open File", f"Could not open file:\n{e}")

    # ----- Background worker -----
    def _worker(self, week_number, building_name, excel_path, pdf_paths, out_dir, ignore_leading_digit):
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

            # Comparison set (with/without leading digit)
            ecs_compare = build_ecs_compare_set(ecs, ignore_leading_digit)

            total = len(pdf_paths)
            done = 0
            overall_matched_bases = set()

            bldg_tag = sanitize_filename(building_name)
            combined_base = os.path.join(out_dir, f"{bldg_tag}_Highlighted_WK{week_number}.pdf")
            combined_out_path = uniquify_path(combined_base)

            # Collect temp annotated files + hit pages for optional review
            processed = []  # list of dicts per file
            for pdf in pdf_paths:
                if self.cancel_flag.is_set():
                    post("status", "Cancelled.")
                    break

                post("status", f"Processing: {os.path.basename(pdf)}")
                hits, matched, tmp_path, hit_pages, total_pages = highlight_to_temp(
                    pdf_path=pdf,
                    ecs_compare_set=ecs_compare,
                    cancel_flag=self.cancel_flag,
                    on_match=on_match,
                    ignore_leading_digit=ignore_leading_digit,
                    out_dir=out_dir
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

                done += 1
                post("progress", int((done / total) * 100))

            # Prepare data for review / combination
            if self.cancel_flag.is_set():
                post("done", None)
                return

            # Post to UI for review/combination stage
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
                    self.prog["value"] = int(payload)

                elif msg_type == "match":
                    code, file_name, page_num = payload
                    iid = self.tree.insert("", "end", values=(code, file_name, page_num))
                    # store a best-effort full path for open; look up by file_name in current list
                    full = next((p for p in self.pdf_list if os.path.basename(p) == file_name), None)
                    if full:
                        self.row_to_fullpath[iid] = full

                elif msg_type == "error":
                    self.lbl_status.config(text="Error")
                    messagebox.showerror("Error", str(payload))

                elif msg_type == "review_data":
                    # Finalize: optional review and combine + CSV for missing codes
                    self._finalize_and_save(payload)

                elif msg_type == "done":
                    # background worker is finished
                    self.btn_start.config(state="normal")
                    self.btn_stop.config(state="disabled")
                    if not self.cancel_flag.is_set():
                        # keep whatever status is current
                        pass
                    else:
                        self.lbl_status.config(text="Stopped.")

        except queue.Empty:
            pass

        self.after(80, self._poll_messages)

    # ----- finalize: review pages + combine + CSV of missing -----
    def _finalize_and_save(self, bundle):
        processed = bundle["processed"]               # list of {display, src_full, tmp_path, hit_pages, total_pages}
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

        # Optionally review highlighted pages and select which to keep
        keep_map = {}  # tmp_path -> set(0-based pages to keep)
        if self.review_pages_var.get():
            items = [{"display": p["display"], "tmp_path": p["tmp_path"], "hit_pages": p["hit_pages"]} for p in processed]
            dlg = ReviewDialog(self, items)
            self.wait_window(dlg)
            if dlg.selection is None:
                self.lbl_status.config(text="Review canceled.")
                return
            keep_map = dlg.selection
        else:
            # Default: keep all highlighted pages
            keep_map = {p["tmp_path"]: set(p["hit_pages"]) for p in processed}

        # Build selections for combiner
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

        # 4) CSV of values not found (using comparison set)
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
        messagebox.showerror("Fatal Error", str(e))
        sys.exit(1)
