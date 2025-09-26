import os
import re
import sys
import base64
import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import fitz  # PyMuPDF
import pandas as pd
from collections import defaultdict

# ---------- Dash handling + token helpers ----------
DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2212"  # -, ‐, -, ‒, –, —, −
_STRIP_EDGE_PUNCT = re.compile(r'^[\s"\'()\[\]{}:;,.–—\-]+|[\s"\'()\[\]{}:;,.–—\-]+$')

def unify_dashes(s: str) -> str:
    if not s:
        return s
    for ch in DASH_CHARS[1:]:
        s = s.replace(ch, "-")
    return s.replace("\u00AD", "")  # soft hyphen

def normalize_base(token: str) -> str:
    """Lowercase + normalize dashes; keep inner hyphens."""
    if not token:
        return ""
    cleaned = _STRIP_EDGE_PUNCT.sub("", token)
    if not cleaned:
        return ""
    cleaned = unify_dashes(cleaned)
    return cleaned.strip().lower()

def normalize_nosep(token: str) -> str:
    """Lowercase + normalize dashes + remove ALL non-alphanumerics."""
    if not token:
        return ""
    token = unify_dashes(token).lower()
    return re.sub(r'[^0-9a-z]', '', token)

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
    """Return (ecs_primary_set_lower, original_map_lower_to_original_string)."""
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

# ---------- Build compare keys & alias map (no separators) ----------
def build_compare_index(ecs_primary, ignore_leading_digit):
    """
    Returns:
      cmp_keys_nosep: set of canonical keys (no separators) to match against
      nosep_to_primary: dict mapping cmp_key_nosep -> primary key from Excel
      max_code_len: longest cmp_key length (for pruning)
    """
    cmp_keys = set()
    nosep_to_primary = {}
    for primary in ecs_primary:
        k = normalize_nosep(primary)
        if k:
            cmp_keys.add(k)
            nosep_to_primary[k] = primary
        if ignore_leading_digit and primary and primary[0].isdigit():
            k2 = normalize_nosep(primary[1:])
            if k2:
                cmp_keys.add(k2)
                nosep_to_primary[k2] = primary
    max_code_len = max((len(k) for k in cmp_keys), default=0)
    return cmp_keys, nosep_to_primary, max_code_len

# ---------- Robust PDF scan: sliding window across words ----------
def scan_pdf_for_rects(pdf_path,
                       cmp_keys_nosep,
                       max_code_len,
                       cancel_flag,
                       on_match,
                       pretty_from_cmpkey,
                       highlight_all_occurrences=False,
                       max_window_words=10):
    """
    Robust search:
      - tokenizes page into words (with rectangles),
      - concatenates adjacent words (up to max_window_words) after removing separators,
      - compares against cmp_keys_nosep.
    Returns:
      hits (int)                              # number of rectangles added (approx.)
      matched_cmp_keys (set[str])             # cmp keys that appeared anywhere
      rects_by_page (dict[int -> list[(x0,y0,x1,y1)]])  # rectangles for highlighting
      code_pages   (dict[cmp_key -> set[int]])           # which 0-based pages had each code
      total_pages  (int)
    """
    doc = fitz.open(pdf_path)
    hits = 0
    matched = set()
    rects_by_page = {}
    code_pages = defaultdict(set)

    try:
        for page in doc:
            if cancel_flag.is_set():
                break

            words = page.get_text("words", sort=True)  # list of tuples
            # Each: (x0, y0, x1, y1, "text", block, line, word)
            W = []
            for w in words:
                x0, y0, x1, y1, t = float(w[0]), float(w[1]), float(w[2]), float(w[3]), (w[4] or "")
                norm = normalize_nosep(t)
                if not norm:
                    continue
                W.append(((x0, y0, x1, y1), norm, t))

            if not W:
                continue

            seen_on_page = set()  # cmp keys already highlighted on this page (if not "all")
            page_rects = []
            # For deduping rectangles a bit (avoid drawing same rect many times)
            rect_key_set = set()

            N = len(W)
            for i in range(N):
                if cancel_flag.is_set():
                    break
                s = ""
                rects_run = []
                # grow window
                for j in range(i, min(i + max_window_words, N)):
                    rect, norm, raw = W[j]
                    s += norm
                    rects_run.append(rect)

                    # prune if absurdly long
                    if len(s) > max_code_len + 4:
                        break

                    if s in cmp_keys_nosep:
                        # Count the page hit
                        code_pages[s].add(page.number)
                        matched.add(s)

                        if highlight_all_occurrences or (s not in seen_on_page):
                            # add rectangles for each word in the run
                            for (x0, y0, x1, y1) in rects_run:
                                key = (round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2))
                                if key not in rect_key_set:
                                    rect_key_set.add(key)
                                    page_rects.append((x0, y0, x1, y1))
                                    hits += 1

                            # Tell UI (once per code per page)
                            if s not in seen_on_page:
                                pretty = pretty_from_cmpkey(s)
                                on_match(s, os.path.basename(pdf_path), page.number + 1, pretty)

                            seen_on_page.add(s)

                        # If not highlighting "all", we can skip ahead to end of this window
                        if not highlight_all_occurrences:
                            break

            if page_rects:
                rects_by_page[page.number] = page_rects

        return hits, matched, rects_by_page, code_pages, doc.page_count
    finally:
        doc.close()

# ---------- Text highlight annotations ----------
def add_text_highlights(page, rects, color=(1, 1, 0), opacity=0.35):
    """Add proper PDF 'Highlight' annotations and make them printable."""
    for (x0, y0, x1, y1) in rects:
        r = fitz.Rect(x0, y0, x1, y1)
        ann = page.add_highlight_annot(r)
        try:
            ann.set_colors(stroke=color)
            ann.set_opacity(opacity)
            if hasattr(fitz, "ANNOT_PRINT"):
                ann.set_flags(fitz.ANNOT_PRINT)
        except Exception:
            pass
        ann.update()

# ---------- Combine (from ORIGINAL PDFs) with TEXT HIGHLIGHT ANNOTS ----------
def combine_from_selection(out_path, selections, only_highlighted_pages, use_text_annotations=True):
    """
    selections: list of dicts:
      {
        "pdf_path": str,
        "hit_pages": list[int],          # 0-based
        "keep_pages": set[int] or None,  # chosen in review (0-based)
        "rects_by_page": dict[int -> list[(x0,y0,x1,y1)]]
      }
    If use_text_annotations: add real highlight annots on copied pages.
    """
    out = fitz.open()
    try:
        for item in selections:
            pdf_path = item.get("pdf_path")
            if not pdf_path:
                continue
            rects_by_page = item.get("rects_by_page", {})
            with fitz.open(pdf_path) as src:
                if only_highlighted_pages:
                    pages = sorted(list(item.get("keep_pages") or item.get("hit_pages") or []))
                else:
                    pages = list(range(src.page_count))

                for p in pages:
                    out.insert_pdf(src, from_page=p, to_page=p)
                    out_pg = out.load_page(out.page_count - 1)
                    if use_text_annotations and p in rects_by_page:
                        add_text_highlights(out_pg, rects_by_page[p], color=(1, 1, 0), opacity=0.35)

        out_path = uniquify_path(out_path)
        out.save(out_path)
        return out_path
    finally:
        out.close()

# ---------- Review dialog with LIVE PREVIEW (no files) ----------
class ReviewDialog(tk.Toplevel):
    def __init__(self, master, items):
        """
        items: list of dicts with:
          display, pdf_path, hit_pages (list[int]), rects_by_page (dict[int -> list[rect]])
        """
        super().__init__(master)
        self.title("Review highlighted pages to keep")
        self.geometry("1100x700")
        self.minsize(980, 600)
        self.transient(master)
        self.grab_set()

        wrapper = ttk.Frame(self)
        wrapper.pack(fill="both", expand=True, padx=8, pady=8)

        left = ttk.Frame(wrapper)
        left.pack(side="left", fill="both", expand=True)
        right = ttk.Frame(wrapper)
        right.pack(side="right", fill="both", expand=True, padx=(8, 0))

        ttk.Label(left, text="Pages (double-click to toggle keep):").pack(anchor="w")

        self.tree = ttk.Treeview(
            left,
            columns=("keep", "file", "page"),
            show="headings",
            selectmode="browse",
            height=22
        )
        self.tree.heading("keep", text="Keep")
        self.tree.heading("file", text="File")
        self.tree.heading("page", text="Page")
        self.tree.column("keep", width=60, anchor="center")
        self.tree.column("file", width=520, anchor="w")
        self.tree.column("page", width=70, anchor="center")
        self.tree.pack(fill="both", expand=True)

        # Keep state + lookup
        self.keep_map = {}               # pdf_path -> set(page_idx)
        self.page_rects = {}             # (pdf_path, page_idx) -> list[rect]
        self._row_mapping = {}           # iid -> (pdf_path, page_idx)

        for it in items:
            pdf_path = it["pdf_path"]
            disp = it["display"]
            hit_pages = it["hit_pages"]
            rects_by_page = it["rects_by_page"]
            self.keep_map[pdf_path] = set(hit_pages)
            for p in hit_pages:
                iid = self.tree.insert("", "end", values=("[x]", disp, p + 1))
                self._row_mapping[iid] = (pdf_path, p)
                self.page_rects[(pdf_path, p)] = rects_by_page.get(p, [])

        # ===== PREVIEW pane =====
        ttk.Label(right, text="Preview").pack(anchor="w")
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

        self._preview_img = None
        self._zoom = 1.25
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

        if self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._preview_selected()

        self.protocol("WM_DELETE_WINDOW", self._cancel)

    # --- keep / selection ---
    def _toggle_keep(self, event=None):
        iid = self.tree.identify_row(event.y) if event else self.tree.focus()
        if not iid:
            return
        pdf_path, page = self._row_mapping[iid]
        if page in self.keep_map[pdf_path]:
            self.keep_map[pdf_path].remove(page)
            self.tree.set(iid, "keep", "[ ]")
        else:
            self.keep_map[pdf_path].add(page)
            self.tree.set(iid, "keep", "[x]")

    def _select_all(self):
        for iid, (pdf_path, page) in self._row_mapping.items():
            self.keep_map[pdf_path].add(page)
            self.tree.set(iid, "keep", "[x]")

    def _clear_all(self):
        for iid, (pdf_path, page) in self._row_mapping.items():
            self.keep_map[pdf_path].discard(page)
            self.tree.set(iid, "keep", "[ ]")

    def _ok(self):
        self.selection = self.keep_map
        self.destroy()

    def _cancel(self):
        self.selection = None
        self.destroy()

    # --- preview logic (in memory) ---
    def _change_zoom(self, delta):
        self._zoom = max(0.3, min(3.0, self._zoom + delta))
        self._preview_selected()

    def _preview_selected(self, event=None):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        pdf_path, page_idx = self._row_mapping[iid]
        self._render_page(pdf_path, page_idx)

    def _render_page(self, pdf_path, page_idx):
        self.stat.config(text=f"{os.path.basename(pdf_path)} — page {page_idx+1}")
        try:
            with fitz.open(pdf_path) as doc:
                pg = doc.load_page(page_idx)
                mat = fitz.Matrix(self._zoom, self._zoom)
                pix = pg.get_pixmap(matrix=mat, alpha=False)
                png_bytes = pix.tobytes("png")
                b64 = base64.b64encode(png_bytes).decode("ascii")
                img = tk.PhotoImage(data=b64)

            self._preview_img = img
            self.canvas.delete("all")
            self.canvas.create_image(0, 0, anchor="nw", image=img)
            self.canvas.config(scrollregion=(0, 0, img.width(), img.height()))

            # Overlay highlight boxes (for preview only)
            rects = self.page_rects.get((pdf_path, page_idx), [])
            z = self._zoom
            for (x0, y0, x1, y1) in rects:
                self.canvas.create_rectangle(x0*z, y0*z, x1*z, y1*z, outline="yellow", width=2)
        except Exception as e:
            self.canvas.delete("all")
            self.canvas.create_text(10, 10, anchor="nw", fill="white",
                                    text=f"Preview error:\n{e}")

# ---------- Summary dialog ----------
class SummaryDialog(tk.Toplevel):
    def __init__(self, master, rows, not_found_count, summary_csv_path):
        super().__init__(master)
        self.title("Match Summary")
        self.geometry("900x520")
        self.minsize(860, 480)
        self.transient(master)
        self.grab_set()

        info = ttk.Label(self, text=f"Codes not found: {not_found_count}    |    Summary CSV: {summary_csv_path}")
        info.pack(fill="x", padx=10, pady=(10, 0))

        cols = ("code", "total_pages", "breakdown")
        tree = ttk.Treeview(self, columns=cols, show="headings")
        tree.heading("code", text="ECS Code")
        tree.heading("total_pages", text="Pages Matched (total)")
        tree.heading("breakdown", text="Per-file breakdown")
        tree.column("code", width=220, anchor="w")
        tree.column("total_pages", width=160, anchor="center")
        tree.column("breakdown", width=460, anchor="w")
        tree.pack(fill="both", expand=True, padx=10, pady=10)

        for r in rows:
            tree.insert("", "end", values=(r["code"], r["total_pages"], r["breakdown"]))

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=10, pady=(0,10))
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="right")

# ---------- GUI App ----------
class HighlighterApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ECS PDF Highlighter")
        self.geometry("980x760")
        self.minsize(960, 720)

        # State
        self.excel_path = tk.StringVar()
        self.week_number = tk.StringVar()
        self.building_name = tk.StringVar()
        self.output_dir = tk.StringVar()

        self.only_highlighted_var = tk.BooleanVar(value=True)
        self.review_pages_var = tk.BooleanVar(value=True)
        self.ignore_lead_digit_var = tk.BooleanVar(value=False)
        self.highlight_all_var = tk.BooleanVar(value=True)
        self.use_text_annots_var = tk.BooleanVar(value=True)

        self.pdf_list = []
        self.cancel_flag = threading.Event()
        self.worker_thread = None
        self.msg_queue = queue.Queue()

        # Excel originals (pretty) + compare-key mapping
        self.ecs_original_map = {}          # primary -> pretty
        self.nosep_to_primary = {}          # cmp_key -> primary

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
        ttk.Checkbutton(fr_opts, text="Use text highlight annotations (prints)", variable=self.use_text_annots_var).pack(side="left", padx=12)

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
        self.tree = ttk.Treeview(fr_log, columns=cols, show="headings", height=12)
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
        path = filedialog.askopenfilename(
            title="Select Excel with ECS Codes",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
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
            bool(self.highlight_all_var.get()),
            bool(self.use_text_annots_var.get())
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
                out_dir, ignore_leading_digit, highlight_all_occurrences, use_text_annotations):
        def post(msg_type, payload=None):
            self.msg_queue.put((msg_type, payload))

        def pretty_from_cmpkey(cmp_key):
            primary = self.nosep_to_primary.get(cmp_key, cmp_key)
            return self.ecs_original_map.get(primary, primary)

        def on_match(cmp_key, file_name, page_num, pretty_text):
            post("match", (pretty_text, file_name, page_num))

        try:
            post("status", "Reading Excel…")
            df = load_table_with_dynamic_header(excel_path, sheet_name=0)
            if df is None:
                post("error", "Could not find a header row containing 'ECS Codes' or 'ECS Code' in the Excel.")
                return
            ecs_primary, original_map = extract_ecs_codes_from_df(df)
            if not ecs_primary:
                post("error", "No ECS codes found under 'ECS Codes'/'ECS Code'.")
                return

            self.ecs_original_map = dict(original_map)

            # Build hyphen/space-insensitive compare keys
            cmp_keys, nosep_to_primary, max_code_len = build_compare_index(ecs_primary, ignore_leading_digit)
            self.nosep_to_primary = dict(nosep_to_primary)

            processed = []
            agg_code_file_pages = defaultdict(lambda: defaultdict(set))  # cmp_key -> file -> set(pages)

            total = len(pdf_paths) if pdf_paths else 1
            for idx, pdf in enumerate(pdf_paths, start=1):
                if self.cancel_flag.is_set():
                    break
                post("status", f"Scanning: {os.path.basename(pdf)}")
                hits, matched, rects_by_page, code_pages, total_pages = scan_pdf_for_rects(
                    pdf_path=pdf,
                    cmp_keys_nosep=cmp_keys,
                    max_code_len=max_code_len,
                    cancel_flag=self.cancel_flag,
                    on_match=on_match,
                    pretty_from_cmpkey=pretty_from_cmpkey,
                    highlight_all_occurrences=highlight_all_occurrences
                )
                if hits > 0 and not self.cancel_flag.is_set():
                    hit_pages = sorted(list(rects_by_page.keys()))
                    processed.append({
                        "display": os.path.basename(pdf),
                        "pdf_path": pdf,
                        "hit_pages": hit_pages,
                        "rects_by_page": rects_by_page,
                        "total_pages": total_pages
                    })
                    fname = os.path.basename(pdf)
                    for cmp_key, pages in code_pages.items():
                        agg_code_file_pages[cmp_key][fname] |= set(pages)
                    post("status", f"Found {hits} match(es) in {os.path.basename(pdf)}")
                else:
                    post("status", f"No matches in {os.path.basename(pdf)}")

                post("progress", int((idx / total) * 100))

            if self.cancel_flag.is_set():
                post("done", None)
                return

            bldg_tag = sanitize_filename(building_name)
            combined_base = os.path.join(out_dir, f"{bldg_tag}_Highlighted_WK{week_number}.pdf")
            combined_out_path = uniquify_path(combined_base)

            # Prepare serializable summary
            agg_serializable = {
                cmp_key: {fn: sorted(list(pages)) for fn, pages in filepages.items()}
                for cmp_key, filepages in agg_code_file_pages.items()
            }

            post("review_data", {
                "processed": processed,
                "combined_out_path": combined_out_path,
                "building_name": building_name,
                "week_number": week_number,
                "out_dir": out_dir,
                "use_text_annotations": use_text_annotations,
                "ecs_primary": sorted(list(ecs_primary)),
                "original_map": dict(original_map),
                "nosep_to_primary": dict(nosep_to_primary),
                "agg_code_file_pages": agg_serializable
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
                    pass
        except queue.Empty:
            pass
        self.after(80, self._poll_messages)

    # ----- finalize: review + combine + CSV + SUMMARY -----
    def _finalize_and_save(self, bundle):
        processed = bundle["processed"]
        combined_out_path = bundle["combined_out_path"]
        building_name = bundle["building_name"]
        week_number = bundle["week_number"]
        out_dir = bundle["out_dir"]
        use_text_annotations = bool(bundle.get("use_text_annotations", True))
        ecs_primary = set(bundle.get("ecs_primary", []))                 # normalized primary keys
        original_map = dict(bundle.get("original_map", {}))              # primary -> pretty
        nosep_to_primary = dict(bundle.get("nosep_to_primary", {}))      # cmp -> primary
        agg_code_file_pages = dict(bundle.get("agg_code_file_pages", {})) # cmp -> {file -> [pages]}

        if not processed:
            messagebox.showinfo("No Matches", "No pages matched; nothing to save.")
            self.lbl_status.config(text="No matches.")
            # NotSurveyed = all codes
            self._write_not_surveyed_csv(out_dir, building_name, week_number,
                                         [original_map.get(p, p) for p in sorted(ecs_primary)])
            return

        # Review selection
        if self.review_pages_var.get():
            items = [{
                "display": p["display"],
                "pdf_path": p["pdf_path"],
                "hit_pages": p["hit_pages"],
                "rects_by_page": p["rects_by_page"]
            } for p in processed]
            dlg = ReviewDialog(self, items)
            self.wait_window(dlg)
            if dlg.selection is None:
                self.lbl_status.config(text="Review canceled.")
                return
            keep_map = dlg.selection  # pdf_path -> set(pages)
        else:
            keep_map = {p["pdf_path"]: set(p["hit_pages"]) for p in processed}

        # Combine
        selections = []
        only_highlighted = bool(self.only_highlighted_var.get())
        for p in processed:
            selections.append({
                "pdf_path": p["pdf_path"],
                "hit_pages": p["hit_pages"],
                "keep_pages": keep_map.get(p["pdf_path"], set(p["hit_pages"])),
                "rects_by_page": p["rects_by_page"]
            })

        try:
            final_path = combine_from_selection(
                out_path=combined_out_path,
                selections=selections,
                only_highlighted_pages=only_highlighted,
                use_text_annotations=use_text_annotations
            )
            if final_path:
                self.lbl_status.config(text=f"Saved: {os.path.basename(final_path)}")
                messagebox.showinfo("Done", f"Combined PDF saved:\n{final_path}")
        except Exception as e:
            messagebox.showerror("Combine", f"Could not save combined PDF:\n{e}")
            self.lbl_status.config(text="Combine failed.")
            return

        # --- Build per-code summary (counts are DISTINCT PAGES) ---
        primary_file_pages = defaultdict(lambda: defaultdict(set))  # primary -> file -> set(pages)
        for cmp_key, file_map in agg_code_file_pages.items():
            primary = nosep_to_primary.get(cmp_key, cmp_key)
            for fn, pages in file_map.items():
                primary_file_pages[primary][fn] |= set(pages)

        rows = []
        found_primary = set()
        for primary in sorted(primary_file_pages.keys()):
            total_pages = sum(len(pages) for pages in primary_file_pages[primary].values())
            found_primary.add(primary)
            pretty = original_map.get(primary, primary)
            breakdown = "; ".join(f"{fn}:{len(sorted(list(pages)))}"
                                  for fn, pages in sorted(primary_file_pages[primary].items()))
            rows.append({"code": pretty, "total_pages": total_pages, "breakdown": breakdown})

        # missing codes (use primaries only)
        missing_primary = sorted(list(ecs_primary - found_primary))
        not_found_count = len(missing_primary)

        # Write CSVs
        summary_csv = self._write_summary_csv(out_dir, building_name, week_number, rows)
        self._write_not_surveyed_csv(out_dir, building_name, week_number,
                                     [original_map.get(p, p) for p in missing_primary])

        # Show summary dialog
        SummaryDialog(self, rows, not_found_count, summary_csv)

    def _write_summary_csv(self, out_dir, building_name, week_number, rows):
        bldg_tag = sanitize_filename(building_name)
        csv_path = os.path.join(out_dir, f"{bldg_tag}_MatchesSummary_WK{week_number}.csv")
        csv_path = uniquify_path(csv_path)
        try:
            pd.DataFrame(rows, columns=["code", "total_pages", "breakdown"]).to_csv(csv_path, index=False)
        except Exception as e:
            messagebox.showwarning("CSV", f"Could not save MatchesSummary CSV:\n{e}")
        return csv_path

    def _write_not_surveyed_csv(self, out_dir, building_name, week_number, not_found_pretty_list):
        if not not_found_pretty_list:
            return None
        bldg_tag = sanitize_filename(building_name)
        csv_path = os.path.join(out_dir, f"{bldg_tag}_NotSurveyed_WK{week_number}.csv")
        csv_path = uniquify_path(csv_path)
        try:
            pd.DataFrame({"ECS_Code_Not_Found": sorted(not_found_pretty_list)}).to_csv(csv_path, index=False)
            self.lbl_status.config(text=f"CSV saved: {os.path.basename(csv_path)}")
        except Exception as e:
            messagebox.showwarning("CSV", f"Could not save NotSurveyed CSV:\n{e}")
        return csv_path

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
