import os
import re
import sys
import base64
import hashlib
import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import fitz  # PyMuPDF
import pandas as pd
from collections import defaultdict, Counter

import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import bisect

# ======================= Normalization & helpers =======================

DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2212"  # -, ‐, -, ‒, –, —, −
_STRIP_EDGE_PUNCT = re.compile(r'^[\s"\'()\[\]{}:;,.–—\-]+|[\s"\'()\[\]{}:;,.–—\-]+$')

# A3 dimensions (72 pt/in)
A3_PORTRAIT = (842.0, 1191.0)
A3_LANDSCAPE = (1191.0, 842.0)

SUMMARY_KEYWORDS = [
    "summary", "contents", "index", "bill of materials", "bom", "schedule"
]

def unify_dashes(s: str) -> str:
    if not s:
        return s
    for ch in DASH_CHARS[1:]:
        s = s.replace(ch, "-")
    return s.replace("\u00AD", "")  # soft hyphen

def normalize_base(token: str) -> str:
    if not token:
        return ""
    cleaned = _STRIP_EDGE_PUNCT.sub("", token)
    if not cleaned:
        return ""
    cleaned = unify_dashes(cleaned)
    return cleaned.strip().lower()

def normalize_nosep(token: str) -> str:
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

# ========================== Excel & ECS list ===========================

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

def build_compare_index(ecs_primary, ignore_leading_digit):
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

def build_prefixes_and_firstchars(cmp_keys_nosep):
    prefixes = set()
    first_chars = set()
    for key in cmp_keys_nosep:
        if not key:
            continue
        first_chars.add(key[0])
        for i in range(1, len(key) + 1):
            prefixes.add(key[:i])
    return prefixes, first_chars

# ========================= Turbo (Aho–Corasick) ========================

try:
    import ahocorasick  # pyahocorasick (C-accelerated)
    _HAS_AC = True
except Exception:
    _HAS_AC = False

def build_aho_automaton(cmp_keys_nosep):
    A = ahocorasick.Automaton()
    for k in cmp_keys_nosep:
        if k:
            A.add_word(k, k)
    A.make_automaton()
    return A

# ============================ PDF Scanners =============================

def scan_pdf_for_rects_fallback(pdf_path,
                                cmp_keys_nosep,
                                max_code_len,
                                cancel_flag,
                                highlight_all_occurrences=False,
                                prefixes=None,
                                first_chars=None):
    """
    Fallback scanner:
      - substring check per word
      - prefix-pruned sliding window across up to 10 words
      - collects rects per page AND per code (for survey duplication)
    Returns: hits, matched_set, rects_by_page, code_pages, code_rects_by_page, total_pages
    """
    if prefixes is None:
        prefixes, first_chars = build_prefixes_and_firstchars(cmp_keys_nosep)
    if first_chars is None:
        first_chars = {k[0] for k in cmp_keys_nosep if k}

    idx_by_first = {}
    for k in cmp_keys_nosep:
        if k:
            idx_by_first.setdefault(k[0], []).append(k)

    doc = fitz.open(pdf_path)
    hits = 0
    matched = set()
    rects_by_page = {}
    code_pages = defaultdict(set)
    code_rects_by_page = defaultdict(lambda: defaultdict(list))  # page_idx -> cmp_key -> [rects]

    try:
        for page in doc:
            if cancel_flag.is_set():
                break

            words = page.get_text("words", sort=True)
            W = []
            for w in words:
                x0, y0, x1, y1, t = float(w[0]), float(w[1]), float(w[2]), float(w[3]), (w[4] or "")
                norm = normalize_nosep(t)
                if not norm:
                    continue
                W.append(((x0, y0, x1, y1), norm, t))

            if not W:
                continue

            page_rects = []
            rect_key_set = set()

            # per-word substrings
            for (x0, y0, x1, y1), norm, raw in W:
                if not norm:
                    continue
                cands = idx_by_first.get(norm[0], [])
                for k in cands:
                    if len(k) > len(norm):
                        continue
                    if k in norm:
                        code_pages[k].add(page.number)
                        matched.add(k)
                        rkey = (round(x0,2), round(y0,2), round(x1,2), round(y1,2))
                        if rkey not in rect_key_set:
                            rect_key_set.add(rkey)
                            page_rects.append((x0, y0, x1, y1))
                            code_rects_by_page[page.number][k].append((x0, y0, x1, y1))
                            hits += 1

            # multi-word sliding window
            N = len(W)
            for i in range(N):
                if cancel_flag.is_set():
                    break
                if not W[i][1]:
                    continue
                if W[i][1][0] not in first_chars:
                    continue

                parts = []
                rects_run = []
                for j in range(i, min(i + 10, N)):
                    rect, norm, raw = W[j]
                    parts.append(norm)
                    rects_run.append(rect)

                    s = "".join(parts)
                    if len(s) > max_code_len + 4:
                        break
                    if s not in prefixes:
                        break
                    if s in cmp_keys_nosep:
                        code_pages[s].add(page.number)
                        matched.add(s)
                        for (rx0, ry0, rx1, ry1) in rects_run:
                            rkey = (round(rx0,2), round(ry0,2), round(rx1,2), round(ry1,2))
                            if rkey not in rect_key_set:
                                rect_key_set.add(rkey)
                                page_rects.append((rx0, ry0, rx1, ry1))
                                code_rects_by_page[page.number][s].append((rx0, ry0, rx1, ry1))
                                hits += 1

            if page_rects:
                rects_by_page[page.number] = page_rects

        return hits, matched, rects_by_page, code_pages, code_rects_by_page, doc.page_count
    finally:
        doc.close()

def scan_pdf_for_rects_ac(pdf_path,
                          automaton,
                          cancel_flag,
                          highlight_all_occurrences=False):
    """
    Aho–Corasick scan.
    Returns: hits, matched_set, rects_by_page, code_pages, code_rects_by_page, total_pages
    """
    doc = fitz.open(pdf_path)
    hits = 0
    matched = set()
    rects_by_page = {}
    code_pages = defaultdict(set)
    code_rects_by_page = defaultdict(lambda: defaultdict(list))

    try:
        for page in doc:
            if cancel_flag.is_set():
                break

            words = page.get_text("words", sort=True)
            if not words:
                continue

            rects = []
            norms = []
            for w in words:
                x0, y0, x1, y1, t = float(w[0]), float(w[1]), float(w[2]), float(w[3]), (w[4] or "")
                n = normalize_nosep(t)
                if not n:
                    continue
                rects.append((x0, y0, x1, y1))
                norms.append(n)

            if not norms:
                continue

            cum = [0]
            for n in norms:
                cum.append(cum[-1] + len(n))
            S = "".join(norms)

            page_rects = []
            rect_key_set = set()

            for end_idx, key in automaton.iter(S):
                start_idx = end_idx - len(key) + 1
                ws = bisect.bisect_right(cum, start_idx) - 1
                we = bisect.bisect_right(cum, end_idx) - 1
                if ws < 0 or we < 0 or ws >= len(rects) or we >= len(rects) or we < ws:
                    continue

                code_pages[key].add(page.number)
                matched.add(key)

                for k in range(ws, we + 1):
                    x0, y0, x1, y1 = rects[k]
                    rkey = (round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2))
                    if rkey not in rect_key_set:
                        rect_key_set.add(rkey)
                        page_rects.append((x0, y0, x1, y1))
                        code_rects_by_page[page.number][key].append((x0, y0, x1, y1))
                        hits += 1

            if page_rects:
                rects_by_page[page.number] = page_rects

        return hits, matched, rects_by_page, code_pages, code_rects_by_page, doc.page_count
    finally:
        doc.close()

# ========================= Annotation & combine ========================

def add_text_highlights(page, rects, color=(1, 1, 0), opacity=0.35):
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

def _fit_scale_and_offset(src_w, src_h, dst_w, dst_h):
    if src_w <= 0 or src_h <= 0:
        return 1.0, 0.0, 0.0
    sx = dst_w / src_w
    sy = dst_h / src_h
    s = min(sx, sy)
    new_w = src_w * s
    new_h = src_h * s
    dx = (dst_w - new_w) * 0.5
    dy = (dst_h - new_h) * 0.5
    return s, dx, dy

def combine_pages_to_new(out_path, page_items, use_text_annotations=True, scale_to_a3=False):
    """
    page_items: list of dicts with:
      "pdf_path", "page_idx", "rects"
    """
    out = fitz.open()
    try:
        by_pdf = defaultdict(list)
        for it in page_items:
            by_pdf[it["pdf_path"]].append(it)

        for pdf_path, items in by_pdf.items():
            items.sort(key=lambda x: x["page_idx"])
            with fitz.open(pdf_path) as src:
                for it in items:
                    p = it["page_idx"]
                    src_pg = src.load_page(p)
                    src_rect = src_pg.rect
                    sw, sh = float(src_rect.width), float(src_rect.height)

                    if not scale_to_a3:
                        out.insert_pdf(src, from_page=p, to_page=p)
                        out_pg = out.load_page(out.page_count - 1)
                        if use_text_annotations and it["rects"]:
                            add_text_highlights(out_pg, it["rects"], color=(1, 1, 0), opacity=0.35)
                        continue

                    # scale to A3
                    src_landscape = sw >= sh
                    tw, th = (A3_LANDSCAPE if src_landscape else A3_PORTRAIT)
                    out_pg = out.new_page(width=tw, height=th)
                    dst_rect = fitz.Rect(0, 0, tw, th)
                    out_pg.show_pdf_page(dst_rect, src, p)
                    s, dx, dy = _fit_scale_and_offset(sw, sh, tw, th)

                    if use_text_annotations and it["rects"]:
                        xfm_rects = []
                        for (x0, y0, x1, y1) in it["rects"]:
                            fx0 = x0 * s + dx
                            fy0 = y0 * s + dy
                            fx1 = x1 * s + dx
                            fy1 = y1 * s + dy
                            xfm_rects.append((fx0, fy0, fx1, fy1))
                        add_text_highlights(out_pg, xfm_rects, color=(1, 1, 0), opacity=0.35)

        out_path = uniquify_path(out_path)
        out.save(out_path)
        return out_path
    finally:
        out.close()

def chunk_list(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# ======================== Building inference logic =====================

_LEADING_D3L = re.compile(r'^[0-9]([a-z]{3})', re.I)
_FIRST_LETTERS = re.compile(r'[a-z]+', re.I)

def infer_building_from_code(pretty_code: str) -> str:
    s = unify_dashes(pretty_code or "").strip()
    if not s:
        return "UNKWN"
    s_no = re.sub(r'[^0-9A-Za-z-]', '', s)
    m = _LEADING_D3L.match(s_no)
    if m:
        return (s_no[0] + m.group(1)).upper()
    t = re.sub(r'^[0-9-]+', '', s_no)
    m2 = _FIRST_LETTERS.match(t)
    if not m2:
        return "UNKWN"
    letters = m2.group(0).upper()
    if '-' in s_no[:5]:
        return letters[:2] if len(letters) >= 2 else letters
    return letters[:3] if len(letters) >= 3 else letters

# ========================= Survey / Drawings helpers ===================

def is_survey_pdf(path, size_limit_bytes=1_200_000):
    try:
        sz = os.path.getsize(path)
    except Exception:
        sz = 0
    name = os.path.basename(path).lower()
    looks_name = ("cut l" in name) or ("cut length report" in name)
    return looks_name and (sz > 0 and sz <= size_limit_bytes)

def fingerprint_page_text(pg):
    try:
        txt = pg.get_text("text") or ""
        txt = re.sub(r'\s+', ' ', txt).strip().lower()
        h = hashlib.sha1()
        h.update(txt.encode("utf-8", errors="ignore"))
        r = pg.rect
        h.update(f"{int(r.width)}x{int(r.height)}".encode())
        return "T:" + h.hexdigest()
    except Exception:
        return None

def fingerprint_page_image(pg, scale=0.35):
    try:
        mat = fitz.Matrix(scale, scale)
        pix = pg.get_pixmap(matrix=mat, alpha=False)
        b = pix.samples
        h = hashlib.sha1()
        h.update(b[:200000])
        return "I:" + h.hexdigest()
    except Exception:
        return None

def page_fingerprint(pdf_path, page_idx):
    try:
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            fh = fingerprint_page_text(pg)
            if fh:
                return fh
            ih = fingerprint_page_image(pg)
            return ih or f"X:{pdf_path}:{page_idx}"
    except Exception:
        return f"X:{pdf_path}:{page_idx}"

def is_summary_like(pdf_path, page_idx, codes_on_page, threshold=15):
    # Heuristic: many different codes on one page OR summary keywords in text
    if len(codes_on_page) >= threshold:
        return True
    try:
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            txt = (pg.get_text("text") or "").lower()
            for kw in SUMMARY_KEYWORDS:
                if kw in txt:
                    return True
    except Exception:
        pass
    return False

# ========================== Worker task (MP) ===========================

def _process_pdf_task(args):
    (pdf_path,
     cmp_keys_list,
     use_ac,
     highlight_all_occurrences) = args

    cancel_flag = _DummyCancel()
    cmp_keys = set(cmp_keys_list)

    try:
        if use_ac and _HAS_AC:
            automaton = build_aho_automaton(cmp_keys)
            hits, matched, rects_by_page, code_pages, code_rects_by_page, total_pages = scan_pdf_for_rects_ac(
                pdf_path=pdf_path,
                automaton=automaton,
                cancel_flag=cancel_flag,
                highlight_all_occurrences=highlight_all_occurrences
            )
        else:
            prefixes, first_chars = build_prefixes_and_firstchars(cmp_keys)
            max_code_len = max((len(k) for k in cmp_keys), default=0)
            hits, matched, rects_by_page, code_pages, code_rects_by_page, total_pages = scan_pdf_for_rects_fallback(
                pdf_path=pdf_path,
                cmp_keys_nosep=cmp_keys,
                max_code_len=max_code_len,
                cancel_flag=cancel_flag,
                highlight_all_occurrences=highlight_all_occurrences,
                prefixes=prefixes,
                first_chars=first_chars
            )

        rects_by_page_ser = {int(k): [tuple(r) for r in v] for k, v in rects_by_page.items()}
        code_pages_ser = {k: sorted(list(v)) for k, v in code_pages.items()}
        code_rects_ser = {int(p): {ck: [tuple(r) for r in rects]
                                   for ck, rects in mp.items()}
                          for p, mp in code_rects_by_page.items()}
        hit_pages = sorted(list(rects_by_page.keys()))

        match_pairs = []
        for k, pages in code_pages_ser.items():
            for p in pages:
                match_pairs.append((k, p + 1))

        return {
            "pdf_path": pdf_path,
            "display": os.path.basename(pdf_path),
            "hits": int(hits),
            "rects_by_page": rects_by_page_ser,
            "code_pages": code_pages_ser,
            "code_rects_by_page": code_rects_ser,
            "hit_pages": hit_pages,
            "total_pages": int(total_pages),
            "match_pairs": match_pairs
        }
    except Exception as e:
        return {
            "pdf_path": pdf_path,
            "display": os.path.basename(pdf_path),
            "error": str(e)
        }

class _DummyCancel:
    def is_set(self): return False

# ============================ Review Dialog ============================

class ReviewDialog(tk.Toplevel):
    def __init__(self, master, items):
        """
        items: list of dicts with:
          display, pdf_path, hit_pages (list[int]),
          rects_by_page (dict[int -> list[rect]]),
          page_codes (dict[int -> list[str]])   # pretty codes per page
        """
        super().__init__(master)
        self.title("Review highlighted pages to keep")
        self.geometry("1200x740")
        self.minsize(1080, 660)
        self.transient(master)
        self.grab_set()

        paned = ttk.Panedwindow(self, orient="horizontal")
        paned.pack(fill="both", expand=True, padx=8, pady=8)

        left = ttk.Frame(paned)
        right = ttk.Frame(paned)
        paned.add(left, weight=3)
        paned.add(right, weight=2)

        ttk.Label(left, text="Pages (double-click to toggle keep). Click headers to sort, use Move to reorder.").pack(anchor="w")

        tree_frame = ttk.Frame(left)
        tree_frame.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=("order", "keep", "file", "page", "codes"),
            show="headings",
            selectmode="browse",
            height=24
        )
        self.tree.heading("order", text="#")
        self.tree.heading("keep", text="Keep", command=lambda: self._sort_tree("keep"))
        self.tree.heading("file", text="File", command=lambda: self._sort_tree("file"))
        self.tree.heading("page", text="Page", command=lambda: self._sort_tree("page"))
        self.tree.heading("codes", text="Codes", command=lambda: self._sort_tree("codes"))
        self.tree.column("order", width=40, anchor="center")
        self.tree.column("keep", width=60, anchor="center")
        self.tree.column("file", width=500, anchor="w")
        self.tree.column("page", width=70, anchor="center")
        self.tree.column("codes", width=300, anchor="w")

        ybar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        self.keep_map = {}
        self.page_rects = {}
        self.page_codes = {}
        self._row_mapping = {}
        self._pdf_display = {}

        row_idx = 1
        for it in items:
            pdf_path = it["pdf_path"]
            disp = it["display"]
            hit_pages = it["hit_pages"]
            rects_by_page = it["rects_by_page"]
            page_codes = it.get("page_codes", {})
            self._pdf_display[pdf_path] = disp
            self.keep_map[pdf_path] = set(hit_pages)
            for p in hit_pages:
                codes_pretty = ", ".join(sorted(page_codes.get(p, [])))
                iid = self.tree.insert("", "end", values=(row_idx, "[x]", disp, p + 1, codes_pretty))
                self._row_mapping[iid] = (pdf_path, p)
                self.page_rects[(pdf_path, p)] = rects_by_page.get(p, [])
                self.page_codes[(pdf_path, p)] = page_codes.get(p, [])
                row_idx += 1

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

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(6, 8))
        ttk.Button(btns, text="Select All", command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Clear All", command=self._clear_all).pack(side="left", padx=6)
        ttk.Button(btns, text="Maximize", command=self._maximize).pack(side="left", padx=12)
        ttk.Button(btns, text="Restore", command=self._restore).pack(side="left", padx=6)
        ttk.Button(btns, text="Fullscreen (F11)", command=self._toggle_fullscreen).pack(side="left", padx=6)
        mv = ttk.Frame(btns); mv.pack(side="left", padx=16)
        ttk.Button(mv, text="Top ⤒", command=self._move_top).pack(side="left", padx=2)
        ttk.Button(mv, text="Up ↑", command=self._move_up).pack(side="left", padx=2)
        ttk.Button(mv, text="Down ↓", command=self._move_down).pack(side="left", padx=2)
        ttk.Button(mv, text="Bottom ⤓", command=self._move_bottom).pack(side="left", padx=2)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)

        self.tree.bind("<Double-1>", self._toggle_keep)
        self.tree.bind("<<TreeviewSelect>>", self._preview_selected)

        self.bind("<F11>", lambda e: self._toggle_fullscreen())
        self.bind("<Escape>", lambda e: (self.attributes("-fullscreen", False), self._restore()))
        self.bind("<Control-m>", lambda e: self._maximize())

        self._sort_state = {"keep": False, "file": False, "page": False, "codes": False}

        if self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._preview_selected()

        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _maximize(self):
        try:
            self.state("zoomed")
            return
        except Exception:
            pass
        w = self.winfo_screenwidth(); h = self.winfo_screenheight()
        self.geometry(f"{int(w*0.98)}x{int(h*0.96)}+0+0")

    def _restore(self):
        try:
            self.state("normal")
        except Exception:
            pass
        self.geometry("1200x740")

    def _toggle_fullscreen(self):
        cur = bool(self.attributes("-fullscreen"))
        self.attributes("-fullscreen", not cur)

    def _rows_snapshot(self):
        rows = []
        for iid in self.tree.get_children():
            pdf_path, page = self._row_mapping[iid]
            disp = self._pdf_display.get(pdf_path, os.path.basename(pdf_path))
            keep = (page in self.keep_map.get(pdf_path, set()))
            codes = ", ".join(sorted(self.page_codes.get((pdf_path, page), [])))
            order = int(self.tree.set(iid, "order") or "0")
            rows.append({
                "iid": iid,
                "order": order,
                "pdf_path": pdf_path,
                "page": page,
                "display": disp,
                "keep": keep,
                "codes": codes
            })
        return rows

    def _rebuild_tree(self, rows):
        self.tree.delete(*self.tree.get_children())
        self._row_mapping.clear()
        for idx, r in enumerate(rows, start=1):
            keep_txt = "[x]" if r["keep"] else "[ ]"
            iid = self.tree.insert("", "end", values=(idx, keep_txt, r["display"], r["page"] + 1, r["codes"]))
            self._row_mapping[iid] = (r["pdf_path"], r["page"])

    def _sort_tree(self, column, toggle=True):
        if toggle:
            self._sort_state[column] = not self._sort_state.get(column, False)
        reverse = self._sort_state.get(column, False)

        rows = self._rows_snapshot()
        if column == "file":
            rows.sort(key=lambda r: (r["display"].lower(), r["page"]), reverse=reverse)
        elif column == "page":
            rows.sort(key=lambda r: r["page"], reverse=reverse)
        elif column == "keep":
            rows.sort(key=lambda r: ((not r["keep"]), r["display"].lower(), r["page"]), reverse=reverse)
        elif column == "codes":
            rows.sort(key=lambda r: (r["codes"].lower(), r["display"].lower(), r["page"]), reverse=reverse)
        else:
            return

        self._rebuild_tree(rows)

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

    def _reindex_orders(self):
        for idx, iid in enumerate(self.tree.get_children(), start=1):
            self.tree.set(iid, "order", str(idx))

    def _move_up(self):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]
        prev = self.tree.prev(iid)
        if not prev: return
        self.tree.move(iid, self.tree.parent(iid), self.tree.index(prev))
        self._reindex_orders()

    def _move_down(self):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]
        next_i = self.tree.next(iid)
        if not next_i: return
        self.tree.move(iid, self.tree.parent(iid), self.tree.index(next_i)+1)
        self._reindex_orders()

    def _move_top(self):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]
        self.tree.move(iid, self.tree.parent(iid), 0)
        self._reindex_orders()

    def _move_bottom(self):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]
        self.tree.move(iid, self.tree.parent(iid), "end")
        self._reindex_orders()

    def _ok(self):
        seq = []
        for iid in self.tree.get_children():
            pdf_path, page = self._row_mapping[iid]
            keep = (page in self.keep_map.get(pdf_path, set()))
            if keep:
                seq.append((pdf_path, page))
        self.selection = {"keep_map": self.keep_map, "sequence": seq}
        self.destroy()

    def _cancel(self):
        self.selection = None
        self.destroy()

    def _change_zoom(self, delta):
        self._zoom = max(0.3, min(3.0, getattr(self, "_zoom", 1.25) + delta))
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
                z = getattr(self, "_zoom", 1.25)
                mat = fitz.Matrix(z, z)
                pix = pg.get_pixmap(matrix=mat, alpha=False)
                png_bytes = pix.tobytes("png")
                b64 = base64.b64encode(png_bytes).decode("ascii")
                img = tk.PhotoImage(data=b64)

            self._preview_img = img
            self.canvas.delete("all")
            self.canvas.create_image(0, 0, anchor="nw", image=img)
            self.canvas.config(scrollregion=(0, 0, img.width(), img.height()))
        except Exception as e:
            self.canvas.delete("all")
            self.canvas.create_text(10, 10, anchor="nw", fill="white",
                                    text=f"Preview error:\n{e}")

# ============================= Summary UI ==============================

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

# ============================== Main App ===============================

class HighlighterApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ECS PDF Highlighter")
        self.geometry("1140x940")
        self.minsize(1100, 900)

        # State
        self.excel_paths = []
        self.week_number = tk.StringVar()
        self.building_name = tk.StringVar()
        self.output_dir = tk.StringVar()
        self.pages_per_file_var = tk.IntVar(value=20)

        self.only_highlighted_var = tk.BooleanVar(value=True)
        self.review_pages_var = tk.BooleanVar(value=True)
        self.ignore_lead_digit_var = tk.BooleanVar(value=False)
        self.highlight_all_var = tk.BooleanVar(value=True)
        self.use_text_annots_var = tk.BooleanVar(value=True)
        self.scale_to_a3_var = tk.BooleanVar(value=False)

        self.turbo_var = tk.BooleanVar(value=True)
        self.parallel_var = tk.BooleanVar(value=True)

        # De-dup / survey / summary controls
        self.treat_survey_var = tk.BooleanVar(value=True)
        self.survey_size_limit = tk.IntVar(value=1200)  # KB
        self.dedupe_var = tk.BooleanVar(value=True)
        self.skip_summary_var = tk.BooleanVar(value=True)
        self.summary_threshold = tk.IntVar(value=15)

        self.pdf_list = []
        self.cancel_flag = threading.Event()
        self.worker_thread = None
        self.msg_queue = queue.Queue()

        self.ecs_original_map = {}
        self.nosep_to_primary = {}

        self.main_page_codes = defaultdict(set)
        self.main_row_iid = {}

        self._build_ui()
        self._poll_messages()

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        fr_top = ttk.Frame(self); fr_top.pack(fill="x", **pad)
        ttk.Label(fr_top, text="Week:").pack(side="left")
        ttk.Entry(fr_top, width=8, textvariable=self.week_number).pack(side="left", padx=8)
        ttk.Label(fr_top, text="Project/Root Name:").pack(side="left", padx=(16, 0))
        ttk.Entry(fr_top, width=30, textvariable=self.building_name).pack(side="left", padx=8, fill="x", expand=True)
        ttk.Label(fr_top, text="Max pages per output:").pack(side="left", padx=(16, 0))
        tk.Spinbox(fr_top, from_=5, to=500, increment=1, width=6, textvariable=self.pages_per_file_var).pack(side="left", padx=6)

        fr_opts = ttk.Frame(self); fr_opts.pack(fill="x", **pad)
        ttk.Checkbutton(fr_opts, text="Only keep highlighted pages", variable=self.only_highlighted_var).pack(side="left")
        ttk.Checkbutton(fr_opts, text="Review pages before saving", variable=self.review_pages_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Ignore leading digit in PDF codes", variable=self.ignore_lead_digit_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Highlight every occurrence", variable=self.highlight_all_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Use text highlight annotations", variable=self.use_text_annots_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Scale output pages to A3", variable=self.scale_to_a3_var).pack(side="left", padx=12)

        fr_perf = ttk.Frame(self); fr_perf.pack(fill="x", **pad)
        ttk.Checkbutton(fr_perf, text="Turbo (Aho–Corasick)", variable=self.turbo_var).pack(side="left")
        ttk.Checkbutton(fr_perf, text="Parallel PDFs", variable=self.parallel_var).pack(side="left", padx=12)

        fr_rules = ttk.LabelFrame(self, text="De-dup & Survey Rules"); fr_rules.pack(fill="x", **pad)
        ttk.Checkbutton(fr_rules, text="Treat 'Cut Length Report' PDFs as survey tables", variable=self.treat_survey_var).grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(fr_rules, text="Survey size ≤ KB:").grid(row=0, column=1, sticky="e")
        tk.Spinbox(fr_rules, from_=200, to=5000, increment=50, width=6, textvariable=self.survey_size_limit).grid(row=0, column=2, sticky="w", padx=6)

        ttk.Checkbutton(fr_rules, text="De-duplicate identical pages (text/image hash)", variable=self.dedupe_var).grid(row=1, column=0, columnspan=2, sticky="w", padx=6, pady=4)
        ttk.Checkbutton(fr_rules, text="Skip summary-like pages", variable=self.skip_summary_var).grid(row=2, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(fr_rules, text="Summary = ≥ codes/page:").grid(row=2, column=1, sticky="e")
        tk.Spinbox(fr_rules, from_=5, to=100, increment=1, width=6, textvariable=self.summary_threshold).grid(row=2, column=2, sticky="w", padx=6)

        fr_excel = ttk.LabelFrame(self, text="Excel files (ECS Codes)"); fr_excel.pack(fill="x", **pad)
        btns_ex = ttk.Frame(fr_excel); btns_ex.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns_ex, text="Add Excel…", command=self._add_excels).pack(side="left")
        ttk.Button(btns_ex, text="Remove Selected", command=self._remove_selected_excels).pack(side="left", padx=6)
        ttk.Button(btns_ex, text="Clear List", command=self._clear_excels).pack(side="left")
        self.lst_excels = tk.Listbox(fr_excel, height=5, selectmode=tk.EXTENDED)
        self.lst_excels.pack(fill="both", expand=True, padx=6, pady=(0,6))

        fr_pdfs = ttk.LabelFrame(self, text="PDFs to Process"); fr_pdfs.pack(fill="both", expand=True, **pad)
        btns = ttk.Frame(fr_pdfs); btns.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns, text="Add PDFs…", command=self._add_pdfs).pack(side="left")
        ttk.Button(btns, text="Remove Selected", command=self._remove_selected_pdfs).pack(side="left", padx=6)
        ttk.Button(btns, text="Clear List", command=self._clear_pdfs).pack(side="left")
        self.lst_pdfs = tk.Listbox(fr_pdfs, height=7, selectmode=tk.EXTENDED)
        self.lst_pdfs.pack(fill="both", expand=True, padx=6, pady=(0,6))

        fr_out = ttk.Frame(self); fr_out.pack(fill="x", **pad)
        ttk.Label(fr_out, text="Output Folder:").pack(side="left")
        ttk.Entry(fr_out, textvariable=self.output_dir).pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_out, text="Select…", command=self._pick_output_dir).pack(side="left")

        fr_log = ttk.LabelFrame(self, text="Matches (ECS Code | File | Page | Codes on Page)"); fr_log.pack(fill="both", expand=True, **pad)
        cols = ("code", "file", "page", "codes_on_page")
        self.tree = ttk.Treeview(fr_log, columns=cols, show="headings", height=12)
        self.tree.heading("code", text="Code")
        self.tree.heading("file", text="File")
        self.tree.heading("page", text="Page")
        self.tree.heading("codes_on_page", text="Codes (on page)")
        self.tree.column("code", width=180, anchor="w")
        self.tree.column("file", width=560, anchor="w")
        self.tree.column("page", width=60, anchor="center")
        self.tree.column("codes_on_page", width=220, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)

        fr_prog = ttk.Frame(self); fr_prog.pack(fill="x", **pad)
        self.prog = ttk.Progressbar(fr_prog, orient="horizontal", mode="determinate", maximum=100)
        self.prog.pack(side="left", expand=True, fill="x")
        self.lbl_status = ttk.Label(fr_prog, text="Idle"); self.lbl_status.pack(side="left", padx=8)

        fr_btns = ttk.Frame(self); fr_btns.pack(fill="x", **pad)
        ttk.Button(fr_btns, text="Start", command=self._start).pack(side="left")
        ttk.Button(fr_btns, text="Stop", command=self._stop).pack(side="left", padx=6)
        ttk.Button(fr_btns, text="Exit", command=self._exit).pack(side="right")

    # ======= Excel / PDF pickers =======
    def _add_excels(self):
        paths = filedialog.askopenfilenames(title="Select Excel files", filetypes=[("Excel files", "*.xlsx *.xls")])
        if paths:
            for p in paths:
                if p not in self.excel_paths:
                    self.excel_paths.append(p)
                    self.lst_excels.insert("end", p)

    def _remove_selected_excels(self):
        sels = list(self.lst_excels.curselection())[::-1]
        for i in sels:
            path = self.lst_excels.get(i)
            self.lst_excels.delete(i)
            try:
                self.excel_paths.remove(path)
            except ValueError:
                pass

    def _clear_excels(self):
        self.lst_excels.delete(0, "end")
        self.excel_paths.clear()

    def _add_pdfs(self):
        paths = filedialog.askopenfilenames(title="Select PDFs", filetypes=[("PDF files", "*.pdf")])
        if paths:
            for p in paths:
                if p not in self.pdf_list:
                    self.pdf_list.append(p)
                    self.lst_pdfs.insert("end", p)

    def _remove_selected_pdfs(self):
        sels = list(self.lst_pdfs.curselection())[::-1]
        for i in sels:
            path = self.lst_pdfs.get(i)
            self.lst_pdfs.delete(i)
            try:
                self.pdf_list.remove(path)
            except ValueError:
                pass

    def _clear_pdfs(self):
        self.lst_pdfs.delete(0, "end")
        self.pdf_list.clear()

    # misc pickers
    def _pick_output_dir(self):
        d = filedialog.askdirectory(title="Select Output Folder")
        if d:
            self.output_dir.set(d)

    # run controls
    def _start(self):
        if self.worker_thread and self.worker_thread.is_alive():
            return
        week = self.week_number.get().strip()
        rootname = self.building_name.get().strip()
        excels = list(self.excel_paths)
        if not week or not excels or not self.pdf_list:
            messagebox.showwarning("Input", "Please provide Week, at least ONE Excel, and PDFs.")
            return

        out_dir = self.output_dir.get().strip() or os.path.dirname(self.pdf_list[0])
        self.output_dir.set(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        # reset main aggregation per run
        self.main_page_codes.clear()
        self.main_row_iid.clear()
        for iid in self.tree.get_children():
            self.tree.delete(iid)

        self.cancel_flag.clear()
        self.prog["value"] = 0
        self.lbl_status.config(text="Starting…")

        args = (
            week, rootname, list(excels), list(self.pdf_list), out_dir,
            int(self.pages_per_file_var.get()),
            bool(self.ignore_lead_digit_var.get()),
            bool(self.highlight_all_var.get()),
            bool(self.use_text_annots_var.get()),
            bool(self.turbo_var.get()),
            bool(self.parallel_var.get()),
            bool(self.scale_to_a3_var.get()),
            bool(self.treat_survey_var.get()),
            int(self.survey_size_limit.get()) * 1024,
            bool(self.dedupe_var.get()),
            bool(self.skip_summary_var.get()),
            int(self.summary_threshold.get()),
        )
        self.worker_thread = threading.Thread(target=self._worker, args=args, daemon=True)
        self.worker_thread.start()

    def _stop(self):
        self.cancel_flag.set()
        self.lbl_status.config(text="Stopping…")

    def _exit(self):
        self.destroy()

    # background worker
    def _worker(self, week_number, root_name, excel_paths, pdf_paths, out_dir, pages_per_file,
                ignore_leading_digit, highlight_all_occurrences,
                use_text_annotations, turbo_mode, parallel_mode, scale_to_a3,
                treat_survey, survey_size_limit_bytes, dedupe_pages, skip_summary, summary_threshold):

        def post(msg_type, payload=None):
            self.msg_queue.put((msg_type, payload))

        try:
            # Load ALL Excels and merge codes
            post("status", "Reading Excel files…")
            ecs_primary_all = set()
            original_map_all = {}
            for xp in excel_paths:
                try:
                    df = load_table_with_dynamic_header(xp, sheet_name=0)
                    ecs_primary, original_map = extract_ecs_codes_from_df(df)
                    ecs_primary_all |= ecs_primary
                    for k, v in original_map.items():
                        original_map_all.setdefault(k, v)
                except Exception as e:
                    post("status", f"Excel error {os.path.basename(xp)}: {e}")

            if not ecs_primary_all:
                post("error", "No ECS codes found in the selected Excel files.")
                return

            self.ecs_original_map = dict(original_map_all)
            cmp_keys, nosep_to_primary, _max_len = build_compare_index(ecs_primary_all, ignore_leading_digit)
            self.nosep_to_primary = dict(nosep_to_primary)

            # Prepare tasks
            tasks = []
            for pdf in pdf_paths:
                tasks.append((
                    pdf,
                    sorted(list(cmp_keys)),
                    bool(turbo_mode and _HAS_AC),
                    bool(highlight_all_occurrences),
                ))

            results = []
            total = len(tasks) if tasks else 1
            completed = 0

            if parallel_mode and len(tasks) > 1:
                max_workers = max(1, (os.cpu_count() or 2))
                post("status", f"Scanning in parallel ({max_workers} workers)…")
                with ProcessPoolExecutor(max_workers=max_workers) as ex:
                    fut_to_pdf = {ex.submit(_process_pdf_task, t): t[0] for t in tasks}
                    for fut in as_completed(fut_to_pdf):
                        if self.cancel_flag.is_set():
                            break
                        res = fut.result()
                        results.append(res)
                        completed += 1
                        post("status", f"Processed: {os.path.basename(res.get('pdf_path',''))}")
                        post("progress", int((completed / total) * 100))
            else:
                post("status", "Scanning (single process)…")
                for t in tasks:
                    if self.cancel_flag.is_set():
                        break
                    res = _process_pdf_task(t)
                    results.append(res)
                    completed += 1
                    post("status", f"Processed: {os.path.basename(res.get('pdf_path',''))}")
                    post("progress", int((completed / total) * 100))

            if self.cancel_flag.is_set():
                post("done", None)
                return

            # Aggregate + push UI match rows
            processed = []
            agg_code_file_pages = defaultdict(lambda: defaultdict(set))  # cmp_key -> file -> set(pages 0-based)

            for res in results:
                if "error" in res:
                    post("status", f"Error in {os.path.basename(res['pdf_path'])}: {res['error']}")
                    continue
                pdf_path = res["pdf_path"]
                display = res["display"]
                rects_by_page = res["rects_by_page"]
                code_rects_by_page = res["code_rects_by_page"]  # page -> cmp_key -> [rect]
                hit_pages = res["hit_pages"]
                total_pages = res["total_pages"]
                code_pages = res["code_pages"]          # cmp_key -> [0-based]
                match_pairs = res["match_pairs"]

                for cmp_key, page_1b in match_pairs:
                    primary = self.nosep_to_primary.get(cmp_key, cmp_key)
                    pretty = self.ecs_original_map.get(primary, primary)
                    self.msg_queue.put(("match", (pretty, display, page_1b)))

                for cmp_key, pages in code_pages.items():
                    agg_code_file_pages[cmp_key][display] |= set(pages)

                processed.append({
                    "display": display,
                    "pdf_path": pdf_path,
                    "hit_pages": hit_pages,
                    "rects_by_page": rects_by_page,
                    "code_rects_by_page": code_rects_by_page,
                    "page_codes": {
                        int(p): sorted({
                            self.ecs_original_map.get(self.nosep_to_primary.get(cmp_key, cmp_key), cmp_key)
                            for cmp_key, pglist in code_pages.items() if int(p) in pglist
                        })
                        for p in hit_pages
                    },
                    "total_pages": total_pages
                })

            agg_serializable = {
                cmp_key: {fn: sorted(list(pages)) for fn, pages in filepages.items()}
                for cmp_key, filepages in agg_code_file_pages.items()
            }

            post("review_data", {
                "processed": processed,
                "root_name": root_name,
                "week_number": week_number,
                "out_dir": out_dir,
                "use_text_annotations": bool(use_text_annotations),
                "ecs_primary": sorted(list(ecs_primary_all)),
                "original_map": dict(original_map_all),
                "nosep_to_primary": dict(nosep_to_primary),
                "agg_code_file_pages": agg_serializable,
                "pages_per_file": int(pages_per_file),
                "scale_to_a3": bool(scale_to_a3),
                "treat_survey": bool(treat_survey),
                "survey_size_limit_bytes": int(survey_size_limit_bytes),
                "dedupe_pages": bool(dedupe_pages),
                "skip_summary": bool(skip_summary),
                "summary_threshold": int(summary_threshold),
            })

        except Exception as e:
            post("error", f"Unexpected error: {e}")
        finally:
            post("done", None)

    # message pump (UI thread)
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
                    pretty_code, file_name, page_num = payload
                    key = (file_name, page_num)
                    self.main_page_codes[key].add(pretty_code)
                    codes_str = ", ".join(sorted(self.main_page_codes[key]))
                    if key in self.main_row_iid:
                        iid = self.main_row_iid[key]
                        self.tree.set(iid, "codes_on_page", codes_str)
                    else:
                        iid = self.tree.insert("", "end", values=(pretty_code, file_name, page_num, codes_str))
                        self.main_row_iid[key] = iid

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

    # finalize: survey-dup, de-dup drawings, skip summary (with new gating), combine, CSVs
    def _finalize_and_save(self, bundle):
        processed = bundle["processed"]
        root_name = bundle["root_name"]
        week_number = bundle["week_number"]
        out_dir = bundle["out_dir"]
        use_text_annotations = bool(bundle.get("use_text_annotations", True))
        ecs_primary = set(bundle.get("ecs_primary", []))
        original_map = dict(bundle.get("original_map", {}))
        nosep_to_primary = dict(bundle.get("nosep_to_primary", {}))
        agg_code_file_pages = dict(bundle.get("agg_code_file_pages", {}))
        pages_per_file = max(1, int(bundle.get("pages_per_file", 20)))
        scale_to_a3 = bool(bundle.get("scale_to_a3", False))
        treat_survey = bool(bundle.get("treat_survey", True))
        survey_size_limit_bytes = int(bundle.get("survey_size_limit_bytes", 1_200_000))
        dedupe_pages = bool(bundle.get("dedupe_pages", True))
        skip_summary = bool(bundle.get("skip_summary", True))
        summary_threshold = int(bundle.get("summary_threshold", 15))

        if not processed:
            messagebox.showinfo("No Matches", "No pages matched; nothing to save.")
            self.lbl_status.config(text="No matches.")
            self._write_not_surveyed_csv(out_dir, root_name, week_number,
                                         [original_map.get(p, p) for p in sorted(ecs_primary)])
            return

        used_review = bool(self.review_pages_var.get())
        if used_review:
            items = [{
                "display": p["display"],
                "pdf_path": p["pdf_path"],
                "hit_pages": p["hit_pages"],
                "rects_by_page": p["rects_by_page"],
                "page_codes": p.get("page_codes", {})
            } for p in processed]
            dlg = ReviewDialog(self, items)
            self.wait_window(dlg)
            if dlg.selection is None:
                self.lbl_status.config(text="Review canceled.")
                return

            if isinstance(dlg.selection, dict) and "keep_map" in dlg.selection:
                keep_map = dlg.selection["keep_map"]
                ordered_kept = dlg.selection.get("sequence", [])
            else:
                keep_map = dlg.selection
                ordered_kept = None
        else:
            keep_map = {p["pdf_path"]: set(p["hit_pages"]) for p in processed}
            ordered_kept = None

        building_buckets = defaultdict(list)
        seen_hashes = set()  # for de-dup

        # --- PATCHED: summary rule gating (not for survey PDFs and not for files ≤ 2MB) ---
        def add_item_if_ok(pdf_path, pg, rects, pretty_codes_for_pg, is_survey: bool):
            apply_summary_rule = False
            if skip_summary and not is_survey:
                try:
                    size_bytes = os.path.getsize(pdf_path)
                except Exception:
                    size_bytes = 0
                apply_summary_rule = size_bytes > 2_000_000  # Only apply to larger, non-survey PDFs

            if apply_summary_rule:
                if is_summary_like(pdf_path, pg, set(pretty_codes_for_pg), threshold=summary_threshold):
                    return

            if dedupe_pages:
                fp = page_fingerprint(pdf_path, pg)
                if fp in seen_hashes:
                    return
                seen_hashes.add(fp)

            bld = "UNKWN"
            if pretty_codes_for_pg:
                inferred = [infer_building_from_code(c) for c in pretty_codes_for_pg]
                cnt = Counter(inferred)
                max_freq = max(cnt.values())
                bld = sorted([b for b, f in cnt.items() if f == max_freq])[0]
            building_buckets[bld].append({
                "pdf_path": pdf_path,
                "page_idx": pg,
                "rects": rects
            })

        for p in processed:
            pdf_path = p["pdf_path"]
            rects_by_page = p["rects_by_page"]
            code_rects_by_page = p["code_rects_by_page"]  # page -> cmp_key -> [rect]
            page_codes = p.get("page_codes", {})
            keep_pages = sorted(list(keep_map.get(pdf_path, set())))
            is_survey = treat_survey and is_survey_pdf(pdf_path, size_limit_bytes=survey_size_limit_bytes)

            for pg in keep_pages:
                pretty_codes = page_codes.get(pg, [])
                if is_survey and pretty_codes:
                    # duplicate the page per ECS code, filtering rects to that code only
                    for pretty in sorted(pretty_codes):
                        cmp_key = normalize_nosep(pretty)
                        per_code_rects = code_rects_by_page.get(pg, {}).get(cmp_key, [])
                        if not per_code_rects:
                            # fallback: at least keep rectangles for page if per-code missing
                            per_code_rects = rects_by_page.get(pg, [])
                        add_item_if_ok(pdf_path, pg, per_code_rects, [pretty], is_survey)
                else:
                    rects = rects_by_page.get(pg, [])
                    add_item_if_ok(pdf_path, pg, rects, pretty_codes, is_survey)

        # Respect manual order per bucket if provided
        if ordered_kept:
            new_buckets = defaultdict(list)
            for (pdf_path, pg) in ordered_kept:
                for bld, lst in building_buckets.items():
                    for it in lst:
                        if it["pdf_path"] == pdf_path and it["page_idx"] == pg:
                            new_buckets[bld].append(it)
            for bld, lst in building_buckets.items():
                for it in lst:
                    if it not in new_buckets[bld]:
                        new_buckets[bld].append(it)
            building_buckets = new_buckets
        else:
            for bld, lst in building_buckets.items():
                lst.sort(key=lambda it: (os.path.basename(it["pdf_path"]).lower(), it["page_idx"]))

        saved_files = []
        for bld, lst in sorted(building_buckets.items(), key=lambda kv: kv[0]):
            if not lst:
                continue
            part_idx = 1
            for chunk in chunk_list(lst, pages_per_file):
                tag = sanitize_filename(root_name) or "Job"
                fname = f"{tag}_{bld}_Highlighted_WK{week_number}_part{part_idx}.pdf"
                out_path = os.path.join(out_dir, fname)
                try:
                    final_path = combine_pages_to_new(out_path, chunk,
                                                      use_text_annotations=use_text_annotations,
                                                      scale_to_a3=scale_to_a3)
                    if final_path:
                        saved_files.append(final_path)
                except Exception as e:
                    messagebox.showerror("Combine", f"Could not save {fname}:\n{e}")
                part_idx += 1

        if saved_files:
            self.lbl_status.config(text=f"Saved {len(saved_files)} file(s)")
            messagebox.showinfo("Done", "Outputs saved:\n" + "\n".join(saved_files))
        else:
            self.lbl_status.config(text="No output files saved.")

        # Build per-code summary (distinct pages)
        primary_file_pages = defaultdict(lambda: defaultdict(set))
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

        missing_primary = sorted(list(ecs_primary - found_primary))
        summary_csv = self._write_summary_csv(out_dir, root_name, week_number, rows)
        self._write_not_surveyed_csv(out_dir, root_name, week_number,
                                     [original_map.get(p, p) for p in missing_primary])

        SummaryDialog(self, rows, len(missing_primary), summary_csv)

    def _write_summary_csv(self, out_dir, root_name, week_number, rows):
        tag = sanitize_filename(root_name) or "Job"
        csv_path = os.path.join(out_dir, f"{tag}_MatchesSummary_WK{week_number}.csv")
        csv_path = uniquify_path(csv_path)
        try:
            pd.DataFrame(rows, columns=["code", "total_pages", "breakdown"]).to_csv(csv_path, index=False)
        except Exception as e:
            messagebox.showwarning("CSV", f"Could not save MatchesSummary CSV:\n{e}")
        return csv_path

    def _write_not_surveyed_csv(self, out_dir, root_name, week_number, not_found_pretty_list):
        if not not_found_pretty_list:
            return None
        tag = sanitize_filename(root_name) or "Job"
        csv_path = os.path.join(out_dir, f"{tag}_NotSurveyed_WK{week_number}.csv")
        csv_path = uniquify_path(csv_path)
        try:
            pd.DataFrame({"ECS_Code_Not_Found": sorted(not_found_pretty_list)}).to_csv(csv_path, index=False)
            self.lbl_status.config(text=f"CSV saved: {os.path.basename(csv_path)}")
        except Exception as e:
            messagebox.showwarning("CSV", f"Could not save NotSurveyed CSV:\n{e}")
        return csv_path

# ================================ main =================================

if __name__ == "__main__":
    multiprocessing.freeze_support()
    try:
        app = HighlighterApp()
        app.mainloop()
    except Exception as e:
        try:
            messagebox.showerror("Fatal Error", str(e))
        except Exception:
            pass
        sys.exit(1)
