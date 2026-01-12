# =========================================================
# EARLY STARTUP LOGGER (MUST BE FIRST THING IN THE FILE)
# Creates startup.log next to the .exe (when frozen) or next to the .py
# =========================================================
import os
import sys
import traceback
from datetime import datetime

def _get_app_dir() -> str:
    """Return directory for logs: exe dir when frozen, else script dir."""
    try:
        if getattr(sys, "frozen", False) and hasattr(sys, "executable"):
            return os.path.dirname(os.path.abspath(sys.executable))
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.getcwd()

_APP_DIR = _get_app_dir()
_STARTUP_LOG = os.path.join(_APP_DIR, "startup.log")

def _log(line: str) -> None:
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(_STARTUP_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {line}\n")
    except Exception:
        pass

def _log_exception(prefix: str, exc: BaseException) -> None:
    try:
        _log(f"{prefix}: {repr(exc)}")
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        for ln in tb.splitlines():
            _log(ln)
    except Exception:
        pass

def _early_bootstrap() -> None:
    _log("==== APP START ====")
    _log(f"sys.version={sys.version}")
    _log(f"frozen={getattr(sys, 'frozen', False)}")
    _log(f"executable={getattr(sys, 'executable', None)}")
    _log(f"cwd={os.getcwd()}")
    _log(f"app_dir={_APP_DIR}")
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        _log(f"_MEIPASS={meipass}")

    def _excepthook(exc_type, exc, tb):
        try:
            _log("UNCAUGHT EXCEPTION (sys.excepthook)")
            tb_txt = "".join(traceback.format_exception(exc_type, exc, tb))
            for ln in tb_txt.splitlines():
                _log(ln)
        except Exception:
            pass

    sys.excepthook = _excepthook

_early_bootstrap()

import os
import re
import sys
import base64
import hashlib
import threading
import queue
import tempfile
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import fitz  # PyMuPDF
import pandas as pd

from collections import defaultdict, Counter, deque
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import bisect
from typing import List, Dict, Optional

# ======================= Normalização & utilitários =======================
DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2212"  # -, ‐, -, ‒, –, —, −
_STRIP_EDGE_PUNCT = re.compile(r'^[\s\"\'\(\)\[\]\{\}:;,.–—\-]+|[\s\"\'\(\)\[\]\{\}:;,.–—\-]+$')

# A3 (72 pt/in)
A3_PORTRAIT = (842.0, 1191.0)
A3_LANDSCAPE = (1191.0, 842.0)

# “Summary-like”
SUMMARY_KEYWORDS = [
    "summary", "contents", "index", "bill of materials", "bom",
    "schedule", "table of contents", "support schedule", "legend"
]
SUMMARY_KEYWORDS_RE = [re.compile(rf"\b{re.escape(k)}\b", re.IGNORECASE) for k in SUMMARY_KEYWORDS]

EMIT_DIAGNOSTICS = False  # opcional: CSV extra de diagnóstico

# === Summary/TOC sem gate de tamanho ===
SUMMARY_TOC_RE = [
    re.compile(r"\bsummary\b", re.IGNORECASE),
    re.compile(r"\btable of contents\b", re.IGNORECASE),
]


def is_summary_keyword_page(pdf_path, page_idx, first_pages_only=7):
    """True se a página contém 'summary' ou 'table of contents'. Checa apenas as N primeiras por padrão."""
    try:
        if first_pages_only and first_pages_only > 0 and page_idx >= first_pages_only:
            return False
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            txt = (pg.get_text("text") or "")
            for rx in SUMMARY_TOC_RE:
                if rx.search(txt):
                    return True
    except Exception:
        return False
    return False


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
    # Mantém apenas [0-9a-z-]
    return re.sub(r'[^0-9a-z\-]', '', token)


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name).strip()


def uniquify_path(path: str) -> str:
    base, ext = os.path.splitext(path)
    out = path
    i = 1
    while os.path.exists(out):
        out = f"{base} ({i}){ext}"
        i += 1
    return out


# ========================== Excel & lista de ECS ==========================
def load_table_with_dynamic_header(xlsx_path, sheet_name=None):
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, dtype=str, engine="openpyxl")
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
    # Split robusto
    tokens = []
    for v in raw:
        parts = re.split(r"[,;\n/\t ]+", v)
        for p in parts:
            t = p.strip().strip('"\'')
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


def build_contextual_indexes(ecs_primary_all: set):
    """
    Contextual ECS matching rules:

    - Surveys: unit-aware -> match FULL normalized code only (e.g. '1hk10st4072')
    - Drawings: unitless -> match WITHOUT the leading unit digit (e.g. 'hk10st4072')
      plus a leading-dash variant (e.g. '-hk10st4072') to match drawings that show codes with a leading '-'.

    - EXCEPTION: HG systems (0HG/1HG/9HG):
        * Drawings are unit-aware for HG -> match FULL code (e.g. '1hg0101zl...')
        * BUT if the drawing shows a leading '-' (e.g. '-hg0101zl...'), it can match ANY unit.
          In that case we allow '-'+remainder and map it to ALL HG primaries that share the remainder.

    Returns:
        cmp_keys_survey: set[str]
        cmp_keys_drawing: set[str]
        cmp_to_primaries: dict[str, list[str]]  (one cmp-key can map to multiple unit-specific primaries)
        max_code_len: int
    """
    cmp_survey = set()
    cmp_drawing = set()
    cmp_to_primaries: Dict[str, List[str]] = {}

    def _add_map(k: str, primary: str, target_set: set):
        if not k:
            return
        target_set.add(k)
        cmp_to_primaries.setdefault(k, [])
        if primary not in cmp_to_primaries[k]:
            cmp_to_primaries[k].append(primary)

    for primary in ecs_primary_all:
        full = normalize_nosep(primary)
        if not full:
            continue

        # Survey: always unit-aware (full code)
        _add_map(full, primary, cmp_survey)

        # Determine HG (e.g. 0HG/1HG/9HG)
        is_hg = bool(len(full) >= 3 and full[0].isdigit() and full[1:3] == "hg")

        # Build remainder (strip leading unit digit if present)
        if primary and str(primary)[0].isdigit():
            remainder = normalize_nosep(str(primary)[1:])
        else:
            remainder = full

        remainder = (remainder or "").lstrip("-")
        if not remainder:
            continue

        if is_hg:
            # HG drawings must match the unit (full code)
            _add_map(full, primary, cmp_drawing)
            # But leading '-' in drawings can match any unit
            _add_map("-" + remainder, primary, cmp_drawing)
        else:
            # Non-HG drawings are unitless
            _add_map(remainder, primary, cmp_drawing)
            _add_map("-" + remainder, primary, cmp_drawing)

    max_code_len = 0
    if cmp_survey or cmp_drawing:
        max_code_len = max((len(k) for k in (cmp_survey | cmp_drawing)), default=0)
    return cmp_survey, cmp_drawing, cmp_to_primaries, max_code_len


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


# ========================= Turbo (Aho–Corasick) =========================
try:
    import ahocorasick  # pyahocorasick
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





# ====================== Survey: highlight linha inteira ======================
def _survey_line_span_rects(words_norm_list, target_rect, y_tol: float = 8.0, margin: float = 1.5):
    """Return ONE rect that covers the whole *row* (minX..maxX of row words).

    Avoids full-page-width highlights while still behaving like 'full row'
    even when columns are separated by big gaps.
    y_tol is intentionally larger to handle different PDF generators where columns have small Y offsets.
    """
    try:
        tx0, ty0, tx1, ty1 = target_rect
        tcy = (ty0 + ty1) * 0.5

        row = []
        for (x0, y0, x1, y1, _norm, _raw) in (words_norm_list or []):
            cy = (y0 + y1) * 0.5
            if abs(cy - tcy) <= y_tol:
                row.append((x0, y0, x1, y1))
                continue
            ov = min(ty1, y1) - max(ty0, y0)
            if ov > 0 and ov >= 0.35 * min((ty1 - ty0), (y1 - y0)):
                row.append((x0, y0, x1, y1))

        if not row:
            return [target_rect]

        y0 = max(0.0, min(r[1] for r in row) - margin)
        y1 = max(r[3] for r in row) + margin
        x0 = max(0.0, min(r[0] for r in row) - margin)
        x1 = max(r[2] for r in row) + margin

        if x1 <= x0 or y1 <= y0:
            return [target_rect]
        return [(float(x0), float(y0), float(x1), float(y1))]
    except Exception:
        return [target_rect]
# ============================ Scanners de PDF ===========================
def scan_pdf_for_rects_fallback(
    pdf_path,
    cmp_keys_nosep,
    max_code_len,
    cancel_flag,
    highlight_all_occurrences=False,
    survey_full_line=False,
    prefixes=None,
    first_chars=None
):
    """Fallback (por palavras + janela multi-palavra + fallback por texto corrido)."""
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
    code_rects_by_page = defaultdict(lambda: defaultdict(list))
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


            words_norm_list = [(rx0, ry0, rx1, ry1, nn, rr) for ((rx0, ry0, rx1, ry1), nn, rr) in W]

            page_rects = []
            rect_key_set = set()

            if W:
                # 1) match por palavra (substring)
                for (x0, y0, x1, y1), norm, raw in W:
                    cands = idx_by_first.get(norm[0], [])
                    for k in cands:
                        if len(k) > len(norm):
                            continue
                        if k in norm:
                            code_pages[k].add(page.number)
                            matched.add(k)
                            rkey = (round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2))
                            if rkey not in rect_key_set:
                                rect_key_set.add(rkey)
                                
                                if survey_full_line:
                                    spans = _survey_line_span_rects(words_norm_list, (x0, y0, x1, y1))
                                    for (sx0, sy0, sx1, sy1) in spans:
                                        rkey2 = (round(sx0, 2), round(sy0, 2), round(sx1, 2), round(sy1, 2))
                                        if rkey2 not in rect_key_set:
                                            rect_key_set.add(rkey2)
                                            page_rects.append((sx0, sy0, sx1, sy1))
                                        code_rects_by_page[page.number][k].append((sx0, sy0, sx1, sy1))
                                else:
                                    page_rects.append((x0, y0, x1, y1))
                                    code_rects_by_page[page.number][k].append((x0, y0, x1, y1))
                            hits += 1

                # 2) janela multi-palavra (até 20)
                N = len(W)
                for i in range(N):
                    if cancel_flag.is_set():
                        break
                    if not W[i][1] or W[i][1][0] not in first_chars:
                        continue
                    parts = []
                    rects_run = []
                    for j in range(i, min(i + 20, N)):
                        rect, norm, raw = W[j]
                        parts.append(norm)
                        rects_run.append(rect)
                        s = "".join(parts)
                        if len(s) > max_code_len + 6:
                            break
                        if s not in prefixes:
                            break
                        if s in cmp_keys_nosep:
                            code_pages[s].add(page.number)
                            matched.add(s)
                            
                            if survey_full_line:
                                rx0, ry0, rx1, ry1 = rects_run[0]
                                spans = _survey_line_span_rects(words_norm_list, (rx0, ry0, rx1, ry1))
                                for (sx0, sy0, sx1, sy1) in spans:
                                    rkey2 = (round(sx0, 2), round(sy0, 2), round(sx1, 2), round(sy1, 2))
                                    if rkey2 not in rect_key_set:
                                        rect_key_set.add(rkey2)
                                        page_rects.append((sx0, sy0, sx1, sy1))
                                    code_rects_by_page[page.number][s].append((sx0, sy0, sx1, sy1))
                            else:
                                for (rx0, ry0, rx1, ry1) in rects_run:
                                    rkey = (round(rx0, 2), round(ry0, 2), round(rx1, 2), round(ry1, 2))
                                    if rkey not in rect_key_set:
                                        rect_key_set.add(rkey)
                                        page_rects.append((rx0, ry0, rx1, ry1))
                                    code_rects_by_page[page.number][s].append((rx0, ry0, rx1, ry1))
                            hits += 1

            # 3) fallback texto corrido (marca página mesmo sem rects)
            if not page_rects:
                txt = (page.get_text("text") or "")
                S_flat = normalize_nosep(txt)
                for k in cmp_keys_nosep:
                    if k and (k in S_flat):
                        code_pages[k].add(page.number)
                        matched.add(k)
                        hits += 1
            else:
                rects_by_page[page.number] = page_rects

        return hits, matched, rects_by_page, code_pages, code_rects_by_page, doc.page_count
    finally:
        doc.close()


def scan_pdf_for_rects_ac(
    pdf_path,
    automaton,
    cancel_flag,
    highlight_all_occurrences=False,
    survey_full_line=False
):
    """Scanner Aho–Corasick (por palavras)."""
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
            raws = []
            for w in words:
                x0, y0, x1, y1, t = float(w[0]), float(w[1]), float(w[2]), float(w[3]), (w[4] or "")
                n = normalize_nosep(t)
                if not n:
                    continue
                rects.append((x0, y0, x1, y1))
                norms.append(n)
                raws.append(t)

            if not norms:
                continue

            words_norm_list = [(r[0], r[1], r[2], r[3], nn, rr) for r, nn, rr in zip(rects, norms, raws)]
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
                
                if survey_full_line:
                    x0, y0, x1, y1 = rects[ws]
                    spans = _survey_line_span_rects(words_norm_list, (x0, y0, x1, y1))
                    for (sx0, sy0, sx1, sy1) in spans:
                        rkey2 = (round(sx0, 2), round(sy0, 2), round(sx1, 2), round(sy1, 2))
                        if rkey2 not in rect_key_set:
                            rect_key_set.add(rkey2)
                            page_rects.append((sx0, sy0, sx1, sy1))
                        code_rects_by_page[page.number][key].append((sx0, sy0, sx1, sy1))
                else:
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


# ========================= Anotar & combinar ============================
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



def stamp_filename_top_left(page, filename: str, margin: float = 28.346, fontsize: float = 9.0):
    """Stamp survey filename (without .pdf) at top-left.

    Requirements:
      - Font size = 9
      - Margin = 10 mm from edge (≈ 28.346 pt)
      - No background box
    Uses Arial if available, else Helvetica.
    """
    if not filename:
        return
    try:
        name = os.path.splitext(os.path.basename(str(filename)))[0]
        if not name:
            return

        x = float(margin)
        y = float(margin) + float(fontsize)

        arial = r"C:\\Windows\\Fonts\\arial.ttf"
        if os.path.exists(arial):
            page.insert_text((x, y), name, fontsize=fontsize, fontfile=arial)
        else:
            page.insert_text((x, y), name, fontsize=fontsize, fontname="helv")
    except Exception:
        pass


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


def combine_pages_to_new(out_path, page_units, use_text_annotations=True, scale_to_a3=False):
    """
    Insere as páginas EXATAMENTE na ordem dada por page_units (sem agrupar por PDF).
    Cada unit: { 'pdf_path', 'page_idx', 'rects' }
    Corrigido: evita rotação inesperada usando insert_pdf no modo "tamanho original" e
    usa bound() para decidir orientação quando escala para A3.
    """
    out = fitz.open()
    src_cache = {}
    try:
        for it in page_units:
            pdf_path = it["pdf_path"]
            pg_idx = it["page_idx"]
            rects = it.get("rects", [])

            if pdf_path not in src_cache:
                src_cache[pdf_path] = fitz.open(pdf_path)
            src = src_cache[pdf_path]
            src_pg = src.load_page(pg_idx)

            if not scale_to_a3:
                # Copia a página exatamente como está (mantém rotação/crop/coords)
                out.insert_pdf(src, from_page=pg_idx, to_page=pg_idx)
                out_pg = out.load_page(out.page_count - 1)
                # --- Survey orientation normalization ---
                if it.get("type") == "Survey":
                    try:
                        # If the page is portrait with rotation 0, rotate for landscape display
                        if out_pg.rotation == 0 and out_pg.rect.height > out_pg.rect.width:
                            out_pg.set_rotation(90)
                        # If the page has rotation 90/270, align cropbox so text coordinates stay consistent
                        if out_pg.rotation in (90, 270):
                            w = float(out_pg.rect.height)
                            h = float(out_pg.rect.width)
                            out_pg.set_cropbox(fitz.Rect(0, 0, w, h))
                    except Exception:
                        pass

                # Stamp survey filename at top-left (Arial 12). Uses file name without .pdf
                if it.get("type") == "Survey":
                    stamp_filename_top_left(out_pg, it.get("display") or os.path.basename(pdf_path), margin=100, fontsize=12)
                if use_text_annotations and rects:
                    add_text_highlights(out_pg, rects, color=(1, 1, 0), opacity=0.35)
            else:
                # Para A3, usar dimensões VISUAIS (respeita rotação/crop)
                b = src_pg.bound()
                sw, sh = float(b.width), float(b.height)
                src_landscape = sw >= sh
                tw, th = (A3_LANDSCAPE if src_landscape else A3_PORTRAIT)
                out_pg = out.new_page(width=tw, height=th)
                dst_rect = fitz.Rect(0, 0, tw, th)
                out_pg.show_pdf_page(dst_rect, src, pg_idx)
                # Stamp survey filename at top-left (Arial 12). Uses file name without .pdf
                if it.get("type") == "Survey":
                    stamp_filename_top_left(out_pg, it.get("display") or os.path.basename(pdf_path), margin=100, fontsize=12)
                # Ajustar rects para o novo tamanho
                s, dx, dy = _fit_scale_and_offset(sw, sh, tw, th)
                if use_text_annotations and rects:
                    xfm_rects = []
                    for (x0, y0, x1, y1) in rects:
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
        for doc in src_cache.values():
            try:
                doc.close()
            except Exception:
                pass
        out.close()


def chunk_list(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


# ====================== Regras de prédio (agrupamento) ==================
_FIRST_LETTERS = re.compile(r'[a-z]+', re.I)


def infer_building_from_code(pretty_code: str) -> str:
    s = unify_dashes(pretty_code or "").strip()
    if not s:
        return "UNKWN"
    s_no = re.sub(r'[^0-9A-Za-z\-]', '', s)
    # Regra simples: se tiver hífen cedo, 2 letras; senão 3 letras
    t = re.sub(r'^[0-9\-]+', '', s_no)
    m2 = _FIRST_LETTERS.match(t)
    if not m2:
        return "UNKWN"
    letters = m2.group(0).upper()
    if '-' in s_no[:5]:
        return letters[:2] if len(letters) >= 2 else letters
    return letters[:3] if len(letters) >= 3 else letters


# ========================= Survey / Duplicatas / Rev ====================
def is_survey_pdf(path, size_limit_bytes=1_200_000):
    try:
        sz = os.path.getsize(path)
    except Exception:
        sz = 0
    name = os.path.basename(path).lower()
    looks_name = ("cut l" in name) or ("cut length report" in name)
    return looks_name and (sz > 0 and sz <= size_limit_bytes)


# Fingerprints de página
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


# -------- Detecção de summary --------
_NEIGHBOR_DELTAS = (-4, -3, -2, -1, 1, 2, 3, 4)
_TRAILING_NUM_RE = re.compile(r"^(.*?)(\d+)$", re.IGNORECASE)


def _neighbors_of_code_norm(norm_code: str):
    m = _TRAILING_NUM_RE.match(norm_code)
    if not m:
        return []
    prefix, num = m.groups()
    width = len(num)
    base = int(num)
    out = []
    for d in _NEIGHBOR_DELTAS:
        n = base + d
        if n < 0:
            continue
        out.append(f"{prefix}{str(n).zfill(width)}")
    return out


def looks_like_summary_by_neighbors(codes_on_page_pretty, neighbor_min_hits=3):
    norm_set = {normalize_base(c) for c in codes_on_page_pretty if c}
    if not norm_set:
        return False
    for norm_code in norm_set:
        neighbors = _neighbors_of_code_norm(norm_code)
        if not neighbors:
            continue
        hits = sum(1 for nc in neighbors if nc in norm_set)
        if hits >= neighbor_min_hits:
            return True
    return False


def is_summary_like(pdf_path, page_idx, codes_on_page, threshold=15, neighbor_min_hits=3):
    """
    Heurística para páginas 'summary' (não usada para surveys).
    """
    try:
        if codes_on_page and looks_like_summary_by_neighbors(codes_on_page, neighbor_min_hits=neighbor_min_hits):
            return True
        if len(set(codes_on_page or ())) >= threshold:
            return True
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            txt = (pg.get_text("text") or "")
            for rx in SUMMARY_KEYWORDS_RE:
                if rx.search(txt):
                    return True
    except Exception:
        return False
    return False


# -------- Escolha da última revisão (survey & handbooks/desenhos) --------
_REV_LET_RE = re.compile(r'(?:^|[^A-Z0-9])REV\s*([A-Z]{1,3})(?:[^A-Z0-9]|$)', re.IGNORECASE)
_REV_NUM_RE = re.compile(r'(?:^|[^0-9])REV\s*([0-9]{1,3})(?:[^0-9]|$)', re.IGNORECASE)
_TAIL_LET_RE = re.compile(r'([_\-])([A-Z]{1,3})(?:\.pdf)?$', re.IGNORECASE)



def _rev_letters_to_int(s: str) -> int:
    """Convert Excel-style revision letters to an integer: A=1, ..., Z=26, AA=27, AB=28, ..."""
    if not s:
        return 0
    s = re.sub(r'[^A-Z]', '', str(s).upper())
    if not s:
        return 0
    val = 0
    for ch in s:
        if 'A' <= ch <= 'Z':
            val = val * 26 + (ord(ch) - ord('A') + 1)
    return val

def _parse_revision_from_name(name: str) -> int:
    if not name:
        return 0
    m = _REV_LET_RE.search(name)
    if m:
        # Letters: A..Z, AA.. etc (Excel-style ordering)
        return 1000 + _rev_letters_to_int(m.group(1))
    m = _REV_NUM_RE.search(name)
    if m:
        return int(m.group(1))
    m = _TAIL_LET_RE.search(os.path.splitext(name)[0])
    if m:
        return 1000 + _rev_letters_to_int(m.group(2))
    return 0



def _strip_revision_tokens(name: str) -> str:
    base = _REV_LET_RE.sub(' ', name)
    base = _REV_NUM_RE.sub(' ', base)
    base = _TAIL_LET_RE.sub('', os.path.splitext(base)[0])
    base = re.sub(r'\s+', ' ', base).strip().lower()
    return base


def select_latest_survey_revisions(pdf_paths: list) -> list:
    keep = []
    groups = {}
    for p in pdf_paths:
        fname = os.path.basename(p)
        if is_survey_pdf(p):
            base = _strip_revision_tokens(fname)
            rev = _parse_revision_from_name(fname)
            cur = groups.get(base)
            if cur is None or rev > cur[1]:
                groups[base] = (p, rev)
        else:
            keep.append(p)
    keep.extend([v[0] for v in groups.values()])
    keep = sorted(set(keep), key=lambda x: os.path.basename(x).lower())
    return keep


def select_latest_non_survey_revisions(pdf_paths: list) -> list:
    groups = {}
    for p in pdf_paths:
        if is_survey_pdf(p):
            continue
        fname = os.path.basename(p)
        base = _strip_revision_tokens(fname)
        rev = _parse_revision_from_name(fname)
        cur = groups.get(base)
        if cur is None or rev > cur[1]:
            groups[base] = (p, rev)
    out = [p for p in pdf_paths if is_survey_pdf(p)]
    out.extend(v[0] for v in groups.values())
    out = sorted(set(out), key=lambda x: os.path.basename(x).lower())
    return out




def select_latest_revisions_any(pdf_paths: list) -> list:
    """Keep only the latest REV per base filename (generic, no survey/drawing classification)."""
    groups = {}
    for p in pdf_paths:
        fname = os.path.basename(p)
        base = _strip_revision_tokens(fname)
        rev = _parse_revision_from_name(fname)
        cur = groups.get(base)
        if cur is None or rev > cur[1]:
            groups[base] = (p, rev)
    out = [v[0] for v in groups.values()]
    out = sorted(set(out), key=lambda x: os.path.basename(x).lower())
    return out

# ========================== Tarefa do worker (MP) =======================
class _DummyCancel:
    def is_set(self): return False


def _process_pdf_task(args):
    (
        pdf_path,
        cmp_keys_list,
        use_ac,
        highlight_all_occurrences,
        survey_full_line
    ) = args
    cancel_flag = _DummyCancel()
    cmp_keys = set(cmp_keys_list)
    try:
        if use_ac and _HAS_AC:
            automaton = build_aho_automaton(cmp_keys)
            hits, matched, rects_by_page, code_pages, code_rects_by_page, total_pages = scan_pdf_for_rects_ac(
                pdf_path=pdf_path,
                automaton=automaton,
                cancel_flag=cancel_flag,
                highlight_all_occurrences=highlight_all_occurrences,
                survey_full_line=bool(survey_full_line)
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
                survey_full_line=bool(survey_full_line),
                prefixes=prefixes,
                first_chars=first_chars
            )

        rects_by_page_ser = {int(k): [tuple(r) for r in v] for k, v in rects_by_page.items()}
        code_pages_ser = {k: sorted(list(v)) for k, v in code_pages.items()}

        code_rects_ser = {}
        for p, mp in code_rects_by_page.items():
            code_rects_ser[int(p)] = {}
            for ck, rect_list in mp.items():
                safe_list = []
                if isinstance(rect_list, list):
                    for r in rect_list:
                        try:
                            safe_list.append(tuple(r))
                        except Exception:
                            pass
                code_rects_ser[int(p)][ck] = safe_list

        match_pairs = []
        for k, pages in code_pages_ser.items():
            for p in pages:
                match_pairs.append((k, p + 1))

        return {
            "pdf_path": pdf_path,
            "display": os.path.basename(pdf_path),
            "hits": int(sum(len(v) for v in rects_by_page_ser.values())),
            "rects_by_page": rects_by_page_ser,
            "code_pages": code_pages_ser,
            "code_rects_by_page": code_rects_ser,
            "hit_pages": sorted(list(rects_by_page_ser.keys())),
            "total_pages": int(total_pages),
            "match_pairs": match_pairs
        }
    except Exception as e:
        return {
            "pdf_path": pdf_path,
            "display": os.path.basename(pdf_path),
            "error": str(e)
        }


# ====================== Conversão e mapeamento de ITR ====================
def try_convert_docx_to_pdf(docx_path: str) -> Optional[str]:
    """Converte DOCX para PDF usando docx2pdf (se disponível). Retorna caminho do PDF ou None se falhar."""
    try:
        from docx2pdf import convert  # type: ignore
    except Exception:
        return None
    try:
        tmp_dir = tempfile.mkdtemp(prefix="itr_pdf_")
        out_pdf = os.path.join(tmp_dir, os.path.splitext(os.path.basename(docx_path))[0] + ".pdf")
        convert(docx_path, out_pdf)
        return out_pdf if os.path.exists(out_pdf) else None
    except Exception:
        return None


def build_itr_map(itr_paths: List[str], cmp_keys: set, nosep_to_primary: Dict[str, str]) -> Dict[str, Dict]:
    """
    Produz um dicionário: primary_code -> {'pdf_path': <pdf>, 'pages': int}
    Faz match do ECS code procurando qualquer cmp_key dentro do nome do arquivo.
    """
    out = {}
    for p in itr_paths:
        ext = os.path.splitext(p)[1].lower()
        if ext == ".docx":
            pdfp = try_convert_docx_to_pdf(p)
            if not pdfp:
                messagebox.showwarning("ITR DOCX", f"Não foi possível converter ITR: {os.path.basename(p)}. "
                                                   f"Instale 'docx2pdf' (requer Microsoft Word).")
                continue
            mapped_pdf = pdfp
        elif ext == ".pdf":
            mapped_pdf = p
        else:
            messagebox.showwarning("ITR", f"Formato não suportado (somente .docx ou .pdf): {os.path.basename(p)}")
            continue

        fname_n = normalize_nosep(os.path.basename(p))
        matched_primary = None
        for k in cmp_keys:
            if not k:
                continue
            if k in fname_n:
                matched_primary = nosep_to_primary.get(k, k)
                break
        if not matched_primary:
            # não achou nenhum cmp_key no nome
            continue

        try:
            with fitz.open(mapped_pdf) as doc:
                pages = doc.page_count
        except Exception:
            pages = 0

        out[matched_primary] = {"pdf_path": mapped_pdf, "pages": pages}
    return out


# ============================ UI: Review Dialog (v3) ====================
class ReviewDialog(tk.Toplevel):
    """
    Review baseado em UNIDADES (linha por página + tipo + código).
    Tipos: Survey, Drawing, ITR. Ordem exibida = ordem que vai para o output.
    """
    def __init__(self, master, units):
        super().__init__(master)
        self.title("Review pages — interleaved S–D–ITR by code")
        self.geometry("1200x740")
        self.minsize(1080, 660)
        self.transient(master)
        self.grab_set()

        self.units = list(units)  # lista de dicts (display, pdf_path, page_idx, code_pretty, rects, type)
        self.keep_idx = set(range(len(self.units)))

        paned = ttk.Panedwindow(self, orient="horizontal")
        paned.pack(fill="both", expand=True, padx=8, pady=8)
        left = ttk.Frame(paned)
        right = ttk.Frame(paned)
        paned.add(left, weight=3)
        paned.add(right, weight=2)

        ttk.Label(left, text="Pages (double-click to toggle keep). Click headers to sort.").pack(anchor="w")
        tree_frame = ttk.Frame(left)
        tree_frame.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(
            tree_frame,
            columns=("order", "keep", "type", "file", "page", "code"),
            show="headings",
            selectmode="browse",
            height=24
        )
        self.tree.heading("order", text="#")
        self.tree.heading("keep", text="Keep", command=lambda: self._sort_tree("keep"))
        self.tree.heading("type", text="Type", command=lambda: self._sort_tree("type"))  # Survey/Drawing/ITR
        self.tree.heading("file", text="File", command=lambda: self._sort_tree("file"))
        self.tree.heading("page", text="Page", command=lambda: self._sort_tree("page"))
        self.tree.heading("code", text="ECS Code", command=lambda: self._sort_tree("code"))
        self.tree.column("order", width=40, anchor="center")
        self.tree.column("keep", width=60, anchor="center")
        self.tree.column("type", width=90, anchor="center")
        self.tree.column("file", width=500, anchor="w")
        self.tree.column("page", width=70, anchor="center")
        self.tree.column("code", width=220, anchor="w")

        ybar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        self._row_iids = []  # índice -> iid
        self._rebuild_tree(self.units)

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
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)

        self.tree.bind("<Double-1>", self._toggle_keep)
        self.tree.bind("<<TreeviewSelect>>", self._preview_selected)
        if self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._preview_selected()

        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _rebuild_tree(self, units):
        self.tree.delete(*self.tree.get_children())
        self._row_iids.clear()
        for idx, it in enumerate(units, start=1):
            keep_txt = "[x]" if (idx-1) in self.keep_idx else "[ ]"
            typ = it.get("type", "Drawing")
            disp = it.get("display") or os.path.basename(it["pdf_path"])
            page1b = it["page_idx"] + 1
            code = it.get("code_pretty") or ""
            iid = self.tree.insert("", "end", values=(idx, keep_txt, typ, disp, page1b, code))
            self._row_iids.append(iid)

    def _rows_snapshot(self):
        rows = []
        for idx, iid in enumerate(self._row_iids):
            it = self.units[idx]
            keep = (idx in self.keep_idx)
            disp = it.get("display") or os.path.basename(it["pdf_path"])
            rows.append({
                "idx": idx,
                "keep": keep,
                "type": it.get("type", "Drawing"),
                "display": disp,
                "page": it["page_idx"],
                "code": it.get("code_pretty") or ""
            })
        return rows

    def _reapply_rows(self, rows):
        # reorganiza self.units e keep_idx segundo 'rows'
        new_units = []
        new_keep = set()
        for i, r in enumerate(rows):
            new_units.append(self.units[r["idx"]])
            if r["keep"]:
                new_keep.add(i)
        self.units = new_units
        self.keep_idx = new_keep
        self._rebuild_tree(self.units)

    def _sort_tree(self, column):
        rows = self._rows_snapshot()
        if column == "file":
            rows.sort(key=lambda r: (r["display"].lower(), r["page"]))
        elif column == "page":
            rows.sort(key=lambda r: r["page"])
        elif column == "keep":
            rows.sort(key=lambda r: ((not r["keep"]), r["display"].lower(), r["page"]))
        elif column == "type":
            rows.sort(key=lambda r: (r["type"], r["display"].lower(), r["page"]))
        elif column == "code":
            rows.sort(key=lambda r: (r["code"].lower(), r["display"].lower(), r["page"]))
        else:
            return
        self._reapply_rows(rows)

    def _toggle_keep(self, event=None):
        iid = self.tree.identify_row(event.y) if event else self.tree.focus()
        if not iid:
            return
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        if pos in self.keep_idx:
            self.keep_idx.remove(pos)
            self.tree.set(iid, "keep", "[ ]")
        else:
            self.keep_idx.add(pos)
            self.tree.set(iid, "keep", "[x]")

    def _select_all(self):
        self.keep_idx = set(range(len(self.units)))
        self._rebuild_tree(self.units)

    def _clear_all(self):
        self.keep_idx.clear()
        self._rebuild_tree(self.units)

    def _ok(self):
        # devolve sequência final nas unidades marcadas como keep
        seq = []
        for i, it in enumerate(self.units):
            if i in self.keep_idx:
                seq.append((it["pdf_path"], it["page_idx"], it.get("code_pretty"), it.get("rects", []), it.get("type", "Drawing")))
        self.selection = {"sequence": seq}
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
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        it = self.units[pos]
        pdf_path, page_idx = it["pdf_path"], it["page_idx"]
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


# ============================ UI: Summary Dialog ========================
class SummaryDialog(tk.Toplevel):
    def __init__(self, master, rows, not_found_count, summary_csv_path):
        super().__init__(master)
        self.title("Match Summary")
        self.geometry("900x520")
        self.minsize(860, 480)
        self.transient(master)
        self.grab_set()

        info = ttk.Label(self, text=f"Codes not found: {not_found_count} \n Summary CSV: {summary_csv_path}")
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
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="right")


# ============================== Cover helpers ===========================
def _excel_letters_to_indices(letters_list, df):
    def letter_to_idx(s):
        s = s.strip().upper()
        if not s:
            return None
        n = 0
        for ch in s:
            if not ('A' <= ch <= 'Z'):
                return None
            n = n * 26 + (ord(ch) - ord('A') + 1)
        return n - 1
    idxs = []
    for L in letters_list:
        i = letter_to_idx(L)
        if i is not None and 0 <= i < len(df.columns):
            idxs.append(i)
    return idxs


def _find_calibri_fontfile():
    candidates = [
        r"C:\Windows\Fonts\calibri.ttf",
        r"/System/Library/Fonts/Supplemental/Calibri.ttf",
        r"/usr/share/fonts/truetype/msttcorefonts/Calibri.ttf",
        os.environ.get("CALIBRI_TTF", "").strip(),
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


def _text_font_kwargs(fontfile_path):
    if fontfile_path and isinstance(fontfile_path, str) and os.path.exists(fontfile_path):
        return {"fontfile": fontfile_path}
    return {"fontname": "helv"}


# ============================== Main App ================================
class HighlighterApp(tk.Tk):
    def __init__(self):
        super().__init__()
        # ===== Nome da aplicação =====
        self.title("WorkPack Creator")
        self.geometry("1180x1040")
        self.minsize(1100, 960)

        # ====== BARRA INFERIOR FIXA (criada primeiro) ======
        self.bottom = ttk.Frame(self)
        self.bottom.pack(side="bottom", fill="x")
        fr_prog = ttk.Frame(self.bottom)
        fr_prog.pack(side="left", fill="x", expand=True, padx=8, pady=6)
        self.prog = ttk.Progressbar(fr_prog, orient="horizontal", mode="determinate", maximum=100)
        self.prog.pack(side="left", expand=True, fill="x")
        self.lbl_status = ttk.Label(fr_prog, text="Idle")
        self.lbl_status.pack(side="left", padx=8)
        fr_btns = ttk.Frame(self.bottom)
        fr_btns.pack(side="right", padx=8, pady=6)
        self.btn_start = ttk.Button(fr_btns, text="Start", command=self._start)
        self.btn_start.pack(side="left")
        self.btn_stop = ttk.Button(fr_btns, text="Stop", command=self._stop)
        self.btn_stop.pack(side="left", padx=6)
        self.btn_exit = ttk.Button(fr_btns, text="Exit")
        self.btn_exit.config(command=self._exit)
        self.btn_exit.pack(side="left")

        # ====== ÁREA COM ROLAGEM AUTOMÁTICA ======
        self._make_scrollable_content()

        # ====== Estilo discreto "Author" no topo ======
        style = ttk.Style()
        style.configure("Author.TLabel", foreground="#7a7a7a")  # cinza discreto

        # Estado
        self.excel_paths = []
        self.week_number = tk.StringVar()
        self.building_name = tk.StringVar()
        self.output_dir = tk.StringVar()
        self.pages_per_file_var = tk.IntVar(value=20)
        self.only_highlighted_var = tk.BooleanVar(value=True)
        self.review_pages_var = tk.BooleanVar(value=True)
        self.highlight_all_var = tk.BooleanVar(value=True)
        self.use_text_annots_var = tk.BooleanVar(value=True)
        self.turbo_var = tk.BooleanVar(value=True)
        self.parallel_var = tk.BooleanVar(value=True)

        # De-dup / survey / summary
        self.treat_survey_var = tk.BooleanVar(value=True)
        self.survey_size_limit = tk.IntVar(value=1200)  # KB
        self.dedupe_var = tk.BooleanVar(value=True)
        self.dedupe_surveys_var = tk.BooleanVar(value=False)
        self.keep_latest_survey_rev_var = tk.BooleanVar(value=True)
        self.keep_latest_non_survey_rev_var = tk.BooleanVar(value=True)

        # ITR
        self.itr_paths = []  # caminhos .docx/.pdf
        self.itr_map = {}    # primary_code -> {'pdf_path', 'pages'}

        self.drawing_pdfs = []
        self.survey_pdfs = []
        self.cancel_flag = threading.Event()
        self.worker_thread = None
        self.msg_queue = queue.Queue()

        self.ecs_original_map = {}
        self.nosep_to_primary = {}
        self.ecs_cmp_keys = set()

        self._build_scrollable_ui(self.content, style)
        self._poll_messages()

    def _make_scrollable_content(self):
        """
        Opção 2: Rolagem **apenas quando necessário**.
        - A scrollbar NÃO fica visível por padrão.
        - Aparece automaticamente quando o conteúdo ultrapassar a altura do canvas.
        """
        self.canvas = tk.Canvas(self, highlightthickness=0)
        self.canvas.pack(side="top", fill="both", expand=True)

        # Scrollbar criada mas **não exibida** inicialmente
        self.vscroll = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self._on_canvas_scroll)

        # Conteúdo real dentro do canvas
        self.content = ttk.Frame(self.canvas)
        self.content_id = self.canvas.create_window(0, 0, anchor="nw", window=self.content)

        # Atualiza a região rolável e mostra/oculta a barra conforme necessário
        def _update_layout(event=None):
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
            # Ajusta a largura do frame ao canvas
            self.canvas.itemconfigure(self.content_id, width=self.canvas.winfo_width())
            self._toggle_scrollbar_visibility()

        # Bind: mudanças no conteúdo e no canvas
        self.content.bind("<Configure>", _update_layout)
        self.canvas.bind("<Configure>", _update_layout)

        # Roda do mouse: permite rolar mesmo quando a barra estiver oculta (se houver overflow)
        def _on_mousewheel(e):
            # Windows: e.delta múltiplos de 120
            self.canvas.yview_scroll(-1 * (e.delta // 120), "units")

        self.canvas.bind_all("<MouseWheel>", _on_mousewheel)

    def _on_canvas_scroll(self, *args):
        """Callback do yscrollcommand — atualiza o scroller quando necessário."""
        # Atualiza a posição da barra se ela estiver visível
        if getattr(self, "_scrollbar_visible", False):
            self.vscroll.set(*args)

    def _toggle_scrollbar_visibility(self):
        """Mostra a scrollbar apenas se o conteúdo for maior que a viewport."""
        bbox = self.canvas.bbox("all")
        if not bbox:
            # Sem conteúdo ainda
            if getattr(self, "_scrollbar_visible", False):
                self.vscroll.pack_forget()
                self._scrollbar_visible = False
            return
        content_height = bbox[3] - bbox[1]
        viewport_height = self.canvas.winfo_height()
        need_scroll = content_height > max(1, viewport_height)

        if need_scroll and not getattr(self, "_scrollbar_visible", False):
            self.vscroll.pack(side="right", fill="y")
            self._scrollbar_visible = True
            # Precisamos conectar o yview apenas quando visível
            self.canvas.configure(yscrollcommand=self.vscroll.set)
        elif not need_scroll and getattr(self, "_scrollbar_visible", False):
            self.vscroll.pack_forget()
            self._scrollbar_visible = False
            # Mantém o yscrollcommand apontando para callback para detectar futuro overflow
            self.canvas.configure(yscrollcommand=self._on_canvas_scroll)

    def _build_scrollable_ui(self, root_frame: ttk.Frame, style: ttk.Style):
        pad = {"padx": 8, "pady": 6}

        # Top
        fr_top = ttk.Frame(root_frame); fr_top.pack(fill="x", **pad)
        ttk.Label(fr_top, text="Week:").pack(side="left")
        ttk.Entry(fr_top, width=8, textvariable=self.week_number).pack(side="left", padx=8)
        ttk.Label(fr_top, text="Project/Root Name:").pack(side="left", padx=(16, 0))
        ttk.Entry(fr_top, width=30, textvariable=self.building_name).pack(side="left", padx=8, fill="x", expand=True)
        ttk.Label(fr_top, text="Max pages per output:").pack(side="left", padx=(16, 0))
        tk.Spinbox(fr_top, from_=5, to=500, increment=1, width=6, textvariable=self.pages_per_file_var).pack(side="left", padx=6)
        # Author discreto no topo direito
        ttk.Label(fr_top, text="Author: Bryan Raimondi", style="Author.TLabel").pack(side="right")

        # Options
        fr_opts = ttk.Frame(root_frame); fr_opts.pack(fill="x", **pad)
        ttk.Checkbutton(fr_opts, text="Only keep highlighted pages", variable=self.only_highlighted_var).pack(side="left")
        ttk.Checkbutton(fr_opts, text="Review pages before saving", variable=self.review_pages_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Highlight every occurrence", variable=self.highlight_all_var).pack(side="left", padx=12)
        ttk.Checkbutton(fr_opts, text="Use text highlight annotations", variable=self.use_text_annots_var).pack(side="left", padx=12)

        # Performance
        fr_perf = ttk.Frame(root_frame); fr_perf.pack(fill="x", **pad)
        ttk.Checkbutton(fr_perf, text="Turbo (Aho–Corasick)", variable=self.turbo_var).pack(side="left")
        ttk.Checkbutton(fr_perf, text="Parallel PDFs", variable=self.parallel_var).pack(side="left", padx=12)

        # Rules
        fr_rules = ttk.LabelFrame(root_frame, text="De-dup & Survey Rules"); fr_rules.pack(fill="x", **pad)
        ttk.Checkbutton(fr_rules, text="Treat 'Cut Length Report' PDFs as survey tables", variable=self.treat_survey_var).grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(fr_rules, text="Survey size ≤ KB:").grid(row=0, column=1, sticky="e")
        tk.Spinbox(fr_rules, from_=50, to=20000, increment=50, width=6, textvariable=self.survey_size_limit).grid(row=0, column=2, sticky="w", padx=6)
        ttk.Checkbutton(fr_rules, text="Keep only latest Survey REV", variable=self.keep_latest_survey_rev_var).grid(row=3, column=0, sticky="w", padx=6, pady=4)
        ttk.Checkbutton(fr_rules, text="Keep only latest Handbook/Drawings REV", variable=self.keep_latest_non_survey_rev_var).grid(row=3, column=1, columnspan=2, sticky="w", padx=6, pady=4)

        # Excels
        fr_excel = ttk.LabelFrame(root_frame, text="Excel files (ECS Codes)"); fr_excel.pack(fill="x", **pad)
        btns_ex = ttk.Frame(fr_excel); btns_ex.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns_ex, text="Add Excel…", command=self._add_excels).pack(side="left")
        ttk.Button(btns_ex, text="Remove Selected", command=self._remove_selected_excels).pack(side="left", padx=6)
        ttk.Button(btns_ex, text="Clear List", command=self._clear_excels).pack(side="left")
        self.lst_excels = tk.Listbox(fr_excel, height=5, selectmode=tk.EXTENDED)
        self.lst_excels.pack(fill="both", expand=True, padx=6, pady=(0, 6))



        # Drawings
        fr_draw = ttk.LabelFrame(root_frame, text="Drawings (PDFs)"); fr_draw.pack(fill="both", expand=True, **pad)
        btns_d = ttk.Frame(fr_draw); btns_d.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns_d, text="Add Drawings…", command=self._add_drawings).pack(side="left")
        ttk.Button(btns_d, text="Remove Selected", command=self._remove_selected_drawings).pack(side="left", padx=6)
        ttk.Button(btns_d, text="Clear List", command=self._clear_drawings).pack(side="left")
        self.lst_drawings = tk.Listbox(fr_draw, height=7, selectmode=tk.EXTENDED)
        self.lst_drawings.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        # Surveys (Cut Length Reports)
        fr_surv = ttk.LabelFrame(root_frame, text="Surveys (Cut Length Reports PDFs)"); fr_surv.pack(fill="both", expand=True, **pad)
        btns_s = ttk.Frame(fr_surv); btns_s.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns_s, text="Add Surveys…", command=self._add_surveys).pack(side="left")
        ttk.Button(btns_s, text="Remove Selected", command=self._remove_selected_surveys).pack(side="left", padx=6)
        ttk.Button(btns_s, text="Clear List", command=self._clear_surveys).pack(side="left")
        self.lst_surveys = tk.Listbox(fr_surv, height=6, selectmode=tk.EXTENDED)
        self.lst_surveys.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        # ITRs
        fr_itr = ttk.LabelFrame(root_frame, text="ITR files (DOCX or PDF, name must contain the ECS code)"); fr_itr.pack(fill="x", **pad)
        btns_itr = ttk.Frame(fr_itr); btns_itr.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns_itr, text="Add ITR…", command=self._add_itrs).pack(side="left")
        ttk.Button(btns_itr, text="Remove Selected", command=self._remove_selected_itrs).pack(side="left", padx=6)
        ttk.Button(btns_itr, text="Clear List", command=self._clear_itrs).pack(side="left")
        self.lst_itrs = tk.Listbox(fr_itr, height=5, selectmode=tk.EXTENDED)
        self.lst_itrs.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        # Output
        fr_out = ttk.Frame(root_frame); fr_out.pack(fill="x", **pad)
        ttk.Label(fr_out, text="Output Folder:").pack(side="left")
        ttk.Entry(fr_out, textvariable=self.output_dir).pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_out, text="Select…", command=self._pick_output_dir).pack(side="left")

        # (REMOVIDO) Painel Matches — não existe mais

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


    def _add_drawings(self):
        paths = filedialog.askopenfilenames(title="Select Drawing PDFs", filetypes=[("PDF files", "*.pdf")])
        if paths:
            for p in paths:
                if p not in self.drawing_pdfs:
                    self.drawing_pdfs.append(p)
                    self.lst_drawings.insert("end", p)
    
    def _remove_selected_drawings(self):
        sels = list(self.lst_drawings.curselection())[:: -1]
        for i in sels:
            path = self.lst_drawings.get(i)
            self.lst_drawings.delete(i)
            try:
                self.drawing_pdfs.remove(path)
            except ValueError:
                pass
    
    def _clear_drawings(self):
        self.lst_drawings.delete(0, "end")
        self.drawing_pdfs.clear()
    
    def _add_surveys(self):
        paths = filedialog.askopenfilenames(title="Select Survey PDFs (Cut Length Reports)", filetypes=[("PDF files", "*.pdf")])
        if paths:
            for p in paths:
                if p not in self.survey_pdfs:
                    self.survey_pdfs.append(p)
                    self.lst_surveys.insert("end", p)
    
    def _remove_selected_surveys(self):
        sels = list(self.lst_surveys.curselection())[:: -1]
        for i in sels:
            path = self.lst_surveys.get(i)
            self.lst_surveys.delete(i)
            try:
                self.survey_pdfs.remove(path)
            except ValueError:
                pass
    
    def _clear_surveys(self):
        self.lst_surveys.delete(0, "end")
        self.survey_pdfs.clear()
    
    # ====== ITR pickers ======
    def _add_itrs(self):
        paths = filedialog.askopenfilenames(title="Select ITR files", filetypes=[("ITR files", "*.docx *.pdf")])
        if paths:
            for p in paths:
                if p not in self.itr_paths:
                    self.itr_paths.append(p)
                    self.lst_itrs.insert("end", p)

    def _remove_selected_itrs(self):
        sels = list(self.lst_itrs.curselection())[::-1]
        for i in sels:
            path = self.lst_itrs.get(i)
            self.lst_itrs.delete(i)
            try:
                self.itr_paths.remove(path)
            except ValueError:
                pass

    def _clear_itrs(self):
        self.lst_itrs.delete(0, "end")
        self.itr_paths.clear()

    def _pick_output_dir(self):
        d = filedialog.askdirectory(title="Select Output Folder")
        if d:
            self.output_dir.set(d)

    # ===== run controls =====
    def _start(self):
        if self.worker_thread and self.worker_thread.is_alive():
            return
        week = self.week_number.get().strip()
        rootname = self.building_name.get().strip()
        excels = list(self.excel_paths)
        if not week or not excels or (not self.drawing_pdfs and not self.survey_pdfs):
            messagebox.showwarning("Input", "Please provide Week, at least ONE Excel, and at least one Drawing or Survey PDF.")
            return
        first_pdf = (self.drawing_pdfs[0] if self.drawing_pdfs else self.survey_pdfs[0])
        out_dir = self.output_dir.get().strip() or os.path.dirname(first_pdf)
        self.output_dir.set(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        self.cancel_flag.clear()
        self.prog["value"] = 0
        self.lbl_status.config(text="Starting…")

        args = (
            week, rootname, list(excels), list(self.drawing_pdfs), list(self.survey_pdfs), list(self.itr_paths), out_dir,
            int(self.pages_per_file_var.get()),
            bool(self.highlight_all_var.get()),
            bool(self.use_text_annots_var.get()),
            bool(self.turbo_var.get()),
            bool(self.parallel_var.get()),
            
            bool(self.treat_survey_var.get()),
            int(self.survey_size_limit.get()) * 1024,
            bool(self.dedupe_var.get()),
            bool(self.dedupe_surveys_var.get()),
            bool(self.keep_latest_survey_rev_var.get()),
            bool(self.keep_latest_non_survey_rev_var.get()),
        )
        self.worker_thread = threading.Thread(target=self._worker, args=args, daemon=True)
        self.worker_thread.start()

    def _stop(self):
        self.cancel_flag.set()
        self.lbl_status.config(text="Stopping…")

    def _exit(self):
        self.destroy()

    # ===== background worker =====
    def _worker(
        self, week_number, root_name, excel_paths, drawing_paths, survey_paths, itr_paths, out_dir, pages_per_file,
        highlight_all_occurrences, use_text_annotations,
        turbo_mode, parallel_mode, treat_survey, survey_size_limit_bytes,
        dedupe_pages, dedupe_surveys,
        keep_latest_survey_rev, keep_latest_non_survey_rev
    ):
        def post(msg_type, payload=None):
            self.msg_queue.put((msg_type, payload))
        try:
            # 1) Carregar planilhas
            post("status", "Reading Excel files…")
            ecs_primary_all = set()
            original_map_all = {}
            for xp in excel_paths:
                try:
                    df = load_table_with_dynamic_header(xp, sheet_name=0)
                    if df is None:
                        df = pd.read_excel(xp, dtype=str, engine="openpyxl")
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
            cmp_keys_survey, cmp_keys_drawing, cmp_to_primaries, _max_len = build_contextual_indexes(ecs_primary_all)
            nosep_to_primary = {k: (v[0] if isinstance(v, list) and v else v) for k, v in cmp_to_primaries.items()}
            self.cmp_to_primaries = dict(cmp_to_primaries)
            self.nosep_to_primary = dict(nosep_to_primary)  # legacy single-primary map (first), for UI/ITR mapping
            self.ecs_cmp_keys_survey = set(cmp_keys_survey)
            self.ecs_cmp_keys_drawing = set(cmp_keys_drawing)
            self.ecs_cmp_keys = set(cmp_keys_survey) | set(cmp_keys_drawing)

            # 2) Filtrar revisões
            if keep_latest_survey_rev:
                try:
                    survey_paths = select_latest_revisions_any(list(survey_paths))
                except Exception:
                    pass
            if keep_latest_non_survey_rev:
                try:
                    drawing_paths = select_latest_revisions_any(list(drawing_paths))
                except Exception:
                    pass

            combined_pdfs = list(drawing_paths) + list(survey_paths)
            survey_set = set(survey_paths)

            # 3) Mapear ITRs (docx/pdf) por código
            try:
                itr_map = build_itr_map(list(itr_paths), (set(cmp_keys_survey) | set(cmp_keys_drawing)), nosep_to_primary)
            except Exception:
                itr_map = {}
            self.itr_map = itr_map  # primary_code -> {'pdf_path','pages'}

            # 4) Tarefas de scan
            tasks = []
            for pdf in combined_pdfs:
                is_survey_task = (pdf in survey_set)
                cmp_list = sorted(list(cmp_keys_survey if is_survey_task else cmp_keys_drawing))
                tasks.append((
                    pdf,
                    cmp_list,
                    bool(turbo_mode and _HAS_AC),
                    bool(highlight_all_occurrences),
                    bool(is_survey_task),
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

            # 5) Agregar dados
            processed = []
            agg_code_file_pages = defaultdict(lambda: defaultdict(set))  # cmp_key -> file -> set(pages)

            for res in results:
                if "error" in res:
                    post("status", f"Error in {os.path.basename(res['pdf_path'])}: {res['error']}")
                    continue
                pdf_path = res["pdf_path"]
                display = res["display"]
                rects_by_page = res["rects_by_page"]
                code_rects_by_page = res["code_rects_by_page"]
                hit_pages = res["hit_pages"]
                total_pages = res["total_pages"]
                code_pages = res["code_pages"]

                for cmp_key, pages in code_pages.items():
                    agg_code_file_pages[cmp_key][display] = set(pages)

                processed.append({
                    "display": display,
                    "pdf_path": pdf_path,
                    "hit_pages": hit_pages,
                    "rects_by_page": rects_by_page,
                    "code_rects_by_page": code_rects_by_page,
                    "page_codes": {
                        int(p): sorted({
                            self.ecs_original_map.get(primary, primary)
                            for cmp_key, pglist in code_pages.items() if int(p) in pglist
                            for primary in self.cmp_to_primaries.get(cmp_key, [self.nosep_to_primary.get(cmp_key, cmp_key)])
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
                "survey_paths": list(survey_paths),
                "processed": processed,
                "root_name": root_name,
                "week_number": week_number,
                "out_dir": out_dir,
                "use_text_annotations": bool(use_text_annotations),
                "ecs_primary": sorted(list(ecs_primary_all)),
                "original_map": dict(original_map_all),
                "nosep_to_primary": dict(nosep_to_primary),
                "cmp_to_primaries": dict(cmp_to_primaries),
                "agg_code_file_pages": agg_serializable,
                "pages_per_file": int(pages_per_file),
                "treat_survey": bool(treat_survey),
                "survey_size_limit_bytes": int(survey_size_limit_bytes),
                "dedupe_pages": bool(dedupe_pages),
                "dedupe_surveys": bool(dedupe_surveys),
            })
        except Exception as e:
            post("error", f"Unexpected error: {e}")
        finally:
            post("done", None)

    # ===== message pump (UI) =====
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

    # ===== finalize: SDI por código, review por unidade, combine =====
    def _finalize_and_save(self, bundle):
        processed = bundle["processed"]
        root_name = bundle["root_name"]
        week_number = bundle["week_number"]
        out_dir = bundle["out_dir"]
        use_text_annotations = bool(bundle.get("use_text_annotations", True))
        ecs_primary = set(bundle.get("ecs_primary", []))
        original_map = dict(bundle.get("original_map", {}))
        nosep_to_primary = dict(bundle.get("nosep_to_primary", {}))
        cmp_to_primaries = dict(bundle.get("cmp_to_primaries", {}))
        agg_code_file_pages = dict(bundle.get("agg_code_file_pages", {}))
        pages_per_file = max(1, int(bundle.get("pages_per_file", 20)))
        treat_survey = bool(bundle.get("treat_survey", True))
        survey_size_limit_bytes = int(bundle.get("survey_size_limit_bytes", 1_200_000))
        survey_set = set(bundle.get("survey_paths", []) or [])
        dedupe_pages = bool(bundle.get("dedupe_pages", True))
        dedupe_surveys = bool(bundle.get("dedupe_surveys", False))

        if not processed:
            messagebox.showinfo("No Matches", "No pages matched; nothing to save.")
            self.lbl_status.config(text="No matches.")
            self._write_not_surveyed_csv(out_dir, root_name, week_number,
                                         [original_map.get(p, p) for p in sorted(ecs_primary)])
            return

        # ---------- Construir UNIDADES Survey & Drawing por prédio, já duplicando por código ----------
        units_by_building = defaultdict(list)

        def _push_unit(pdf_path, display, pg, unit_type, code_pretty, rects):
            if code_pretty:
                bld = infer_building_from_code(code_pretty)
            else:
                bld = "UNKWN"
            units_by_building[bld].append({
                "display": display,
                "pdf_path": pdf_path,
                "page_idx": pg,
                "type": unit_type,  # "Survey" | "Drawing" | "ITR"
                "code_pretty": code_pretty or "",
                "rects": rects or []
            })

        # 1) filtrar Summary/TOC (somente primeiras páginas)
        def _is_summary_or_toc(pdf_path: str, page_idx: int) -> bool:
            return is_summary_keyword_page(pdf_path, page_idx, first_pages_only=7)

        for p in processed:
            pdf_path = p["pdf_path"]
            display = p["display"]
            rects_by_page = p["rects_by_page"]
            code_rects_by_page = p["code_rects_by_page"]
            page_codes = p.get("page_codes", {})
            keep_pages_base = sorted(list(p["hit_pages"]))
            keep_pages = [pg for pg in keep_pages_base if not _is_summary_or_toc(pdf_path, pg)]

            is_survey_flag = bool(treat_survey) and (pdf_path in survey_set)
            unit_type = "Survey" if is_survey_flag else "Drawing"

            for pg in keep_pages:
                pretty_codes = page_codes.get(pg, [])
                if pretty_codes:
                    for pretty in sorted(pretty_codes):
                        cmp_key = normalize_nosep(pretty)
                        per_code_rects = code_rects_by_page.get(pg, {}).get(cmp_key, [])
                        if not per_code_rects:
                            per_code_rects = rects_by_page.get(pg, [])
                        _push_unit(pdf_path, display, pg, unit_type, pretty, per_code_rects)
                else:
                    rects = rects_by_page.get(pg, [])
                    _push_unit(pdf_path, display, pg, unit_type, "", rects)

        # 2) Acrescentar ITRs por código
        per_building_per_code = defaultdict(lambda: defaultdict(lambda: {"S": deque(), "D": deque(), "ITR": []}))

        for bld, lst in units_by_building.items():
            lst.sort(key=lambda it: (os.path.basename(it["pdf_path"]).lower(), it["page_idx"], it.get("code_pretty", "").lower()))
            for it in lst:
                code = it.get("code_pretty") or ""
                typ = it.get("type")
                if typ == "Survey":
                    per_building_per_code[bld][code]["S"].append(it)
                else:
                    per_building_per_code[bld][code]["D"].append(it)

        # ITR: criar unidades por página (se mapeado) — uma vez por código
        for bld, codemap in per_building_per_code.items():
            for code in list(codemap.keys()):
                code_norm = normalize_base(code)
                primary_guess = None
                for primary, pretty in self.ecs_original_map.items():
                    if normalize_base(pretty) == code_norm:
                        primary_guess = primary
                        break
                if not primary_guess:
                    primary_guess = code_norm
                itr_info = self.itr_map.get(primary_guess)
                if itr_info and itr_info.get("pages", 0) > 0:
                    itr_pdf = itr_info["pdf_path"]
                    pages = itr_info["pages"]
                    codemap[code]["ITR"] = [{
                        "display": os.path.basename(itr_pdf),
                        "pdf_path": itr_pdf,
                        "page_idx": i,
                        "type": "ITR",
                        "code_pretty": code,
                        "rects": []
                    } for i in range(pages)]

        # 3) Interlevar por código em tríades S–D–ITR
        review_units = []
        for bld, codemap in sorted(per_building_per_code.items(), key=lambda kv: kv[0]):
            codes_order = sorted(codemap.keys(), key=lambda c: (c.lower()))
            has_remaining = True
            used_itr_for_code = {c: False for c in codes_order}
            while has_remaining:
                has_remaining = False
                for c in codes_order:
                    buckets = codemap[c]
                    emitted = False
                    if buckets["S"]:
                        review_units.append(buckets["S"].popleft())
                        emitted = True
                    if buckets["D"]:
                        review_units.append(buckets["D"].popleft())
                        emitted = True
                    if not used_itr_for_code[c] and buckets["ITR"]:
                        review_units.extend(buckets["ITR"])
                        used_itr_for_code[c] = True
                        emitted = True
                    has_remaining = has_remaining or bool(buckets["S"] or buckets["D"] or (not used_itr_for_code[c] and buckets["ITR"]))

        used_review = bool(self.review_pages_var.get())
        if used_review:
            dlg = ReviewDialog(self, review_units)
            self.wait_window(dlg)
            if dlg.selection is None:
                self.lbl_status.config(text="Review canceled.")
                return
            ordered_kept = dlg.selection.get("sequence", [])
        else:
            ordered_kept = [(it["pdf_path"], it["page_idx"], it.get("code_pretty"), it.get("rects", []), it.get("type", "Drawing"))
                            for it in review_units]

        # 4) Aplicar dedupe e salvar
        building_buckets = defaultdict(list)
        seen_hashes = set()
        audit_log = []

        def add_unit_if_ok(pdf_path, pg, rects, code_pretty, unit_type):
            fp = page_fingerprint(pdf_path, pg)
            fpsum = fp or f"X:{os.path.basename(pdf_path)}:{pg}"
            if code_pretty:
                fpsum = f"{fpsum}::CODE::{code_pretty}"
            fpsum = f"{fpsum}::TYPE::{unit_type}"

            if bool(self.dedupe_var.get()):
                if unit_type == "Survey" and not bool(self.dedupe_surveys_var.get()):
                    pass
                else:
                    if fpsum in seen_hashes:
                        audit_log.append({
                            "reason": "duplicate_page",
                            "file": os.path.basename(pdf_path),
                            "page": int(pg) + 1,
                            "codes_on_page": code_pretty or "",
                        })
                        return
                    seen_hashes.add(fpsum)

            if code_pretty:
                bld = infer_building_from_code(code_pretty)
            else:
                bld = "UNKWN"
            building_buckets[bld].append({"pdf_path": pdf_path, "page_idx": pg, "rects": rects or [], "type": unit_type, "display": os.path.basename(pdf_path)})

        for (pdf_path, pg, code_pretty, rects, unit_type) in ordered_kept:
            add_unit_if_ok(pdf_path, pg, rects, code_pretty or "", unit_type or "Drawing")

        # Salvar por prédio em partes
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
                                                      scale_to_a3=False)
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

        # --------- Resumo por código ----------
        primary_file_pages = defaultdict(lambda: defaultdict(set))
        for cmp_key, file_map in agg_code_file_pages.items():
            primaries = cmp_to_primaries.get(cmp_key, [nosep_to_primary.get(cmp_key, cmp_key)])
            for fn, pages in file_map.items():
                for primary in primaries:
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
        # Cover Sheet generation disabled (feature removed)

        SummaryDialog(self, rows, len(missing_primary), summary_csv)

    # ===== CSVs & Cover =====
    def _write_summary_csv(self, out_dir, root_name, week_number, rows):
        tag = sanitize_filename(root_name) or "Job"
        csv_path = os.path.join(out_dir, f"{tag}_MatchesSummary_WK{week_number}.csv")
        csv_path = uniquify_path(csv_path)
        try:
            base_df = pd.DataFrame(rows, columns=["code", "total_pages", "breakdown"])
            base_df.to_csv(csv_path, index=False)
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

    def _draw_table_page(self, page, df, margin=36, row_h=18, header_fill=(0.92, 0.92, 0.92),
                         fontfile=None, fontsize=10):
        width, height = float(page.rect.width), float(page.rect.height)
        x_left = margin
        x_right = width - margin
        y = margin + 24

        cols = list(df.columns)

        sample_rows = min(100, len(df))
        col_weights = []
        for c in cols:
            w = max(len(str(c)), max((len(str(df.iloc[i][c])) for i in range(sample_rows)), default=0))
            col_weights.append(max(6, w))
        total_w = sum(col_weights)
        col_widths = [(w / total_w) * (x_right - x_left) for w in col_weights]

        header_top = y
        header_bottom = y + row_h
        page.draw_rect(fitz.Rect(x_left, header_top, x_right, header_bottom), color=(0, 0, 0), fill=header_fill)

        font_kwargs = _text_font_kwargs(fontfile)

        cx = x_left
        for i, c in enumerate(cols):
            cell_rect = fitz.Rect(cx, header_top, cx + col_widths[i], header_bottom)
            page.draw_rect(cell_rect, color=(0, 0, 0), width=0.7)
            page.insert_textbox(
                cell_rect,
                str(c),
                fontsize=fontsize,
                align=fitz.TEXT_ALIGN_LEFT,
                **font_kwargs,
            )
            cx += col_widths[i]
        y = header_bottom

        max_rows = int((height - y - margin) // row_h)

        end = min(len(df), max_rows)
        for r in range(end):
            row_top = y
            row_bottom = y + row_h
            cx = x_left
            for i, c in enumerate(cols):
                cell_rect = fitz.Rect(cx, row_top, cx + col_widths[i], row_bottom)
                page.draw_rect(cell_rect, color=(0, 0, 0), width=0.5)
                txt = "" if pd.isna(df.iloc[r][c]) else str(df.iloc[r][c])
                page.insert_textbox(
                    fitz.Rect(cx + 2, row_top + 1, cx + col_widths[i] - 2, row_bottom - 1),
                    txt,
                    fontsize=fontsize,
                    align=fitz.TEXT_ALIGN_LEFT,
                    **font_kwargs,
                )
                cx += col_widths[i]
            y = row_bottom

        return end

    def _generate_cover_sheet_pdf(self, *args, **kwargs):
        return None

# --- ENTRY POINT ---
if __name__ == "__main__":
    try:
        _log("Entering __main__")
        import multiprocessing
        multiprocessing.freeze_support()
        _log("freeze_support() OK")

        app = HighlighterApp()
        _log("HighlighterApp created; entering mainloop()")
        app.mainloop()
        _log("Exited mainloop() normally")
    except Exception as e:
        _log_exception("FATAL STARTUP ERROR", e)
        raise
