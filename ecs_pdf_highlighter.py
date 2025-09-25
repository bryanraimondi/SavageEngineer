import os
import re
import sys
import uuid
import threading
import queue
import tempfile
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import fitz  # PyMuPDF
import pandas as pd

# ---------- Dash handling + token helpers ----------
DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2212"  # -, ‐, -, ‒, –, —, −
# Strip edge punctuation (incl. various dashes) but keep internal hyphens inside codes:
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
                      ignore_leading_digit, tmp_dir_path, highlight_all_occurrences=False):
    """
    Annotates matches in a copy of the PDF saved to tmp_dir_path (only if there are matches).
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
    highlighted_bases = set()  # guard for "first per code per PDF" mode
    hit_pages = set()

    try:
        for page in doc:
            if cancel_flag.is_set():
                break
            page_hits = 0

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
            # write annotated copy into the dedicated temp directory (NOT the output folder)
            tmp_path = os.path.join(tmp_dir_path, f"__tmp_annot_{uuid.uuid4().hex}.pdf")
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
