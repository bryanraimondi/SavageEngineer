import os
import fitz
from typing import Optional
from rules import normalize_nosep


def survey_row_highlight_rect(page: fitz.Page, ecs_code_pretty: str,
                              y_tol: float = 3.0, pad: float = 1.2,
                              max_concat_tokens: int = 3) -> "fitz.Rect | None":
    """Return ONE rectangle covering the *table row* that contains the ECS code.

    Rules:
      - NO rotation / orientation normalization. Uses page's native coordinate space.
      - Exact match on normalize_nosep token (no prefix/substring matching).
      - Handles codes split across 2-3 adjacent tokens on the same (block,line).
      - Assumes reading direction is left->right.

    The returned rect spans the table width inferred from words below the header.
    """
    if not ecs_code_pretty:
        return None
    target = normalize_nosep(ecs_code_pretty)
    if not target:
        return None

    words = page.get_text("words", sort=True)
    if not words:
        return None

    # Group by (block,line)
    lines = {}
    for w in words:
        x0, y0, x1, y1, txt, bno, lno, wno = w
        n = normalize_nosep(txt or "")
        if not n:
            continue
        lines.setdefault((bno, lno), []).append((float(x0), float(y0), float(x1), float(y1), n, txt))

    if not lines:
        return None

    for k in list(lines.keys()):
        lines[k].sort(key=lambda t: (t[0], t[1]))  # left->right

    # Infer table X span using words below header (heuristic but layout-based, not orientation-based)
    header_tokens = {normalize_nosep(t) for t in ("Revision", "ECS", "Code", "Support", "Size", "Location", "Design", "Length")}
    header_y_bottom = None
    for ws in lines.values():
        for x0, y0, x1, y1, n, raw in ws:
            if normalize_nosep(raw) in header_tokens:
                header_y_bottom = max(header_y_bottom or 0.0, y1)

    table_x0, table_x1 = page.rect.x0, page.rect.x1
    below = []
    if header_y_bottom is not None:
        for ws in lines.values():
            for x0, y0, x1, y1, n, raw in ws:
                if y0 > header_y_bottom + 2:
                    below.append((x0, x1))
    if below:
        table_x0 = min(a for a, b in below)
        table_x1 = max(b for a, b in below)

    def row_matches(ws):
        norms = [t[4] for t in ws]
        # exact token
        if any(n == target for n in norms):
            return True
        # exact concat of adjacent tokens
        N = len(norms)
        for i in range(N):
            acc = norms[i]
            if acc == target:
                return True
            for j in range(i + 1, min(N, i + max_concat_tokens)):
                acc += norms[j]
                if acc == target:
                    return True
        return False

    target_row = None
    for ws in lines.values():
        if row_matches(ws):
            target_row = ws
            break
    if not target_row:
        return None

    y0 = min(t[1] for t in target_row) - pad
    y1 = max(t[3] for t in target_row) + pad

    # Clamp
    x0 = max(page.rect.x0, table_x0)
    x1 = min(page.rect.x1, table_x1)
    y0 = max(page.rect.y0, y0)
    y1 = min(page.rect.y1, y1)

    if x1 <= x0 or y1 <= y0:
        return None
    return fitz.Rect(x0, y0, x1, y1)


def combine_pages_to_new(out_path, page_units, use_text_annotations=True):
    """Combine pages into a single output PDF.

    HARD RULES:
      - NEVER rotate / normalize orientation / cropbox/mediabox.
      - Preserve the source page as-is (including rotation metadata) via insert_pdf().
      - Surveys: highlight the *table row* containing the ECS code (exact match),
        using survey_row_highlight_rect(); if user adjusted during Review, use unit['rects'] override.
      - Stamp: applied ONLY on Surveys and ONLY ONCE.
    """
    out = fitz.open()
    src_cache = {}

    def _open_src(p):
        if p not in src_cache:
            src_cache[p] = fitz.open(p)
        return src_cache[p]

    try:
        for it in page_units:
            pdf_path = it["pdf_path"]
            pg_idx = it["page_idx"]
            rects = it.get("rects") or []
            is_survey = (it.get("type") == "Survey")
            code_pretty = (it.get("code_pretty") or "").strip()

            src = _open_src(pdf_path)
            src_pg = src.load_page(pg_idx)

            # Copy page as-is (preserve rotation metadata)
            out.insert_pdf(src, from_page=pg_idx, to_page=pg_idx)
            out_pg = out.load_page(out.page_count - 1)

            # Stamp once for surveys
            if is_survey:
                stamp_filename_top_left(out_pg, it.get("display") or os.path.basename(pdf_path))

            # Highlights
            if not use_text_annotations:
                continue

            if is_survey:
                # Surveys: if user provided manual override rects, apply ALL of them (supports combined surveys).
                # Otherwise compute a single row band for the code (if provided).
                if rects:
                    for r in rects:
                        try:
                            rr = fitz.Rect(*r)
                            a = out_pg.add_rect_annot(rr)
                            a.set_colors(stroke=None, fill=(1, 0.75, 0))
                            a.set_opacity(0.35)
                            a.update()
                        except Exception:
                            pass
                elif code_pretty:
                    r0 = survey_row_highlight_rect(src_pg, code_pretty)
                    if r0:
                        a = out_pg.add_rect_annot(r0)
                        a.set_colors(stroke=None, fill=(1, 0.75, 0))
                        a.set_opacity(0.35)
                        a.update()

            elif rects:
                # Drawings: keep existing highlight behaviour (rects already provided)
                add_text_highlights(out_pg, rects, color=(1, 1, 0), opacity=0.35)

    finally:
        for d in src_cache.values():
            try:
                d.close()
            except Exception:
                pass

    out.save(out_path)
    out.close()

def chunk_list(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


