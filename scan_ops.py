import os, re, bisect
from collections import defaultdict
from typing import List, Dict, Optional
import fitz


from rules import build_prefixes_and_firstchars, normalize_nosep


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


