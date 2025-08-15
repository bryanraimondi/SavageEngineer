import sys
import os
import re
import fitz  # PyMuPDF
import pandas as pd
from datetime import datetime

# ========= Regex helpers =========
_SPLIT_RE = re.compile(r"[.\-_]")  # suffix split
_STRIP_PUNCT = re.compile(r'^[\s"\'\(\)\[\]\{\}:;,]+|[\s"\'\(\)\[\]\{\}:;,]+$')
DATE_RE = re.compile(r"\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}|\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})\b")

# ========= Target columns =========
DESIGN_COLS = [
    "support", "support size", "location", "x (m)", "y (m)", "z (m)", "design length", "green end ±"
]
CUTS_COLS = [
    "top right & hvac (m)", "bottom right (m)", "bottom left (m)", "top left (m)"
]

# ========== Excel parsing ==========

def load_table_with_dynamic_header(xlsx_path, sheet_name=None):
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, dtype=str)
    target = {"ecs codes", "ecs code"}
    header_row_idx = None
    for i in range(len(df)):
        row = df.iloc[i].astype(str)
        if any(str(c).strip().lower() in target for c in row):
            header_row_idx = i
            break
    if header_row_idx is None:
        return None
    header = df.iloc[header_row_idx].tolist()
    data = df.iloc[header_row_idx + 1:].reset_index(drop=True)
    data.columns = header
    return data.dropna(axis=1, how='all')

def extract_ecs_codes_from_df(df):
    if df is None or df.empty:
        return set(), {}
    cols = [c for c in df.columns if str(c).strip().lower() in ("ecs codes", "ecs code")]
    if not cols:
        return set(), {}
    raw = []
    for c in cols:
        raw += df[c].dropna().astype(str).tolist()

    toks = []
    for v in raw:
        for p in re.split(r"[,\n;/\t ]+", v):
            t = p.strip().strip('"\'' )
            if t and re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                toks.append(t)

    ecs_lower_set = set()
    original_map = {}
    for t in toks:
        low = t.lower()
        if low not in ecs_lower_set:
            ecs_lower_set.add(low)
            original_map[low] = t
    return ecs_lower_set, original_map

# ========== PDF token utils ==========

def normalize_base(token: str) -> str:
    if not token:
        return ""
    cleaned = _STRIP_PUNCT.sub("", token)
    if not cleaned:
        return ""
    base = _SPLIT_RE.split(cleaned, 1)[0]
    return base.strip().lower()

def page_words(page):
    # (x0, y0, x1, y1, text, block, line, word_no)
    return page.get_text("words", sort=True)

# ========== Header field extraction ==========

HEADER_LABELS = {
    "date": ["date"],
    "surveyor": ["surveyor"],
    "unit": ["unit"],
    "building": ["building"],
    "room": ["room", "section", "location", "room/section/location"],
    "reference": ["reference", "references", "ref", "ref(s)"],
    "revision": ["revision", "rev"],
}

def extract_value_after_label(words, label_candidates):
    """
    Find label (e.g., 'Date') and return text to the right on same line,
    or a regex date if label is missing but date pattern exists.
    """
    # group by (block, line)
    from collections import defaultdict
    lines = defaultdict(list)
    for (x0,y0,x1,y1,w,b,l,n) in words:
        lines[(b,l)].append((x0,y0,x1,y1,w))
    for k in lines:
        lines[k].sort(key=lambda t: t[0])

    # pass 1: label + value on same line
    for (_bl, lst) in lines.items():
        tokens = [w for *_, w in lst]
        tokens_low = [t.lower().strip(":") for t in tokens]
        for i, t in enumerate(tokens_low):
            if t in label_candidates:
                # collect remainder of line to the right
                vals = tokens[i+1:]
                joined = " ".join(v for v in vals if v.strip())
                if joined:
                    return joined.strip()

    # pass 2: heuristics for Date if label missing
    if "date" in label_candidates:
        all_text = " ".join(w for *_, w in words)
        m = DATE_RE.search(all_text)
        if m:
            return m.group(0)

    return ""

def extract_header_fields_from_page(page):
    w = page_words(page)
    def val(key):
        return extract_value_after_label(
            w, [s.lower() for s in HEADER_LABELS[key]]
        )

    return {
        "Date": val("date"),
        "Surveyor": val("surveyor"),
        "Unit": val("unit"),
        "Building": val("building"),
        "Room/Section/Location": val("room"),
        "Reference(s)": val("reference"),
        "Revision": val("revision"),
    }

# ========== Table extraction (column-ranged by header positions) ==========

def find_header_line(words, required_labels):
    """
    Locate a line that contains at least one of the required labels (fuzzy match by lower).
    Returns the word-list for that line (sorted), or None.
    """
    from collections import defaultdict
    lines = defaultdict(list)
    for (x0,y0,x1,y1,w,b,l,n) in words:
        lines[(b,l)].append((x0,y0,x1,y1,w))
    for k in lines:
        lines[k].sort(key=lambda t: t[0])

    req = set(required_labels)
    best = None; best_hits = 0
    for lst in lines.values():
        lows = [w[4].strip().lower() for w in lst]
        hits = sum(1 for lab in req if any(lab in tok for tok in lows))
        if hits > best_hits and hits >= 1:
            best = lst; best_hits = hits
    return best

def build_column_ranges(header_words, target_cols):
    """
    Given a header line (list of word tuples), build x-ranges for the target columns.
    Strategy: for each target col, find the word in header that contains it (fuzzy),
    then x0 is its left, x1 is left of next header word; last column extends to far right.
    Returns dict: {canonical_col_name: (x0, x1)}
    """
    if not header_words:
        return {}

    # Flatten header tokens
    hdr = [(x0,y0,x1,y1,w) for (x0,y0,x1,y1,w) in header_words]
    hdr.sort(key=lambda t: t[0])

    # Build a searchable list of tokens
    tokens = [w[4].strip().lower() for w in hdr]
    xs = [w[0] for w in hdr]

    col_ranges = {}
    for col in target_cols:
        col_low = col.lower()
        # find token that contains the column label substring
        idx = None
        for i, tok in enumerate(tokens):
            if col_low in tok:
                idx = i; break
        if idx is None:
            # tolerate exact match without units/parenthesis
            col_nounits = re.sub(r"\s*\([^)]*\)", "", col_low).strip()
            for i, tok in enumerate(tokens):
                tok_nounits = re.sub(r"\s*\([^)]*\)", "", tok).strip()
                if col_nounits == tok_nounits:
                    idx = i; break

        if idx is not None:
            x0 = hdr[idx][0]
            # x1 = next header token start or +200pt if last
            x1 = hdr[idx+1][0] if idx+1 < len(hdr) else hdr[idx][0] + 200.0
            col_ranges[col] = (x0, x1)

    return col_ranges

def extract_table_section(page, required_cols, stop_after_y=None):
    """
    Extract a table section with the required columns.
    - Detect header line that mentions at least one required col.
    - Build column x-ranges from header.
    - Extract rows below header, mapping cells by x-range.
    - Stop if we pass 'stop_after_y' (used to split DESIGN and CUT LENGTHS).
    Returns (rows:list[dict], header_y_bottom:float, col_ranges:dict)
    """
    words = page_words(page)
    header_line = find_header_line(words, required_cols)
    if not header_line:
        return [], None, {}

    header_y_bottom = max(w[3] for w in header_line)
    col_ranges = build_column_ranges(header_line, required_cols)
    if not col_ranges:
        return [], header_y_bottom, {}

    rows = []
    # Group words into lines below header
    from collections import defaultdict
    lines = defaultdict(list)
    for (x0,y0,x1,y1,w,b,l,n) in words:
        if y0 <= header_y_bottom + 0.5:
            continue
        if stop_after_y is not None and y0 > stop_after_y:
            continue
        lines[y0].append((x0,y0,x1,y1,w))

    # sort lines top→bottom
    for y in sorted(lines.keys()):
        items = sorted(lines[y], key=lambda t: t[0])
        row = {col: "" for col in required_cols}
        for (x0,y0,x1,y1,w) in items:
            for col, (cx0, cx1) in col_ranges.items():
                if x0 >= cx0 - 1 and x1 <= cx1 + 1:
                    row[col] = (row[col] + " " + w).strip()
                    break
        # keep row if any cell is non-empty
        if any(row[c] for c in required_cols):
            rows.append(row)

    return rows, header_y_bottom, col_ranges

# ========== Highlighting (first occurrence per ECS base) ==========

def highlight_tokens_anywhere(pdf_file, ecs_lower_set, out_path, per_pdf_hits, matched_codes_set):
    doc = fitz.open(pdf_file)
    hits = 0
    highlighted_bases = set()

    for page in doc:
        for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
            tok = (wtext or "").strip()
            if not tok:
                continue
            base = normalize_base(tok)
            if base in ecs_lower_set and base not in highlighted_bases:
                ann = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                ann.update()
                hits += 1
                highlighted_bases.add(base)
                matched_codes_set.add(base)

    if os.path.exists(out_path):
        os.remove(out_path)
    doc.save(out_path)
    doc.close()
    per_pdf_hits[pdf_file] = hits
    return hits

# ========== Report builders (PDF) ==========

def save_missing_codes_pdf(missing_codes_list, week_number, output_folder):
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet

    report_pdf = os.path.join(output_folder, f"NotSurveyed_WK{week_number}.pdf")
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(report_pdf, title=f"Not Surveyed - Week {week_number}")
    elements = []
    elements.append(Paragraph(f"Not Surveyed - Week {week_number}", styles['Title']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"Total ECS Codes not found: {len(missing_codes_list)}", styles['Normal']))
    elements.append(Spacer(1, 12))
    for code in sorted(missing_codes_list, key=str):
        elements.append(Paragraph(code, styles['Normal']))
    doc.build(elements)
    return report_pdf

def save_consolidated_report(consolidated, week_number, output_folder):
    """
    consolidated: list of dict with keys:
      'source_pdf', 'header' (dict), 'design_rows' (list of dict), 'cut_rows' (list of dict)
    """
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    out_pdf = os.path.join(output_folder, f"Consolidated_Survey_WK{week_number}.pdf")
    styles = getSampleStyleSheet()
    elements = []

    for idx, item in enumerate(consolidated):
        hdr = item["header"]
        source = os.path.basename(item["source_pdf"])

        # Header title
        elements.append(Paragraph(f"Survey: {source}", styles['Heading2']))
        elements.append(Spacer(1, 6))

        # Header block (exact labels/order you asked)
        header_lines = [
            f"Date: {hdr.get('Date','')}",
            f"Surveyor: {hdr.get('Surveyor','')}",
            f"Unit: {hdr.get('Unit','')}",
            f"Building: {hdr.get('Building','')}",
            f"Room/Section/Location: {hdr.get('Room/Section/Location','')}",
            f"Reference(s): {hdr.get('Reference(s)','')}",
            f"Revision: {hdr.get('Revision','')}",
        ]
        for line in header_lines:
            elements.append(Paragraph(line, styles['Normal']))
        elements.append(Spacer(1, 10))

        # DESIGN sub-header and table
        elements.append(Paragraph("DESIGN", styles['Heading3']))
        d_rows = item["design_rows"]
        if d_rows:
            d_data = [ [c for c in DESIGN_COLS] ]
            for r in d_rows:
                d_data.append([r.get(c,"") for c in DESIGN_COLS])
            t = Table(d_data, hAlign="LEFT")
            t.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0), colors.lightgrey),
                ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
                ("ALIGN",(0,0),(-1,0),"CENTER"),
                ("VALIGN",(0,0),(-1,-1),"TOP"),
            ]))
            elements.append(t)
        else:
            elements.append(Paragraph("No DESIGN rows found.", styles['Italic']))
        elements.append(Spacer(1, 10))

        # CUT LENGTHS sub-header and table
        elements.append(Paragraph("CUT LENGTHS", styles['Heading3']))
        c_rows = item["cut_rows"]
        if c_rows:
            c_data = [ [c for c in CUTS_COLS] ]
            for r in c_rows:
                c_data.append([r.get(c,"") for c in CUTS_COLS])
            t2 = Table(c_data, hAlign="LEFT")
            t2.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0), colors.lightgrey),
                ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
                ("ALIGN",(0,0),(-1,0),"CENTER"),
                ("VALIGN",(0,0),(-1,-1),"TOP"),
            ]))
            elements.append(t2)
        else:
            elements.append(Paragraph("No CUT LENGTHS rows found.", styles['Italic']))

        if idx < len(consolidated)-1:
            elements.append(PageBreak())
        else:
            elements.append(Spacer(1, 12))

    doc = SimpleDocTemplate(out_pdf, title=f"Consolidated Survey - Week {week_number}")
    doc.build(elements)
    return out_pdf

# ========== Console & flow helpers ==========

def is_excel(path): return path.lower().endswith((".xlsx", ".xls"))
def is_pdf(path): return path.lower().endswith(".pdf")

def parse_dragdrop_line(raw):
    paths = []
    raw = raw.strip()
    if not raw: return paths
    if raw.startswith('"'):
        for p in raw.split('" "'):
            p = p.strip().strip('"').strip()
            if p: paths.append(p)
    else:
        for p in raw.split():
            p = p.strip().strip('"').strip()
            if p: paths.append(p)
    return paths

# ========== MAIN ==========

def main():
    print("=== ECS PDF Highlighter + Consolidated Report ===")

    week_number = input("Enter week number (e.g., 34): ").strip()
    if not week_number:
        print("Week number is required."); sys.exit(1)

    print("\nDrag & drop the Excel AND one or more PDF files here, then press Enter:")
    paths = parse_dragdrop_line(input())

    excel_file = None; pdf_files = []
    for p in paths:
        if is_excel(p) and excel_file is None: excel_file = p
        elif is_pdf(p): pdf_files.append(p)

    if not excel_file: print("❌ Provide one Excel file."); sys.exit(1)
    if not pdf_files: print("❌ Provide at least one PDF."); sys.exit(1)

    print("\nOutput folder (press Enter to save next to each PDF):")
    out_dir = input().strip()
    use_custom = bool(out_dir)
    if use_custom:
        try: os.makedirs(out_dir, exist_ok=True)
        except Exception as e: print(f"❌ Output folder error: {e}"); sys.exit(1)

    # Excel
    print("\nReading Excel and extracting ECS codes...")
    df = load_table_with_dynamic_header(excel_file, sheet_name=0)
    if df is None:
        print("❌ Could not find 'ECS Codes' / 'ECS Code' header in Excel."); sys.exit(1)
    ecs_lower_set, original_map = extract_ecs_codes_from_df(df)
    if not ecs_lower_set:
        print("⚠ No ECS codes found."); sys.exit(0)

    print(f"Found {len(ecs_lower_set)} ECS codes. Processing PDFs...")

    # For downstream reports
    per_pdf_hits = {}
    overall_matched = set()
    consolidated = []

    for pdf_path in pdf_files:
        if not os.path.exists(pdf_path):
            print(f" - Skipping (not found): {pdf_path}")
            continue

        base = os.path.splitext(os.path.basename(pdf_path))[0]
        out_folder = out_dir if use_custom else os.path.dirname(pdf_path)
        highlighted_pdf = os.path.join(out_folder, f"{base}_WK{week_number}_priorities.pdf")

        # 1) Highlight first occurrence per ECS base
        matched_this_pdf = set()
        try:
            hits = highlight_tokens_anywhere(pdf_path, ecs_lower_set, highlighted_pdf, per_pdf_hits, matched_this_pdf)
            overall_matched |= matched_this_pdf
            print(f" - Highlighted: {os.path.basename(highlighted_pdf)} (highlights: {hits}, matched codes: {len(matched_this_pdf)})")
        except Exception as e:
            print(f" - Error highlighting {pdf_path}: {e}")

        # 2) Extract header + tables (DESIGN + CUT LENGTHS)
        try:
            doc = fitz.open(pdf_path)
            page = doc[0]  # header + table assumed on page 1 for metadata
            header_vals = extract_header_fields_from_page(page)

            # DESIGN section
            d_rows, d_y, d_ranges = extract_table_section(page, DESIGN_COLS)
            # CUT LENGTHS section — try to locate on same page; if not found, scan subsequent pages
            c_rows, _, _ = extract_table_section(page, CUTS_COLS)
            if not c_rows and len(doc) > 1:
                for pidx in range(1, len(doc)):
                    cr, _, _ = extract_table_section(doc[pidx], CUTS_COLS)
                    if cr:
                        c_rows = cr
                        break
            doc.close()

            # Filter rows to ECS codes (by Support base)
            def keep_row(r):
                sup = r.get("Support","").strip()
                base = normalize_base(sup)
                return bool(base) and (base in ecs_lower_set)

            d_rows_f = [ {k: r.get(k,"") for k in DESIGN_COLS} for r in d_rows if keep_row(r) ]
            c_rows_f = [ {k: r.get(k,"") for k in CUTS_COLS}   for r in c_rows   if True ]  # cut lengths may not repeat support

            consolidated.append({
                "source_pdf": pdf_path,
                "header": header_vals,
                "design_rows": d_rows_f,
                "cut_rows": c_rows_f,
            })
        except Exception as e:
            print(f" - Error extracting tables/headers from {pdf_path}: {e}")

    # Missing-codes report (overall)
    missing_codes_lower = sorted(list(ecs_lower_set - overall_matched))
    missing_pretty = [original_map.get(c, c) for c in missing_codes_lower]
    report_folder = out_dir if use_custom else os.path.dirname(pdf_files[0])
    try:
        ns_pdf = save_missing_codes_pdf(missing_pretty, week_number, report_folder)
        print(f"📄 NotSurveyed report saved: {ns_pdf}")
    except Exception as e:
        print(f"⚠ Could not save NotSurveyed report: {e}")

    # Consolidated report
    try:
        cons_pdf = save_consolidated_report(consolidated, week_number, report_folder)
        print(f"📄 Consolidated report saved: {cons_pdf}")
    except Exception as e:
        print(f"⚠ Could not save consolidated report: {e}")

    print("\n===== Summary =====")
    for pdf_path, hits in per_pdf_hits.items():
        print(f"  {os.path.basename(pdf_path)}: {hits} highlights")
    print(f"Total distinct ECS codes matched across PDFs: {len(overall_matched)}")
    print(f"ECS codes NOT found (overall): {len(missing_pretty)}")
    print("\n✅ Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
