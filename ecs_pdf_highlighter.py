import sys
import os
import re
import fitz  # PyMuPDF
import pandas as pd
from datetime import datetime

# ========= Regular expressions for token cleanup and suffix stripping =========
_SPLIT_RE = re.compile(r"[.\-_]")  # split suffix on first '.', '-', or '_'
_STRIP_PUNCT = re.compile(r'^[\s"\'\(\)\[\]\{\}:;,]+|[\s"\'\(\)\[\]\{\}:;,]+$')

# ========= Excel parsing =========

def load_table_with_dynamic_header(xlsx_path, sheet_name=None):
    """
    Scan the sheet to find the row that contains 'ECS Codes' / 'ECS Code'
    and treat that row as the header. Return a DataFrame with proper headers.
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, dtype=str)
    target_labels = {"ecs codes", "ecs code"}
    header_row_idx = None
    for i in range(len(df)):
        row = df.iloc[i].astype(str)
        for cell in row:
            label = str(cell).strip().lower()
            if label in target_labels:
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
    Pull values from 'ECS Codes' / 'ECS Code' columns and split into code-like tokens.
    Keep tokens that contain letters AND digits, and have no spaces.
    Returns:
      - ecs_lower_set: set of normalized (lowercase) ECS codes
      - original_map: dict lower->original exemplar (for pretty reporting)
    """
    if df is None or df.empty:
        return set(), {}

    cols = [c for c in df.columns if str(c).strip().lower() in ("ecs codes", "ecs code")]
    if not cols:
        return set(), {}

    raw_values = []
    for c in cols:
        raw_values.extend(df[c].dropna().astype(str).tolist())

    tokens = []
    for v in raw_values:
        parts = re.split(r"[,\n;/\t ]+", v)  # split by delimiters and spaces
        for p in parts:
            t = p.strip().strip('"\'' )
            if not t:
                continue
            if re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                tokens.append(t)

    ecs_lower_set = set()
    original_map = {}
    for t in tokens:
        low = t.lower()
        if low not in ecs_lower_set:
            ecs_lower_set.add(low)
            original_map[low] = t
    return ecs_lower_set, original_map

# ========= PDF highlighting =========

def normalize_base(token: str) -> str:
    """
    Clean stray punctuation at the edges, then take the base (before first '.', '-', or '_'),
    and lowercase.
    """
    if not token:
        return ""
    cleaned = _STRIP_PUNCT.sub("", token)
    if not cleaned:
        return ""
    base = _SPLIT_RE.split(cleaned, 1)[0]
    return base.strip().lower()

def highlight_tokens_anywhere(pdf_file, ecs_lower_set, out_path, per_pdf_hits, matched_codes_set):
    """
    Highlight only the FIRST occurrence per ECS base code (before '.', '-', or '_') in the PDF.
    Subsequent variants/suffixes of the same base are skipped.
    """
    doc = fitz.open(pdf_file)
    hits = 0
    highlighted_bases = set()

    for page in doc:
        for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
            tok = (wtext or "").strip()
            if not tok:
                continue
            base = normalize_base(tok)
            if not base:
                continue
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

# ========= Console helpers =========

def is_excel(path):
    return path.lower().endswith((".xlsx", ".xls"))

def is_pdf(path):
    return path.lower().endswith(".pdf")

def parse_dragdrop_line(raw):
    """
    Robustly parse Windows drag&drop input that may contain quoted paths with spaces.
    Accepts Excel + multiple PDFs.
    """
    paths = []
    raw = raw.strip()
    if not raw:
        return paths
    if raw.startswith('"'):
        parts = raw.split('" "')
        for p in parts:
            p = p.strip().strip('"').strip()
            if p:
                paths.append(p)
    else:
        for p in raw.split():
            p = p.strip().strip('"').strip()
            if p:
                paths.append(p)
    return paths

# ========= NotSurveyed PDF report =========

def save_missing_codes_pdf(missing_codes_list, week_number, output_folder):
    """
    Save a simple PDF named NotSurveyed_WK<week>.pdf listing ECS codes not found.
    """
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet

    report_pdf = os.path.join(output_folder, f"NotSurveyed_WK{week_number}.pdf")
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(report_pdf)
    elements = []
    elements.append(Paragraph(f"Not Surveyed - Week {week_number}", styles['Title']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"Total ECS Codes not found: {len(missing_codes_list)}", styles['Normal']))
    elements.append(Spacer(1, 12))
    for code in missing_codes_list:
        elements.append(Paragraph(code, styles['Normal']))
    doc.build(elements)
    return report_pdf

# ========= Main =========

def main():
    print("=== ECS PDF Highlighter (multi-PDF + NotSurveyed report) ===")

    week_number = input("Enter week number (e.g., 34): ").strip()
    if not week_number:
        print("Week number is required.")
        sys.exit(1)

    print("\nDrag & drop the Excel AND one or more PDF files here, then press Enter:")
    paths = parse_dragdrop_line(input())

    excel_file = None
    pdf_files = []
    for p in paths:
        if is_excel(p) and excel_file is None:
            excel_file = p
        elif is_pdf(p):
            pdf_files.append(p)

    if not excel_file:
        print("❌ Please provide one Excel file (.xlsx/.xls).")
        sys.exit(1)
    if not pdf_files:
        print("❌ Please provide at least one PDF (.pdf).")
        sys.exit(1)

    print("\nOutput folder (press Enter to save next to each PDF):")
    out_dir = input().strip()
    use_custom_outdir = bool(out_dir)
    if use_custom_outdir:
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception as e:
            print(f"❌ Could not create/use output folder: {e}")
            sys.exit(1)

    print("\nReading Excel and extracting ECS codes...")
    df = load_table_with_dynamic_header(excel_file, sheet_name=0)
    if df is None:
        print("❌ Could not find a header row containing 'ECS Codes' or 'ECS Code'.")
        sys.exit(1)

    ecs_lower_set, original_map = extract_ecs_codes_from_df(df)
    if not ecs_lower_set:
        print("⚠ No ECS codes found under 'ECS Codes' / 'ECS Code'. Nothing to highlight.")
        sys.exit(0)

    print(f"Found {len(ecs_lower_set)} ECS code token(s). Processing PDFs...")

    per_pdf_hits = {}
    overall_matched_codes = set()

    for pdf_path in pdf_files:
        if not os.path.exists(pdf_path):
            print(f" - Skipping (not found): {pdf_path}")
            continue

        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        out_folder = out_dir if use_custom_outdir else os.path.dirname(pdf_path)
        out_path = os.path.join(out_folder, f"{base_name}_WK{week_number}_priorities.pdf")

        matched_codes_this_pdf = set()
        try:
            hits = highlight_tokens_anywhere(pdf_path, ecs_lower_set, out_path, per_pdf_hits, matched_codes_this_pdf)
            overall_matched_codes |= matched_codes_this_pdf
            print(f" - {os.path.basename(pdf_path)} → {os.path.basename(out_path)} (highlights: {hits}, codes matched: {len(matched_codes_this_pdf)})")
        except Exception as e:
            print(f" - Error processing {pdf_path}: {e}")

    missing_codes_lower = sorted(list(ecs_lower_set - overall_matched_codes))
    missing_pretty = [original_map.get(c, c) for c in missing_codes_lower]

    print("\n===== Summary =====")
    for pdf_path, hits in per_pdf_hits.items():
        print(f"  {os.path.basename(pdf_path)}: {hits} highlights")
    print(f"\nTotal distinct ECS codes matched across PDFs: {len(overall_matched_codes)}")
    print(f"ECS codes NOT found (overall): {len(missing_pretty)}")

    report_folder = out_dir if use_custom_outdir else os.path.dirname(pdf_files[0])
    try:
        report_pdf = save_missing_codes_pdf(missing_pretty, week_number, report_folder)
        print(f"📄 NotSurveyed report saved: {report_pdf}")
    except Exception as e:
        print(f"⚠ Could not save NotSurveyed PDF: {e}")

    print("\n✅ Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
