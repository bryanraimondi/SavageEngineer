import sys
import os
import re
import fitz  # PyMuPDF
import pandas as pd

# ========= Regex helpers =========
_SPLIT_RE = re.compile(r"[.\-_]")  # suffix split for bases
_STRIP_PUNCT = re.compile(r'^[\s"\'\(\)\[\]\{\}:;,]+|[\s"\'\(\)\[\]\{\}:;,]+$')

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

# ========== Token utils ==========
def normalize_base(token: str) -> str:
    if not token:
        return ""
    cleaned = _STRIP_PUNCT.sub("", token)
    if not cleaned:
        return ""
    base = _SPLIT_RE.split(cleaned, 1)[0]
    return base.strip().lower()

# ========== Highlighting ==========
def highlight_pdf_return_hits(pdf_file, ecs_lower_set, per_pdf_out_path):
    """
    Highlights only the FIRST occurrence per ECS base in a PDF.
    Saves annotated file ONLY if hits > 0.
    Returns: total_hits (int), matched_bases (set[str])
    """
    doc = fitz.open(pdf_file)
    total_hits = 0
    matched_bases = set()
    highlighted_bases = set()

    for page in doc:
        for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
            tok = (wtext or "").strip()
            if not tok:
                continue
            base = normalize_base(tok)
            if base and base in ecs_lower_set and base not in highlighted_bases:
                ann = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                ann.update()
                total_hits += 1
                highlighted_bases.add(base)
                matched_bases.add(base)

    if total_hits > 0:
        if os.path.exists(per_pdf_out_path):
            os.remove(per_pdf_out_path)
        doc.save(per_pdf_out_path)

    doc.close()
    return total_hits, matched_bases

# ========== NotSurveyed report ==========
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

# ========== Console helpers ==========
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
    print("=== ECS PDF Highlighter (save only hits + combine highlighted reports) ===")

    # Week
    week_number = input("Enter week number (e.g., 34): ").strip()
    if not week_number:
        print("Week number is required."); sys.exit(1)

    # Inputs
    print("\nDrag & drop the Excel AND one or more PDF files here, then press Enter:")
    paths = parse_dragdrop_line(input())
    excel_file = None; pdf_files = []
    for p in paths:
        if is_excel(p) and excel_file is None: excel_file = p
        elif is_pdf(p): pdf_files.append(p)
    if not excel_file: print("❌ Provide one Excel file."); sys.exit(1)
    if not pdf_files: print("❌ Provide at least one PDF."); sys.exit(1)

    # Output folder
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

    # We will combine the **highlighted** versions (annotated) of reports that had hits
    combined_doc = fitz.open()
    overall_matched = set()
    reports_with_hits = 0

    annotated_paths = []

    for pdf_path in pdf_files:
        if not os.path.exists(pdf_path):
            print(f" - Skipping (not found): {pdf_path}")
            continue

        out_folder = out_dir if use_custom else os.path.dirname(pdf_path)
        base = os.path.splitext(os.path.basename(pdf_path))[0]
        per_pdf_out = os.path.join(out_folder, f"{base}_WK{week_number}_priorities.pdf")

        hits, matched_bases = highlight_pdf_return_hits(pdf_path, ecs_lower_set, per_pdf_out)
        if hits > 0:
            reports_with_hits += 1
            overall_matched |= matched_bases
            annotated_paths.append(per_pdf_out)
            print(f" - {os.path.basename(pdf_path)} → saved annotated (hits: {hits})")
        else:
            print(f" - {os.path.basename(pdf_path)} → no highlights (no file saved)")

    # Save combined "Highlighted_WKxx.pdf" with **annotated** versions (keeps yellow highlights)
    if annotated_paths:
        combined_out_folder = out_dir if use_custom else os.path.dirname(pdf_files[0])
        combined_out = os.path.join(combined_out_folder, f"Highlighted_WK{week_number}.pdf")
        if os.path.exists(combined_out):
            os.remove(combined_out)
        # Insert each annotated report (all pages)
        for ap in annotated_paths:
            src = fitz.open(ap)
            combined_doc.insert_pdf(src)  # all pages from the annotated file
            src.close()
        combined_doc.save(combined_out)
        print(f"\n📄 Combined highlighted reports saved: {combined_out}")
    else:
        print("\nℹ No reports had highlights; no combined file created.")
    combined_doc.close()

    # NotSurveyed report
    missing_codes_lower = sorted(list(ecs_lower_set - overall_matched))
    missing_pretty = [original_map.get(c, c) for c in missing_codes_lower]
    report_folder = out_dir if use_custom else os.path.dirname(pdf_files[0])
    try:
        ns_pdf = save_missing_codes_pdf(missing_pretty, week_number, report_folder)
        print(f"📄 NotSurveyed report saved: {ns_pdf}")
    except Exception as e:
        print(f"⚠ Could not save NotSurveyed report: {e}")

    # Summary
    print("\n===== Summary =====")
    print(f"Reports with highlights: {reports_with_hits} / {len(pdf_files)}")
    print(f"Total distinct ECS bases matched: {len(overall_matched)}")
    print(f"ECS codes NOT found (overall): {len(missing_pretty)}")
    print("\n✅ Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
