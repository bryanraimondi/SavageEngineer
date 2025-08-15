import sys
import os
import re
import fitz  # PyMuPDF
import pandas as pd

# ---------------- Core logic ----------------

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
    A token is kept if it contains letters & digits and has no spaces.
    """
    if df is None or df.empty:
        return set()
    cols = [c for c in df.columns if str(c).strip().lower() in ("ecs codes", "ecs code")]
    if not cols:
        return set()

    raw_values = []
    for c in cols:
        raw_values.extend(df[c].dropna().astype(str).tolist())

    tokens = []
    for v in raw_values:
        # split by common delimiters incl. spaces (handles multiple codes in one cell)
        parts = re.split(r"[,\n;/\t ]+", v)
        for p in parts:
            t = p.strip().strip('"\'' )
            if not t:
                continue
            if re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                tokens.append(t)

    # Deduplicate case-insensitively
    return set(t.lower() for t in tokens)

def highlight_tokens_anywhere(pdf_file, token_set_lower, out_path):
    doc = fitz.open(pdf_file)
    hits = 0
    for page in doc:
        for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
            tok = (wtext or "").strip()
            if tok and tok.lower() in token_set_lower:
                ann = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                ann.update()
                hits += 1
    # Overwrite if exists
    if os.path.exists(out_path):
        os.remove(out_path)
    doc.save(out_path)
    doc.close()
    return hits

# ---------------- Helpers & CLI flow ----------------

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
        # Split on " " between quoted segments
        parts = raw.split('" "')
        for p in parts:
            p = p.strip().strip('"').strip()
            if p:
                paths.append(p)
    else:
        # Fallback split on whitespace, strip quotes
        for p in raw.split():
            p = p.strip().strip('"').strip()
            if p:
                paths.append(p)
    return paths

def main():
    print("=== ECS PDF Highlighter (multi-PDF + custom output folder) ===")

    # 1) Week number
    week_number = input("Enter week number (e.g., 34): ").strip()
    if not week_number:
        print("Week number is required.")
        sys.exit(1)

    # 2) Drag & drop files (one Excel + multiple PDFs allowed)
    print("\nDrag & drop the Excel AND one or more PDF files here, then press Enter:")
    raw = input()
    paths = parse_dragdrop_line(raw)

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

    # 3) Optional: choose an output folder (or press Enter to save next to each PDF)
    print("\nOutput folder (press Enter to save next to each PDF):")
    out_dir = input().strip()
    use_custom_outdir = bool(out_dir)
    if use_custom_outdir:
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception as e:
            print(f"❌ Could not create/use output folder: {e}")
            sys.exit(1)

    # 4) Read Excel (dynamic header detection) and extract ECS tokens
    print("\nReading Excel and extracting ECS codes...")
    df = load_table_with_dynamic_header(excel_file, sheet_name=0)
    if df is None:
        print("❌ Could not find a header row containing 'ECS Codes' or 'ECS Code'.")
        sys.exit(1)

    ecs_tokens = extract_ecs_codes_from_df(df)
    if not ecs_tokens:
        print("⚠ No ECS codes found under 'ECS Codes' / 'ECS Code'. Nothing to highlight.")
        sys.exit(0)

    print(f"Found {len(ecs_tokens)} ECS code token(s). Processing PDFs...")

    # 5) Process each PDF
    total_files = 0
    total_hits = 0
    for pdf_path in pdf_files:
        if not os.path.exists(pdf_path):
            print(f" - Skipping (not found): {pdf_path}")
            continue
        base = os.path.splitext(os.path.basename(pdf_path))[0]
        ext = ".pdf"
        out_folder = out_dir if use_custom_outdir else os.path.dirname(pdf_path)
        out_path = os.path.join(out_folder, f"{base}_WK{week_number}_priorities{ext}")

        try:
            hits = highlight_tokens_anywhere(pdf_path, ecs_tokens, out_path)
            print(f" - {os.path.basename(pdf_path)} → {os.path.basename(out_path)} (highlights: {hits})")
            total_hits += hits
            total_files += 1
        except Exception as e:
            print(f" - Error processing {pdf_path}: {e}")

    print(f"\n✅ Done. Files processed: {total_files}, total highlights: {total_hits}")
    if use_custom_outdir:
        print(f"📁 Outputs saved to: {os.path.abspath(out_dir)}")
    else:
        print("📁 Outputs saved next to each source PDF.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
