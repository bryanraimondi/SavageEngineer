import sys
import os
import re
import fitz  # PyMuPDF
import pandas as pd

def load_table_with_dynamic_header(xlsx_path, sheet_name=None):
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
    data = df.iloc[header_row_idx+1:].reset_index(drop=True)
    data.columns = header
    data = data.dropna(axis=1, how='all')
    return data

def extract_ecs_codes_from_df(df):
    if df is None or df.empty:
        return set()
    cols = [c for c in df.columns if str(c).strip().lower() in ("ecs codes", "ecs code")]
    if not cols:
        return set()
    raw_values = []
    for c in cols:
        series = df[c].dropna().astype(str).tolist()
        raw_values.extend(series)
    tokens = []
    for v in raw_values:
        parts = re.split(r"[,\n;/\t ]+", v)  # split by common delimiters incl. spaces
        for p in parts:
            t = p.strip().strip('"\'' )
            if not t:
                continue
            if re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                tokens.append(t)
    return set(t.lower() for t in tokens)

def highlight_tokens_anywhere(pdf_file, token_set_lower, out_path):
    doc = fitz.open(pdf_file)
    hits = 0
    for page in doc:
        for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
            tok = (wtext or "").strip()
            if tok and tok.lower() in token_set_lower:
                rect = fitz.Rect(x0, y0, x1, y1)
                ann = page.add_highlight_annot(rect)
                ann.update()
                hits += 1
    if os.path.exists(out_path):
        os.remove(out_path)
    doc.save(out_path)
    doc.close()
    return hits

def is_excel(path):
    return path.lower().endswith((".xlsx", ".xls"))

def is_pdf(path):
    return path.lower().endswith(".pdf")

def main():
    print("=== ECS PDF Highlighter ===")
    week_number = input("Enter week number (e.g., 34): ").strip()
    if not week_number:
        print("Week number is required.")
        sys.exit(1)

    print("Drag & drop BOTH the Excel and the PDF here, then press Enter:")
    raw = input().strip()
    if raw.startswith('"') and raw.endswith('"') and '" "' in raw:
        paths = [p.strip('"') for p in raw.split('" "')]
    else:
        paths = [p.strip().strip('"') for p in raw.split() if p.strip()]

    excel_file = None
    pdf_file = None
    for p in paths:
        if is_excel(p):
            excel_file = p
        elif is_pdf(p):
            pdf_file = p

    if not excel_file or not pdf_file:
        print("❌ Please provide one Excel file (.xlsx/.xls) and one PDF (.pdf).")
        sys.exit(1)

    df = load_table_with_dynamic_header(excel_file, sheet_name=0)
    if df is None:
        print("❌ Could not find a header row containing 'ECS Codes' or 'ECS Code'.")
        sys.exit(1)

    ecs_tokens = extract_ecs_codes_from_df(df)
    if not ecs_tokens:
        print("⚠ No ECS codes found under 'ECS Codes'/'ECS Code' columns.")
        sys.exit(0)

    print(f"Found {len(ecs_tokens)} ECS code token(s). Highlighting in PDF...")

    base, ext = os.path.splitext(pdf_file)
    out_pdf = f"{base}_WK{week_number}_priorities{ext}"

    hits = highlight_tokens_anywhere(pdf_file, ecs_tokens, out_pdf)
    print(f"✅ Done. Highlights added: {hits}")
    print(f"📄 Output: {out_pdf}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
