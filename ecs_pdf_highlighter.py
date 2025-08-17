import os
import re
import sys
import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import fitz  # PyMuPDF
import pandas as pd

# ---------- Token helpers (suffix-insensitive, punctuation-safe) ----------
_SPLIT_RE = re.compile(r"[.\-_]")  # split on first '.', '-', or '_'
_STRIP_PUNCT = re.compile(r'^[\s"\'\(\)\[\]\{\}:;,]+|[\s"\'\(\)\[\]\{\}:;,]+$')

def normalize_base(token: str) -> str:
    """Trim stray punctuation and return base before first . - or _ (lowercased)."""
    if not token:
        return ""
    cleaned = _STRIP_PUNCT.sub("", token)
    if not cleaned:
        return ""
    base = _SPLIT_RE.split(cleaned, 1)[0]
    return base.strip().lower()

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
    Return a set of ECS codes (lowercased) from columns 'ECS Codes' or 'ECS Code'.
    We split cells by common delimiters and keep tokens that contain letters+digits and no spaces.
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
        parts = re.split(r"[,\n;/\t ]+", v)
        for p in parts:
            t = p.strip().strip('"\'' )
            if not t:
                continue
            if re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                tokens.append(t)

    return set(t.lower() for t in tokens)

# ---------- PDF operations ----------
def highlight_pdf_return_hits(pdf_path, ecs_lower_set, out_path, cancel_flag):
    """
    Highlight only the FIRST occurrence per ECS base (suffix-insensitive).
    Save annotated file ONLY if hits > 0.
    Returns: hits (int), matched_bases (set)
    """
    doc = fitz.open(pdf_path)
    hits = 0
    matched_bases = set()
    highlighted_bases = set()

    try:
        for page in doc:
            if cancel_flag.is_set():
                break
            # reading order
            for (x0, y0, x1, y1, wtext, b, l, n) in page.get_text("words", sort=True):
                if cancel_flag.is_set():
                    break
                tok = (wtext or "").strip()
                if not tok:
                    continue
                base = normalize_base(tok)
                if base and base in ecs_lower_set and base not in highlighted_bases:
                    rect = fitz.Rect(x0, y0, x1, y1)
                    ann = page.add_highlight_annot(rect)
                    ann.update()
                    hits += 1
                    highlighted_bases.add(base)
                    matched_bases.add(base)
    finally:
        # Save only if hits > 0 and not cancelled mid-write
        if hits > 0 and not cancel_flag.is_set():
            if os.path.exists(out_path):
                os.remove(out_path)
            doc.save(out_path)
        doc.close()

    return hits, matched_bases

def combine_pdfs(output_path, input_paths):
    """
    Combine all pages from input_paths into a single PDF at output_path.
    """
    if not input_paths:
        return False
    out = fitz.open()
    try:
        for p in input_paths:
            src = fitz.open(p)
            out.insert_pdf(src)
            src.close()
        if os.path.exists(output_path):
            os.remove(output_path)
        out.save(output_path)
        return True
    finally:
        out.close()

# ---------- GUI App ----------
class HighlighterApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ECS PDF Highlighter")
        self.geometry("720x520")
        self.minsize(680, 520)

        # State
        self.excel_path = tk.StringVar()
        self.week_number = tk.StringVar()
        self.output_dir = tk.StringVar()
        self.pdf_list = []
        self.cancel_flag = threading.Event()
        self.worker_thread = None
        self.msg_queue = queue.Queue()

        self._build_ui()
        self._poll_messages()

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        # Row 0: Week number
        fr_week = ttk.Frame(self)
        fr_week.pack(fill="x", **pad)
        ttk.Label(fr_week, text="Week Number (e.g., 34):").pack(side="left")
        self.ent_week = ttk.Entry(fr_week, width=10, textvariable=self.week_number)
        self.ent_week.pack(side="left", padx=8)

        # Row 1: Excel picker
        fr_excel = ttk.Frame(self)
        fr_excel.pack(fill="x", **pad)
        ttk.Label(fr_excel, text="Excel (ECS Codes):").pack(side="left")
        self.ent_excel = ttk.Entry(fr_excel, textvariable=self.excel_path)
        self.ent_excel.pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_excel, text="Browse…", command=self._pick_excel).pack(side="left")

        # Row 2: PDFs list + buttons
        fr_pdfs = ttk.LabelFrame(self, text="PDFs to Process")
        fr_pdfs.pack(fill="both", expand=True, **pad)

        btns = ttk.Frame(fr_pdfs)
        btns.pack(fill="x", padx=6, pady=6)
        ttk.Button(btns, text="Add PDFs…", command=self._add_pdfs).pack(side="left")
        ttk.Button(btns, text="Remove Selected", command=self._remove_selected).pack(side="left", padx=6)
        ttk.Button(btns, text="Clear List", command=self._clear_list).pack(side="left")

        self.lst_pdfs = tk.Listbox(fr_pdfs, height=10, selectmode=tk.EXTENDED)
        self.lst_pdfs.pack(fill="both", expand=True, padx=6, pady=(0,6))

        # Row 3: Output folder
        fr_out = ttk.Frame(self)
        fr_out.pack(fill="x", **pad)
        ttk.Label(fr_out, text="Output Folder:").pack(side="left")
        self.ent_out = ttk.Entry(fr_out, textvariable=self.output_dir)
        self.ent_out.pack(side="left", expand=True, fill="x", padx=8)
        ttk.Button(fr_out, text="Select…", command=self._pick_output_dir).pack(side="left")

        # Row 4: Progress + Controls
        fr_prog = ttk.Frame(self)
        fr_prog.pack(fill="x", **pad)
        self.prog = ttk.Progressbar(fr_prog, orient="horizontal", mode="determinate", maximum=100)
        self.prog.pack(side="left", expand=True, fill="x")
        self.lbl_status = ttk.Label(fr_prog, text="Idle")
        self.lbl_status.pack(side="left", padx=8)

        fr_btns = ttk.Frame(self)
        fr_btns.pack(fill="x", **pad)
        self.btn_start = ttk.Button(fr_btns, text="Start", command=self._start)
        self.btn_start.pack(side="left")
        self.btn_stop = ttk.Button(fr_btns, text="Stop", command=self._stop, state="disabled")
        self.btn_stop.pack(side="left", padx=6)
        ttk.Button(fr_btns, text="Exit", command=self._exit).pack(side="right")

    # ----- UI actions -----
    def _pick_excel(self):
        path = filedialog.askopenfilename(
            title="Select Excel with ECS Codes",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if path:
            self.excel_path.set(path)

    def _add_pdfs(self):
        paths = filedialog.askopenfilenames(
            title="Select PDFs",
            filetypes=[("PDF files", "*.pdf")]
        )
        if paths:
            for p in paths:
                if p not in self.pdf_list:
                    self.pdf_list.append(p)
                    self.lst_pdfs.insert("end", p)

    def _remove_selected(self):
        sels = list(self.lst_pdfs.curselection())[::-1]
        for i in sels:
            path = self.lst_pdfs.get(i)
            self.lst_pdfs.delete(i)
            try:
                self.pdf_list.remove(path)
            except ValueError:
                pass

    def _clear_list(self):
        self.lst_pdfs.delete(0, "end")
        self.pdf_list.clear()

    def _pick_output_dir(self):
        d = filedialog.askdirectory(title="Select Output Folder")
        if d:
            self.output_dir.set(d)

    # ----- Start/Stop/Exit -----
    def _start(self):
        if self.worker_thread and self.worker_thread.is_alive():
            return
        week = self.week_number.get().strip()
        if not week:
            messagebox.showwarning("Week Number", "Please enter a week number (e.g., 34).")
            return
        excel = self.excel_path.get().strip()
        if not excel or not os.path.exists(excel):
            messagebox.showwarning("Excel", "Please select a valid Excel file.")
            return
        if not self.pdf_list:
            messagebox.showwarning("PDFs", "Please add at least one PDF.")
            return

        out_dir = self.output_dir.get().strip()
        if not out_dir:
            # default: next to the first PDF
            out_dir = os.path.dirname(self.pdf_list[0])
            self.output_dir.set(out_dir)
        os.makedirs(out_dir, exist_ok=True)

        # reset UI
        self.cancel_flag.clear()
        self.prog["value"] = 0
        self.lbl_status.config(text="Starting…")
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")

        # spawn worker
        args = (week, excel, list(self.pdf_list), out_dir)
        self.worker_thread = threading.Thread(target=self._worker, args=args, daemon=True)
        self.worker_thread.start()

    def _stop(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.cancel_flag.set()
            self.lbl_status.config(text="Stopping…")

    def _exit(self):
        if self.worker_thread and self.worker_thread.is_alive():
            if not messagebox.askyesno("Exit", "Processing is still running. Stop and exit?"):
                return
            self.cancel_flag.set()
            # we won't join here to avoid blocking UI; let the thread finish promptly
        self.destroy()

    # ----- Background worker -----
    def _worker(self, week_number, excel_path, pdf_paths, out_dir):
        # Helper to push messages safely to UI
        def post(msg_type, payload=None):
            self.msg_queue.put((msg_type, payload))

        try:
            post("status", "Reading Excel…")
            df = load_table_with_dynamic_header(excel_path, sheet_name=0)
            if df is None:
                post("error", "Could not find a header row containing 'ECS Codes' or 'ECS Code' in the Excel.")
                return
            ecs = extract_ecs_codes_from_df(df)
            if not ecs:
                post("error", "No ECS codes found under 'ECS Codes'/'ECS Code'.")
                return

            total = len(pdf_paths)
            done = 0
            annotated_paths = []
            overall_matched = set()

            for pdf in pdf_paths:
                if self.cancel_flag.is_set():
                    post("status", "Cancelled.")
                    break

                base = os.path.splitext(os.path.basename(pdf))[0]
                per_pdf_out = os.path.join(out_dir, f"{base}_WK{week_number}_priorities.pdf")
                post("status", f"Processing: {os.path.basename(pdf)}")

                try:
                    hits, matched = highlight_pdf_return_hits(pdf, ecs, per_pdf_out, self.cancel_flag)
                except Exception as e:
                    post("status", f"Error: {os.path.basename(pdf)} → {e}")
                    hits = 0
                    matched = set()

                if hits > 0 and not self.cancel_flag.is_set():
                    annotated_paths.append(per_pdf_out)
                    overall_matched |= matched
                    post("status", f"Saved: {os.path.basename(per_pdf_out)} (hits: {hits})")
                else:
                    post("status", f"No highlights: {os.path.basename(pdf)}")

                done += 1
                percent = int((done / total) * 100)
                post("progress", percent)

            # Combine only the annotated PDFs
            if not self.cancel_flag.is_set():
                if annotated_paths:
                    combined_out = os.path.join(out_dir, f"Highlighted_WK{week_number}.pdf")
                    ok = combine_pdfs(combined_out, annotated_paths)
                    if ok:
                        post("status", f"Combined saved: {os.path.basename(combined_out)}")
                    else:
                        post("status", "No combined file created.")
                else:
                    post("status", "No PDFs had highlights; nothing to combine.")

        except Exception as e:
            post("error", f"Unexpected error: {e}")
        finally:
            post("done", None)

    # ----- UI message pump -----
    def _poll_messages(self):
        try:
            while True:
                msg_type, payload = self.msg_queue.get_nowait()
                if msg_type == "status":
                    self.lbl_status.config(text=str(payload))
                elif msg_type == "progress":
                    self.prog["value"] = int(payload)
                elif msg_type == "error":
                    self.lbl_status.config(text="Error")
                    messagebox.showerror("Error", str(payload))
                elif msg_type == "done":
                    self.btn_start.config(state="normal")
                    self.btn_stop.config(state="disabled")
                    if not self.cancel_flag.is_set():
                        self.lbl_status.config(text="Finished.")
                    else:
                        self.lbl_status.config(text="Stopped.")
                # no else; ignore unknown
        except queue.Empty:
            pass
        # schedule next poll
        self.after(80, self._poll_messages)

if __name__ == "__main__":
    try:
        app = HighlighterApp()
        app.mainloop()
    except Exception as e:
        messagebox.showerror("Fatal Error", str(e))
        sys.exit(1)
