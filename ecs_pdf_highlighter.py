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
import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import pandas as pd
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

from collections import defaultdict, deque

from rules import (
    load_table_with_dynamic_header, extract_ecs_codes_from_df, build_contextual_indexes,
    select_latest_revisions_any, sanitize_filename, uniquify_path, is_summary_keyword_page,
    infer_building_from_code, page_fingerprint, chunk_list, normalize_nosep, normalize_base
)
from scan_ops import _process_pdf_task, _HAS_AC
from itr_ops import build_itr_map
from pdf_ops import combine_pages_to_new
from review_ui import ReviewDialog, SummaryDialog


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
                    try:
                        _log("Opening Review window (review_data received)")
                        self._finalize_and_save(payload)
                    except Exception as e:
                        _log_exception("FINALIZE/REVIEW ERROR", e)
                        self.lbl_status.config(text="Error")
                        try:
                            messagebox.showerror("Error", str(e))
                        except Exception:
                            pass
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
                    # ECS-based fallback: if page_codes is empty, recover codes from code_rects_by_page keys.
                    # This keeps grouping logic based on the ECS matches (NOT filename).
                    page_code_dict = code_rects_by_page.get(pg, {}) or {}

                    if page_code_dict:
                        for cmp_key in sorted(page_code_dict.keys()):
                            per_code_rects = page_code_dict.get(cmp_key, [])
                            primaries = self.cmp_to_primaries.get(
                                cmp_key,
                                [self.nosep_to_primary.get(cmp_key, cmp_key)]
                            )
                            pretty_list = sorted({
                                self.ecs_original_map.get(primary, primary)
                                for primary in primaries
                            })
                            for pretty in pretty_list:
                                _push_unit(
                                    pdf_path,
                                    display,
                                    pg,
                                    unit_type,
                                    pretty,
                                    per_code_rects or rects_by_page.get(pg, [])
                                )
                    else:
                        rects = rects_by_page.get(pg, [])
                        _push_unit(pdf_path, display, pg, unit_type, "", rects)

        
        # ---- Building lookup by (pdf_path, page_idx, type) to avoid UNKWN outputs ----
        # Some units may reach the save stage with code_pretty empty (e.g., combined survey highlights).
        # We still want Surveys/Drawings to be bucketed by building based on the ECS matches detected earlier.
        bld_by_page = defaultdict(set)  # (pdf_path, page_idx, type) -> {building}
        try:
            for bld, _lst in units_by_building.items():
                for _it in _lst:
                    k = (_it.get("pdf_path"), int(_it.get("page_idx", -1)), _it.get("type"))
                    bld_by_page[k].add(bld)
        except Exception:
            bld_by_page = defaultdict(set)

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

        # Review is mandatory by project requirement.
        # If the user closes/cancels the dialog, we abort saving.
        dlg = ReviewDialog(self, review_units)
        self.wait_window(dlg)
        if dlg.selection is None:
            self.lbl_status.config(text="Review canceled.")
            return
        ordered_kept = dlg.selection.get("sequence", [])

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

            # Determine building(s)
            blds = set()
            if code_pretty:
                blds.add(infer_building_from_code(code_pretty))
            else:
                # Fallback to previously detected building(s) for this exact page/type
                try:
                    k = (pdf_path, int(pg), unit_type)
                    blds |= set(bld_by_page.get(k, set()))
                except Exception:
                    pass

            # If we still can't determine building, do NOT create an UNKWN output.
            # We log and skip instead.
            if not blds:
                try:
                    _log(f"[SKIP_NO_BUILDING] type={unit_type} file={os.path.basename(pdf_path)} page={int(pg)+1}")
                except Exception:
                    pass
                return

            # Add to each building bucket (if multiple, page will be duplicated across outputs)
            for bld in sorted(blds):
                building_buckets[bld].append({
                    "pdf_path": pdf_path,
                    "page_idx": pg,
                    "rects": rects or [],
                    "type": unit_type,
                    "display": os.path.basename(pdf_path),
                })


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
                                                      use_text_annotations=use_text_annotations)
                    # pdf_ops.combine_pages_to_new may not return a path; we still saved to out_path.
                    saved_files.append(final_path or out_path)
                    _log(f"[SAVE_DONE] building={bld} part={part_idx} pages={len(chunk)} path={out_path}")
                except Exception as e:
                    _log_exception(f"[SAVE_FAIL] building={bld} part={part_idx} path={out_path}", e)
                    messagebox.showerror("Combine", f"Could not save {fname}:\n{e}")
                part_idx += 1

        _log(f"[SAVE_SUMMARY] total_files={len(saved_files)} files={saved_files}")

        if saved_files:
            self.lbl_status.config(text=f"Saved {len(saved_files)} file(s)")
            messagebox.showinfo("Done", "Outputs saved:\n" + "\n".join(saved_files))
        else:
            self.lbl_status.config(text="No output files saved.")

        # --------- Resumo por código ----------
        # Split summary into Survey vs Drawing
        # We classify each source file using the processed list and the survey_set.
        display_is_survey = {}
        try:
            for _p in processed:
                _disp = _p.get("display")
                _path = _p.get("pdf_path")
                if _disp and _path:
                    display_is_survey[_disp] = (_path in survey_set)
        except Exception:
            display_is_survey = {}

        # Build primary -> (survey file pages) and (drawing file pages)
        primary_file_pages_survey = defaultdict(lambda: defaultdict(set))
        primary_file_pages_drawing = defaultdict(lambda: defaultdict(set))

        for cmp_key, file_map in agg_code_file_pages.items():
            primaries = cmp_to_primaries.get(cmp_key, [nosep_to_primary.get(cmp_key, cmp_key)])
            for fn, pages in file_map.items():
                is_surv = bool(display_is_survey.get(fn, False))
                for primary in primaries:
                    if is_surv:
                        primary_file_pages_survey[primary][fn] |= set(pages)
                    else:
                        primary_file_pages_drawing[primary][fn] |= set(pages)

        # Prepare CSV rows (new format)
        csv_rows = []
        found_primary = set()

        all_primaries = set(primary_file_pages_survey.keys()) | set(primary_file_pages_drawing.keys())
        for primary in sorted(all_primaries):
            found_primary.add(primary)
            pretty = original_map.get(primary, primary)

            surv_total = sum(len(pages) for pages in primary_file_pages_survey[primary].values())
            draw_total = sum(len(pages) for pages in primary_file_pages_drawing[primary].values())

            surv_breakdown = "; ".join(
                f"{fn}:{len(sorted(list(pages)))}"
                for fn, pages in sorted(primary_file_pages_survey[primary].items())
            )
            draw_breakdown = "; ".join(
                f"{fn}:{len(sorted(list(pages)))}"
                for fn, pages in sorted(primary_file_pages_drawing[primary].items())
            )

            csv_rows.append({
                "code": pretty,
                "total_survey_pages": surv_total,
                "total_drawing_pages": draw_total,
                "survey_breakdown": surv_breakdown,
                "drawing_breakdown": draw_breakdown,
            })

        missing_primary = sorted(list(ecs_primary - found_primary))

        # Write enhanced summary CSV
        summary_csv = self._write_summary_csv(out_dir, root_name, week_number, csv_rows)

        # Keep NotSurveyed CSV as before
        self._write_not_surveyed_csv(out_dir, root_name, week_number,
                                     [original_map.get(p, p) for p in missing_primary])
        # Cover Sheet generation disabled (feature removed)

        # Legacy rows for SummaryDialog (so UI doesn't break)
        legacy_rows = []
        for r in csv_rows:
            legacy_rows.append({
                "code": r["code"],
                "total_pages": int(r["total_survey_pages"]) + int(r["total_drawing_pages"]),
                "breakdown": "; ".join([b for b in [r["survey_breakdown"], r["drawing_breakdown"]] if b]),
            })

        SummaryDialog(self, legacy_rows, len(missing_primary), summary_csv)

        _log("[FINALIZE] _finalize_and_save completed")

    # ===== CSVs & Cover =====
    def _write_summary_csv(self, out_dir, root_name, week_number, rows):
        tag = sanitize_filename(root_name) or "Job"
        csv_path = os.path.join(out_dir, f"{tag}_MatchesSummary_WK{week_number}.csv")
        csv_path = uniquify_path(csv_path)
        try:
            base_df = pd.DataFrame(rows)
            # Ensure consistent column order when available
            preferred = ["code", "total_survey_pages", "total_drawing_pages", "survey_breakdown", "drawing_breakdown"]
            cols = [c for c in preferred if c in base_df.columns] + [c for c in base_df.columns if c not in preferred]
            base_df = base_df[cols]
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
