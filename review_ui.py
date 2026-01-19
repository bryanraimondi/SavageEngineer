import os, base64
import tkinter as tk
from tkinter import ttk
import fitz
from pdf_ops import survey_row_highlight_rect


# ============================ UI: Review Dialog (v3) ====================
class ReviewDialog(tk.Toplevel):
    """
    Review baseado em UNIDADES (linha por página + tipo + código).
    Tipos: Survey, Drawing, ITR. Ordem exibida = ordem que vai para o output.
    """
    def __init__(self, master, units):
        super().__init__(master)
        self.title("Review pages — interleaved S–D–ITR by code")
        try:
            if len(units) == 0:
                print("[ReviewDialog] Opened with 0 units")
        except Exception:
            pass
        self.geometry("1200x740")
        self.minsize(1080, 660)
        # NOTE: Do not call transient() here; it disables minimize/maximize on Windows.
        self.grab_set()
        # Bring the dialog to the front (Windows may open Toplevel behind the main window)
        try:
            self.lift()
            self.focus_force()
            self.attributes("-topmost", True)
            self.after(250, lambda: self.attributes("-topmost", False))
        except Exception:
            pass


        self.units = list(units)  # lista de dicts (display, pdf_path, page_idx, code_pretty, rects, type)
        self.keep_idx = set(range(len(self.units)))
        # If enabled, surveys that are on the same PDF page will be combined into a single output page.
        # Review remains per-code (easier to validate), but output can be deduplicated for printing.
        self.combine_surveys_same_page_var = tk.BooleanVar(value=False)

        paned = ttk.Panedwindow(self, orient="horizontal")
        paned.pack(fill="both", expand=True, padx=8, pady=8)
        left = ttk.Frame(paned)
        right = ttk.Frame(paned)
        paned.add(left, weight=3)
        paned.add(right, weight=2)

        ttk.Label(left, text="Pages (double-click to toggle keep). Click headers to sort.").pack(anchor="w")
        tree_frame = ttk.Frame(left)
        tree_frame.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(
            tree_frame,
            columns=("order", "keep", "type", "file", "page", "code"),
            show="headings",
            selectmode="browse",
            height=24
        )
        self.tree.heading("order", text="#")
        self.tree.heading("keep", text="Keep", command=lambda: self._sort_tree("keep"))
        self.tree.heading("type", text="Type", command=lambda: self._sort_tree("type"))  # Survey/Drawing/ITR
        self.tree.heading("file", text="File", command=lambda: self._sort_tree("file"))
        self.tree.heading("page", text="Page", command=lambda: self._sort_tree("page"))
        self.tree.heading("code", text="ECS Code", command=lambda: self._sort_tree("code"))
        self.tree.column("order", width=40, anchor="center")
        self.tree.column("keep", width=60, anchor="center")
        self.tree.column("type", width=90, anchor="center")
        self.tree.column("file", width=500, anchor="w")
        self.tree.column("page", width=70, anchor="center")
        self.tree.column("code", width=220, anchor="w")

        ybar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        xbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        ybar.grid(row=0, column=1, sticky="ns")
        xbar.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        self._row_iids = []  # índice -> iid
        self._rebuild_tree(self.units)

        ttk.Label(right, text="Preview").pack(anchor="w")
        canvas_frame = ttk.Frame(right)
        canvas_frame.pack(fill="both", expand=True)
        self.canvas = tk.Canvas(canvas_frame, bg="#202020", highlightthickness=0)
        xscroll = ttk.Scrollbar(canvas_frame, orient="horizontal", command=self.canvas.xview)
        yscroll = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        canvas_frame.rowconfigure(0, weight=1)
        canvas_frame.columnconfigure(0, weight=1)

        self._preview_img = None
        self._zoom = 1.25
        self._edit_mode = False
        self._overlay_items = []
        self._drag_mode = None
        self._drag_start = None
        self._cur_rect_pdf = None
        # canvas editing bindings (active only in edit mode)
        self.canvas.bind("<ButtonPress-1>", self._on_canvas_down)
        self.canvas.bind("<B1-Motion>", self._on_canvas_drag)
        self.canvas.bind("<ButtonRelease-1>", self._on_canvas_up)
        controls = ttk.Frame(right)
        controls.pack(fill="x", pady=(6, 0))
        ttk.Button(controls, text="Zoom -", command=lambda: self._change_zoom(-0.15)).pack(side="left")
        ttk.Button(controls, text="Zoom +", command=lambda: self._change_zoom(+0.15)).pack(side="left", padx=6)
        ttk.Button(controls, text="Edit highlight", command=self._toggle_edit).pack(side="left", padx=10)
        ttk.Button(controls, text="Reset", command=self._reset_highlight).pack(side="left")
        self.stat = ttk.Label(controls, text="—")
        self.stat.pack(side="right")

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(6, 8))
        ttk.Button(btns, text="Select All", command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Clear All", command=self._clear_all).pack(side="left", padx=6)
        ttk.Button(btns, text="Toggle Selected", command=self._toggle_selected_btn).pack(side="left", padx=6)
        ttk.Checkbutton(
            btns,
            text="Combine surveys on same page",
            variable=self.combine_surveys_same_page_var
        ).pack(side="left", padx=10)

        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)

        self.tree.bind("<Double-1>", self._toggle_keep)
        self.tree.bind("<<TreeviewSelect>>", self._preview_selected)
        if self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._preview_selected()

        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _rebuild_tree(self, units):
        self.tree.delete(*self.tree.get_children())
        self._row_iids.clear()
        for idx, it in enumerate(units, start=1):
            keep_txt = "[x]" if (idx-1) in self.keep_idx else "[ ]"
            typ = it.get("type", "Drawing")
            disp = it.get("display") or os.path.basename(it["pdf_path"])
            page1b = it["page_idx"] + 1
            code = it.get("code_pretty") or ""
            iid = self.tree.insert("", "end", values=(idx, keep_txt, typ, disp, page1b, code))
            self._row_iids.append(iid)

    def _rows_snapshot(self):
        rows = []
        for idx, iid in enumerate(self._row_iids):
            it = self.units[idx]
            keep = (idx in self.keep_idx)
            disp = it.get("display") or os.path.basename(it["pdf_path"])
            rows.append({
                "idx": idx,
                "keep": keep,
                "type": it.get("type", "Drawing"),
                "display": disp,
                "page": it["page_idx"],
                "code": it.get("code_pretty") or ""
            })
        return rows

    def _reapply_rows(self, rows):
        # reorganiza self.units e keep_idx segundo 'rows'
        new_units = []
        new_keep = set()
        for i, r in enumerate(rows):
            new_units.append(self.units[r["idx"]])
            if r["keep"]:
                new_keep.add(i)
        self.units = new_units
        self.keep_idx = new_keep
        self._rebuild_tree(self.units)

    def _sort_tree(self, column):
        rows = self._rows_snapshot()
        if column == "file":
            rows.sort(key=lambda r: (r["display"].lower(), r["page"]))
        elif column == "page":
            rows.sort(key=lambda r: r["page"])
        elif column == "keep":
            rows.sort(key=lambda r: ((not r["keep"]), r["display"].lower(), r["page"]))
        elif column == "type":
            rows.sort(key=lambda r: (r["type"], r["display"].lower(), r["page"]))
        elif column == "code":
            rows.sort(key=lambda r: (r["code"].lower(), r["display"].lower(), r["page"]))
        else:
            return
        self._reapply_rows(rows)

    def _toggle_keep(self, event=None):
        iid = self.tree.identify_row(event.y) if event else self.tree.focus()
        if not iid:
            return
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        if pos in self.keep_idx:
            self.keep_idx.remove(pos)
            self.tree.set(iid, "keep", "[ ]")
        else:
            self.keep_idx.add(pos)
            self.tree.set(iid, "keep", "[x]")

    def _toggle_selected_btn(self):
        """Toggle keep/unkeep for the currently selected row (no mouse event required)."""
        sel = self.tree.selection()
        iid = sel[0] if sel else self.tree.focus()
        if not iid:
            return
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        if pos in self.keep_idx:
            self.keep_idx.remove(pos)
            self.tree.set(iid, "keep", "[ ]")
        else:
            self.keep_idx.add(pos)
            self.tree.set(iid, "keep", "[x]")


    def _select_all(self):
        self.keep_idx = set(range(len(self.units)))
        # If enabled, surveys that are on the same PDF page will be combined into a single output page.
        # Review remains per-code (easier to validate), but output can be deduplicated for printing.
        self.combine_surveys_same_page_var = tk.BooleanVar(value=False)
        self._rebuild_tree(self.units)

    def _clear_all(self):
        self.keep_idx.clear()
        self._rebuild_tree(self.units)

    def _ok(self):
        # Build final sequence in the order shown in the Review (keep only checked items).
        seq = []
        for i, it in enumerate(self.units):
            if i in self.keep_idx:
                seq.append((
                    it["pdf_path"],
                    it["page_idx"],
                    it.get("code_pretty"),
                    it.get("rects", []),
                    it.get("type", "Drawing")
                ))

        # Optional: combine surveys that point to the same PDF page into a single output page.
        # This does NOT change the Review (still per-code), only the final output sequence.
        if bool(self.combine_surveys_same_page_var.get()):
            combined = []
            survey_groups = {}  # (pdf_path, page_idx) -> {"rects": [...], "display": ..., "type": "Survey"}
            for (pdf_path, page_idx, code_pretty, rects, typ) in seq:
                if typ == "Survey":
                    key = (pdf_path, page_idx)
                    g = survey_groups.setdefault(key, {"rects": []})
                    # Merge rectangles (can be empty; empty means "compute later", but when combining we prefer explicit rects)
                    if rects:
                        for r in rects:
                            try:
                                t = tuple(map(float, r))
                                if len(t) == 4 and t not in g["rects"]:
                                    g["rects"].append(t)
                            except Exception:
                                pass
                else:
                    combined.append((pdf_path, page_idx, code_pretty, rects, typ))

            # Append merged surveys back in a stable order (first appearance order in seq)
            seen = set()
            for (pdf_path, page_idx, code_pretty, rects, typ) in seq:
                if typ != "Survey":
                    continue
                key = (pdf_path, page_idx)
                if key in seen:
                    continue
                seen.add(key)
                g = survey_groups.get(key, {"rects": []})
                # For combined surveys we can leave code_pretty empty; rects drive the highlights.
                combined.append((pdf_path, page_idx, "", g["rects"], "Survey"))

            seq = combined

        self.selection = {"sequence": seq}
        self.destroy()

    def _cancel(self):
        self.selection = None
        self.destroy()

    def _change_zoom(self, delta):
        self._zoom = max(0.3, min(3.0, getattr(self, "_zoom", 1.25) + delta))
        self._preview_selected()

    def _preview_selected(self, event=None):
        # save any edited highlight for the previously selected row
        self._save_current_override()
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        it = self.units[pos]
        pdf_path, page_idx = it["pdf_path"], it["page_idx"]
        self._render_page(pdf_path, page_idx)

    def _render_page(self, pdf_path, page_idx):
        self.stat.config(text=f"{os.path.basename(pdf_path)} — page {page_idx+1}")
        try:
            with fitz.open(pdf_path) as doc:
                pg = doc.load_page(page_idx)
                z = getattr(self, "_zoom", 1.25)
                mat = fitz.Matrix(z, z)
                pix = pg.get_pixmap(matrix=mat, alpha=False)
                png_bytes = pix.tobytes("png")
                # Prepare default highlight overlay for Surveys
                self._page_width_pdf = float(pg.rect.width)
                self._page_height_pdf = float(pg.rect.height)
                rect_pdf = None
                try:
                    if it.get('type') == 'Survey':
                        # use override if exists
                        rlist = it.get('rects') or []
                        if rlist:
                            rect_pdf = tuple(map(float, rlist[0]))
                        elif it.get('code_pretty'):
                            r = survey_row_highlight_rect(pg, it.get('code_pretty'))
                            if r:
                                rect_pdf = (r.x0, r.y0, r.x1, r.y1)
                except Exception:
                    rect_pdf = None
                self._cur_rect_pdf = rect_pdf
                b64 = base64.b64encode(png_bytes).decode("ascii")
                img = tk.PhotoImage(data=b64)
                self._preview_img = img
                self.canvas.delete("all")
                self.canvas.create_image(0, 0, anchor="nw", image=img)
                self.canvas.config(scrollregion=(0, 0, img.width(), img.height()))
                # Draw highlight overlay (if any)
                self._draw_overlay(self._cur_rect_pdf)
        except Exception as e:
            self.canvas.delete("all")
            self.canvas.create_text(10, 10, anchor="nw", fill="white",
                                    text=f"Preview error:\n{e}")



    # -------- Highlight edit helpers --------
    def _toggle_edit(self):
        self._edit_mode = not getattr(self, "_edit_mode", False)
        # Update status text quickly
        mode = "EDIT" if self._edit_mode else "VIEW"
        try:
            self.stat.config(text=f"{self.stat.cget('text')}  [{mode}]")
        except Exception:
            pass

    def _reset_highlight(self):
        # Remove override for current unit (if any) and redraw calculated overlay
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        it = self.units[pos]
        it["rects"] = []
        self._cur_rect_pdf = None
        self._preview_selected()

    def _clear_overlay(self):
        for item in getattr(self, "_overlay_items", []):
            try:
                self.canvas.delete(item)
            except Exception:
                pass
        self._overlay_items = []

    def _draw_overlay(self, rect_pdf):
        # Draw overlay rectangle + corner handles in canvas coordinates
        self._clear_overlay()
        if not rect_pdf:
            return
        z = getattr(self, "_zoom", 1.25)
        x0, y0, x1, y1 = rect_pdf
        cx0, cy0, cx1, cy1 = x0 * z, y0 * z, x1 * z, y1 * z

        # main rectangle
        r_id = self.canvas.create_rectangle(cx0, cy0, cx1, cy1, outline="#ffcc00", width=2)
        self._overlay_items.append(r_id)

        # handles
        hs = 6  # half-size
        handles = {
            "nw": (cx0, cy0),
            "ne": (cx1, cy0),
            "sw": (cx0, cy1),
            "se": (cx1, cy1),
        }
        for tag, (hx, hy) in handles.items():
            hid = self.canvas.create_rectangle(hx-hs, hy-hs, hx+hs, hy+hs, outline="#ffcc00", fill="#ffcc00", tags=("handle", tag))
            self._overlay_items.append(hid)

    def _save_current_override(self):
        # Persist edited rect into current unit["rects"] as PDF coords
        if not getattr(self, "_cur_rect_pdf", None):
            return
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        try:
            pos = self._row_iids.index(iid)
        except ValueError:
            return
        it = self.units[pos]
        # Only surveys have editable highlight
        if it.get("type") != "Survey":
            return
        x0, y0, x1, y1 = self._cur_rect_pdf
        # normalize
        x0, x1 = sorted([float(x0), float(x1)])
        y0, y1 = sorted([float(y0), float(y1)])
        it["rects"] = [(x0, y0, x1, y1)]

    def _canvas_xy(self, event):
        # Convert event coords to canvas coords considering scroll
        return (self.canvas.canvasx(event.x), self.canvas.canvasy(event.y))

    def _hit_test_handle(self, x, y):
        # return handle tag if near a handle
        items = self.canvas.find_withtag("handle")
        for it in items:
            bbox = self.canvas.bbox(it)
            if bbox and bbox[0] <= x <= bbox[2] and bbox[1] <= y <= bbox[3]:
                tags = self.canvas.gettags(it)
                # tags includes ("handle", "nw") etc.
                for t in tags:
                    if t in ("nw", "ne", "sw", "se"):
                        return t
        return None

    def _on_canvas_down(self, event):
        if not getattr(self, "_edit_mode", False):
            return
        if not getattr(self, "_cur_rect_pdf", None):
            return
        x, y = self._canvas_xy(event)
        handle = self._hit_test_handle(x, y)
        if handle:
            self._drag_mode = handle
        else:
            # inside rect? then move
            z = getattr(self, "_zoom", 1.25)
            x0, y0, x1, y1 = self._cur_rect_pdf
            cx0, cy0, cx1, cy1 = x0*z, y0*z, x1*z, y1*z
            if cx0 <= x <= cx1 and cy0 <= y <= cy1:
                self._drag_mode = "move"
            else:
                self._drag_mode = None
                return
        self._drag_start = (x, y, *self._cur_rect_pdf)

    def _on_canvas_drag(self, event):
        if not getattr(self, "_edit_mode", False):
            return
        if not self._drag_mode or not self._drag_start:
            return
        x, y = self._canvas_xy(event)
        sx, sy, x0, y0, x1, y1 = self._drag_start
        z = getattr(self, "_zoom", 1.25)

        dx = (x - sx) / z
        dy = (y - sy) / z

        mode = self._drag_mode
        nx0, ny0, nx1, ny1 = x0, y0, x1, y1

        if mode == "move":
            nx0, nx1 = x0 + dx, x1 + dx
            ny0, ny1 = y0 + dy, y1 + dy
        elif mode == "nw":
            nx0, ny0 = x0 + dx, y0 + dy
        elif mode == "ne":
            nx1, ny0 = x1 + dx, y0 + dy
        elif mode == "sw":
            nx0, ny1 = x0 + dx, y1 + dy
        elif mode == "se":
            nx1, ny1 = x1 + dx, y1 + dy

        # constrain minimum size
        min_w, min_h = 10.0, 6.0
        if (nx1 - nx0) < min_w:
            if mode in ("nw", "sw"):
                nx0 = nx1 - min_w
            elif mode in ("ne", "se"):
                nx1 = nx0 + min_w
        if (ny1 - ny0) < min_h:
            if mode in ("nw", "ne"):
                ny0 = ny1 - min_h
            elif mode in ("sw", "se"):
                ny1 = ny0 + min_h

        # clamp to page bounds if we have them
        try:
            pw = getattr(self, "_page_width_pdf", None)
            ph = getattr(self, "_page_height_pdf", None)
            if pw and ph:
                nx0 = max(0.0, min(pw, nx0))
                nx1 = max(0.0, min(pw, nx1))
                ny0 = max(0.0, min(ph, ny0))
                ny1 = max(0.0, min(ph, ny1))
        except Exception:
            pass

        self._cur_rect_pdf = (nx0, ny0, nx1, ny1)
        self._draw_overlay(self._cur_rect_pdf)

    def _on_canvas_up(self, event):
        if not getattr(self, "_edit_mode", False):
            return
        # persist rect into unit rects
        self._save_current_override()
        self._drag_mode = None
        self._drag_start = None


# ============================ UI: Summary Dialog ========================
class SummaryDialog(tk.Toplevel):
    def __init__(self, master, rows, not_found_count, summary_csv_path):
        super().__init__(master)
        self.title("Match Summary")
        self.geometry("900x520")
        self.minsize(860, 480)
        # NOTE: Do not call transient() here; it disables minimize/maximize on Windows.
        self.grab_set()

        info = ttk.Label(self, text=f"Codes not found: {not_found_count} \n Summary CSV: {summary_csv_path}")
        info.pack(fill="x", padx=10, pady=(10, 0))

        cols = ("code", "total_pages", "breakdown")
        tree = ttk.Treeview(self, columns=cols, show="headings")
        tree.heading("code", text="ECS Code")
        tree.heading("total_pages", text="Pages Matched (total)")
        tree.heading("breakdown", text="Per-file breakdown")
        tree.column("code", width=220, anchor="w")
        tree.column("total_pages", width=160, anchor="center")
        tree.column("breakdown", width=460, anchor="w")
        tree.pack(fill="both", expand=True, padx=10, pady=10)

        for r in rows:
            tree.insert("", "end", values=(r["code"], r["total_pages"], r["breakdown"]))

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="right")


