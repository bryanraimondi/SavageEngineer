import os, tempfile
from typing import List, Dict, Optional
import fitz
from rules import normalize_nosep


# ====================== Conversão e mapeamento de ITR ====================
def try_convert_docx_to_pdf(docx_path: str) -> Optional[str]:
    """Converte DOCX para PDF usando docx2pdf (se disponível). Retorna caminho do PDF ou None se falhar."""
    try:
        from docx2pdf import convert  # type: ignore
    except Exception:
        return None
    try:
        tmp_dir = tempfile.mkdtemp(prefix="itr_pdf_")
        out_pdf = os.path.join(tmp_dir, os.path.splitext(os.path.basename(docx_path))[0] + ".pdf")
        convert(docx_path, out_pdf)
        return out_pdf if os.path.exists(out_pdf) else None
    except Exception:
        return None


def build_itr_map(itr_paths: List[str], cmp_keys: set, nosep_to_primary: Dict[str, str]) -> Dict[str, Dict]:
    """
    Produz um dicionário: primary_code -> {'pdf_path': <pdf>, 'pages': int}
    Faz match do ECS code procurando qualquer cmp_key dentro do nome do arquivo.
    """
    out = {}
    for p in itr_paths:
        ext = os.path.splitext(p)[1].lower()
        if ext == ".docx":
            pdfp = try_convert_docx_to_pdf(p)
            if not pdfp:
                messagebox.showwarning("ITR DOCX", f"Não foi possível converter ITR: {os.path.basename(p)}. "
                                                   f"Instale 'docx2pdf' (requer Microsoft Word).")
                continue
            mapped_pdf = pdfp
        elif ext == ".pdf":
            mapped_pdf = p
        else:
            messagebox.showwarning("ITR", f"Formato não suportado (somente .docx ou .pdf): {os.path.basename(p)}")
            continue

        fname_n = normalize_nosep(os.path.basename(p))
        matched_primary = None
        for k in cmp_keys:
            if not k:
                continue
            if k in fname_n:
                matched_primary = nosep_to_primary.get(k, k)
                break
        if not matched_primary:
            # não achou nenhum cmp_key no nome
            continue

        try:
            with fitz.open(mapped_pdf) as doc:
                pages = doc.page_count
        except Exception:
            pages = 0

        out[matched_primary] = {"pdf_path": mapped_pdf, "pages": pages}
    return out


