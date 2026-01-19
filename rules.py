import os, re, hashlib
from collections import defaultdict, deque
import fitz
import pandas as pd


# ======================= Normalização & utilitários =======================
DASH_CHARS = "-\u2010\u2011\u2012\u2013\u2014\u2212"  # -, ‐, -, ‒, –, —, −
_STRIP_EDGE_PUNCT = re.compile(r'^[\s\"\'\(\)\[\]\{\}:;,.–—\-]+|[\s\"\'\(\)\[\]\{\}:;,.–—\-]+$')

# A3 (72 pt/in)
A3_PORTRAIT = (842.0, 1191.0)
A3_LANDSCAPE = (1191.0, 842.0)

# “Summary-like”
SUMMARY_KEYWORDS = [
    "summary", "contents", "index", "bill of materials", "bom",
    "schedule", "table of contents", "support schedule", "legend"
]
SUMMARY_KEYWORDS_RE = [re.compile(rf"\b{re.escape(k)}\b", re.IGNORECASE) for k in SUMMARY_KEYWORDS]

EMIT_DIAGNOSTICS = False  # opcional: CSV extra de diagnóstico

# === Summary/TOC sem gate de tamanho ===
SUMMARY_TOC_RE = [
    re.compile(r"\bsummary\b", re.IGNORECASE),
    re.compile(r"\btable of contents\b", re.IGNORECASE),
]


def is_summary_keyword_page(pdf_path, page_idx, first_pages_only=7):
    """True se a página contém 'summary' ou 'table of contents'. Checa apenas as N primeiras por padrão."""
    try:
        if first_pages_only and first_pages_only > 0 and page_idx >= first_pages_only:
            return False
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            txt = (pg.get_text("text") or "")
            for rx in SUMMARY_TOC_RE:
                if rx.search(txt):
                    return True
    except Exception:
        return False
    return False


def unify_dashes(s: str) -> str:
    if not s:
        return s
    for ch in DASH_CHARS[1:]:
        s = s.replace(ch, "-")
    return s.replace("\u00AD", "")  # soft hyphen


def normalize_base(token: str) -> str:
    if not token:
        return ""
    cleaned = _STRIP_EDGE_PUNCT.sub("", token)
    if not cleaned:
        return ""
    cleaned = unify_dashes(cleaned)
    return cleaned.strip().lower()


def normalize_nosep(token: str) -> str:
    if not token:
        return ""
    token = unify_dashes(token).lower()
    # Mantém apenas [0-9a-z-]
    return re.sub(r'[^0-9a-z\-]', '', token)


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name).strip()


def uniquify_path(path: str) -> str:
    base, ext = os.path.splitext(path)
    out = path
    i = 1
    while os.path.exists(out):
        out = f"{base} ({i}){ext}"
        i += 1
    return out


# ========================== Excel & lista de ECS ==========================
def load_table_with_dynamic_header(xlsx_path, sheet_name=None):
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, dtype=str, engine="openpyxl")
    target_labels = {"ecs codes", "ecs code"}
    header_row_idx = None
    for i in range(len(df)):
        row = df.iloc[i].astype(str)
        if any(str(cell).strip().lower() in target_labels for cell in row):
            header_row_idx = i
            break
    if header_row_idx is None:
        return None
    header = df.iloc[header_row_idx].tolist()
    data = df.iloc[header_row_idx + 1:].reset_index(drop=True)
    data.columns = header
    data = data.dropna(axis=1, how='all')
    return data


def extract_ecs_codes_from_df(df):
    if df is None or df.empty:
        return set(), {}
    cols = [c for c in df.columns if str(c).strip().lower() in ("ecs codes", "ecs code")]
    if not cols:
        return set(), {}
    raw = []
    for c in cols:
        raw += df[c].dropna().astype(str).tolist()
    # Split robusto
    tokens = []
    for v in raw:
        parts = re.split(r"[,;\n/\t ]+", v)
        for p in parts:
            t = p.strip().strip('"\'')
            if t and re.search(r"[A-Za-z]", t) and re.search(r"\d", t) and " " not in t:
                tokens.append(t)

    ecs_set = set()
    original_map = {}
    for t in tokens:
        low = normalize_base(t)
        if low and (low not in ecs_set):
            ecs_set.add(low)
            original_map[low] = t
    return ecs_set, original_map


def build_compare_index(ecs_primary, ignore_leading_digit):
    cmp_keys = set()
    nosep_to_primary = {}
    for primary in ecs_primary:
        k = normalize_nosep(primary)
        if k:
            cmp_keys.add(k)
            nosep_to_primary[k] = primary
        if ignore_leading_digit and primary and primary[0].isdigit():
            k2 = normalize_nosep(primary[1:])
            if k2:
                cmp_keys.add(k2)
                nosep_to_primary[k2] = primary
    max_code_len = max((len(k) for k in cmp_keys), default=0)
    return cmp_keys, nosep_to_primary, max_code_len


def build_contextual_indexes(ecs_primary_all: set):
    """
    Contextual ECS matching rules:

    - Surveys: unit-aware -> match FULL normalized code only (e.g. '1hk10st4072')
    - Drawings: unitless -> match WITHOUT the leading unit digit (e.g. 'hk10st4072')
      plus a leading-dash variant (e.g. '-hk10st4072') to match drawings that show codes with a leading '-'.

    - EXCEPTION: HG systems (0HG/1HG/9HG):
        * Drawings are unit-aware for HG -> match FULL code (e.g. '1hg0101zl...')
        * BUT if the drawing shows a leading '-' (e.g. '-hg0101zl...'), it can match ANY unit.
          In that case we allow '-'+remainder and map it to ALL HG primaries that share the remainder.

    Returns:
        cmp_keys_survey: set[str]
        cmp_keys_drawing: set[str]
        cmp_to_primaries: dict[str, list[str]]  (one cmp-key can map to multiple unit-specific primaries)
        max_code_len: int
    """
    cmp_survey = set()
    cmp_drawing = set()
    cmp_to_primaries: Dict[str, List[str]] = {}

    def _add_map(k: str, primary: str, target_set: set):
        if not k:
            return
        target_set.add(k)
        cmp_to_primaries.setdefault(k, [])
        if primary not in cmp_to_primaries[k]:
            cmp_to_primaries[k].append(primary)

    for primary in ecs_primary_all:
        full = normalize_nosep(primary)
        if not full:
            continue

        # Survey: always unit-aware (full code)
        _add_map(full, primary, cmp_survey)

        # Determine HG (e.g. 0HG/1HG/9HG)
        is_hg = bool(len(full) >= 3 and full[0].isdigit() and full[1:3] == "hg")

        # Build remainder (strip leading unit digit if present)
        if primary and str(primary)[0].isdigit():
            remainder = normalize_nosep(str(primary)[1:])
        else:
            remainder = full

        remainder = (remainder or "").lstrip("-")
        if not remainder:
            continue

        if is_hg:
            # HG drawings must match the unit (full code)
            _add_map(full, primary, cmp_drawing)
            # But leading '-' in drawings can match any unit
            _add_map("-" + remainder, primary, cmp_drawing)
        else:
            # Non-HG drawings are unitless
            _add_map(remainder, primary, cmp_drawing)
            _add_map("-" + remainder, primary, cmp_drawing)

    max_code_len = 0
    if cmp_survey or cmp_drawing:
        max_code_len = max((len(k) for k in (cmp_survey | cmp_drawing)), default=0)
    return cmp_survey, cmp_drawing, cmp_to_primaries, max_code_len


def build_prefixes_and_firstchars(cmp_keys_nosep):
    prefixes = set()
    first_chars = set()
    for key in cmp_keys_nosep:
        if not key:
            continue
        first_chars.add(key[0])
        for i in range(1, len(key) + 1):
            prefixes.add(key[:i])
    return prefixes, first_chars




def infer_building_from_code(pretty_code: str) -> str:
    s = unify_dashes(pretty_code or "").strip()
    if not s:
        return "UNKWN"
    s_no = re.sub(r'[^0-9A-Za-z\-]', '', s)
    # Regra simples: se tiver hífen cedo, 2 letras; senão 3 letras
    t = re.sub(r'^[0-9\-]+', '', s_no)
    m2 = _FIRST_LETTERS.match(t)
    if not m2:
        return "UNKWN"
    letters = m2.group(0).upper()
    if '-' in s_no[:5]:
        return letters[:2] if len(letters) >= 2 else letters
    return letters[:3] if len(letters) >= 3 else letters


# ========================= Survey / Duplicatas / Rev ====================
def is_survey_pdf(path, size_limit_bytes=1_200_000):
    try:
        sz = os.path.getsize(path)
    except Exception:
        sz = 0
    name = os.path.basename(path).lower()
    looks_name = ("cut l" in name) or ("cut length report" in name)
    return looks_name and (sz > 0 and sz <= size_limit_bytes)


# Fingerprints de página
def fingerprint_page_text(pg):
    try:
        txt = pg.get_text("text") or ""
        txt = re.sub(r'\s+', ' ', txt).strip().lower()
        h = hashlib.sha1()
        h.update(txt.encode("utf-8", errors="ignore"))
        r = pg.rect
        h.update(f"{int(r.width)}x{int(r.height)}".encode())
        return "T:" + h.hexdigest()
    except Exception:
        return None


def fingerprint_page_image(pg, scale=0.35):
    try:
        mat = fitz.Matrix(scale, scale)
        pix = pg.get_pixmap(matrix=mat, alpha=False)
        b = pix.samples
        h = hashlib.sha1()
        h.update(b[:200000])
        return "I:" + h.hexdigest()
    except Exception:
        return None


def page_fingerprint(pdf_path, page_idx):
    try:
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            fh = fingerprint_page_text(pg)
            if fh:
                return fh
            ih = fingerprint_page_image(pg)
            return ih or f"X:{pdf_path}:{page_idx}"
    except Exception:
        return f"X:{pdf_path}:{page_idx}"


# -------- Detecção de summary --------
_NEIGHBOR_DELTAS = (-4, -3, -2, -1, 1, 2, 3, 4)
_TRAILING_NUM_RE = re.compile(r"^(.*?)(\d+)$", re.IGNORECASE)


def _neighbors_of_code_norm(norm_code: str):
    m = _TRAILING_NUM_RE.match(norm_code)
    if not m:
        return []
    prefix, num = m.groups()
    width = len(num)
    base = int(num)
    out = []
    for d in _NEIGHBOR_DELTAS:
        n = base + d
        if n < 0:
            continue
        out.append(f"{prefix}{str(n).zfill(width)}")
    return out


def looks_like_summary_by_neighbors(codes_on_page_pretty, neighbor_min_hits=3):
    norm_set = {normalize_base(c) for c in codes_on_page_pretty if c}
    if not norm_set:
        return False
    for norm_code in norm_set:
        neighbors = _neighbors_of_code_norm(norm_code)
        if not neighbors:
            continue
        hits = sum(1 for nc in neighbors if nc in norm_set)
        if hits >= neighbor_min_hits:
            return True
    return False


def is_summary_like(pdf_path, page_idx, codes_on_page, threshold=15, neighbor_min_hits=3):
    """
    Heurística para páginas 'summary' (não usada para surveys).
    """
    try:
        if codes_on_page and looks_like_summary_by_neighbors(codes_on_page, neighbor_min_hits=neighbor_min_hits):
            return True
        if len(set(codes_on_page or ())) >= threshold:
            return True
        with fitz.open(pdf_path) as doc:
            pg = doc.load_page(page_idx)
            txt = (pg.get_text("text") or "")
            for rx in SUMMARY_KEYWORDS_RE:
                if rx.search(txt):
                    return True
    except Exception:
        return False
    return False


# -------- Escolha da última revisão (survey & handbooks/desenhos) --------
_REV_LET_RE = re.compile(r'(?:^|[^A-Z0-9])REV\s*([A-Z]{1,3})(?:[^A-Z0-9]|$)', re.IGNORECASE)
_REV_NUM_RE = re.compile(r'(?:^|[^0-9])REV\s*([0-9]{1,3})(?:[^0-9]|$)', re.IGNORECASE)
_TAIL_LET_RE = re.compile(r'([_\-])([A-Z]{1,3})(?:\.pdf)?$', re.IGNORECASE)



def _rev_letters_to_int(s: str) -> int:
    """Convert Excel-style revision letters to an integer: A=1, ..., Z=26, AA=27, AB=28, ..."""
    if not s:
        return 0
    s = re.sub(r'[^A-Z]', '', str(s).upper())
    if not s:
        return 0
    val = 0
    for ch in s:
        if 'A' <= ch <= 'Z':
            val = val * 26 + (ord(ch) - ord('A') + 1)
    return val

def _parse_revision_from_name(name: str) -> int:
    if not name:
        return 0
    m = _REV_LET_RE.search(name)
    if m:
        # Letters: A..Z, AA.. etc (Excel-style ordering)
        return 1000 + _rev_letters_to_int(m.group(1))
    m = _REV_NUM_RE.search(name)
    if m:
        return int(m.group(1))
    m = _TAIL_LET_RE.search(os.path.splitext(name)[0])
    if m:
        return 1000 + _rev_letters_to_int(m.group(2))
    return 0



def _strip_revision_tokens(name: str) -> str:
    base = _REV_LET_RE.sub(' ', name)
    base = _REV_NUM_RE.sub(' ', base)
    base = _TAIL_LET_RE.sub('', os.path.splitext(base)[0])
    base = re.sub(r'\s+', ' ', base).strip().lower()
    return base


def select_latest_survey_revisions(pdf_paths: list) -> list:
    keep = []
    groups = {}
    for p in pdf_paths:
        fname = os.path.basename(p)
        if is_survey_pdf(p):
            base = _strip_revision_tokens(fname)
            rev = _parse_revision_from_name(fname)
            cur = groups.get(base)
            if cur is None or rev > cur[1]:
                groups[base] = (p, rev)
        else:
            keep.append(p)
    keep.extend([v[0] for v in groups.values()])
    keep = sorted(set(keep), key=lambda x: os.path.basename(x).lower())
    return keep


def select_latest_non_survey_revisions(pdf_paths: list) -> list:
    groups = {}
    for p in pdf_paths:
        if is_survey_pdf(p):
            continue
        fname = os.path.basename(p)
        base = _strip_revision_tokens(fname)
        rev = _parse_revision_from_name(fname)
        cur = groups.get(base)
        if cur is None or rev > cur[1]:
            groups[base] = (p, rev)
    out = [p for p in pdf_paths if is_survey_pdf(p)]
    out.extend(v[0] for v in groups.values())
    out = sorted(set(out), key=lambda x: os.path.basename(x).lower())
    return out




def select_latest_revisions_any(pdf_paths: list) -> list:
    """Keep only the latest REV per base filename (generic, no survey/drawing classification)."""
    groups = {}
    for p in pdf_paths:
        fname = os.path.basename(p)
        base = _strip_revision_tokens(fname)
        rev = _parse_revision_from_name(fname)
        cur = groups.get(base)
        if cur is None or rev > cur[1]:
            groups[base] = (p, rev)
    out = [v[0] for v in groups.values()]
    out = sorted(set(out), key=lambda x: os.path.basename(x).lower())
    return out

# ========================== Tarefa do worker (MP) =======================


def chunk_list(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]