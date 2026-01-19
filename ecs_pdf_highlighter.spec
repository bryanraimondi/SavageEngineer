# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all

block_cipher = None

# Collect binaries/data/hiddenimports for packages that commonly cause "works locally but not in EXE"
datas, binaries, hiddenimports = [], [], []

def _collect(pkg: str):
    global datas, binaries, hiddenimports
    try:
        d, b, h = collect_all(pkg)
        datas += d
        binaries += b
        hiddenimports += h
    except Exception:
        pass

# PyMuPDF (fitz) is critical
_collect("fitz")

# Common runtime deps
_collect("pandas")
_collect("openpyxl")

# Optional: Aho–Corasick acceleration (if installed)
_collect("ahocorasick")

# Ensure local refactor modules are bundled (they may not be discovered if dynamically imported)
hiddenimports += [
    "rules",
    "pdf_ops",
    "scan_ops",
    "review_ui",
    "itr_ops",
]

a = Analysis(
    ["ecs_pdf_highlighter.py"],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# One-file, windowed EXE (no console). UPX disabled for runner compatibility.
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name="ecs_pdf_highlighter",   # dist/ecs_pdf_highlighter.exe
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
)
