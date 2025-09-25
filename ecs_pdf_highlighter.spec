# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all

block_cipher = None

# Collect PyMuPDF (fitz) binaries/data so highlights render in the EXE
datas, binaries, hiddenimports = [], [], []
for m in ("fitz",):
    try:
        d, b, h = collect_all(m)
        datas += d
        binaries += b
        hiddenimports += h
    except Exception:
        pass

a = Analysis(
    ['ecs_pdf_highlighter.py'],   # <-- make sure this filename matches your script
    pathex=['.'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# One-file, windowed EXE (no console). UPX disabled for runner compatibility.
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='ecs_pdf_highlighter',   # dist/ecs_pdf_highlighter.exe
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False
)
