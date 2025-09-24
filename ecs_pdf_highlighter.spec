# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all

block_cipher = None

datas, binaries, hiddenimports = [], [], []
for m in ("fitz",):  # collect PyMuPDF data
    try:
        d, b, h = collect_all(m)
        datas += d; binaries += b; hiddenimports += h
    except Exception:
        pass

a = Analysis(
    ['ecs_pdf_highlighter.py'],  # <-- rename your .py file to this exact name
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

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='ecs_pdf_highlighter',    # dist/ecs_pdf_highlighter.exe
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,                     # safer on runners
    console=False                  # windowed app
)
