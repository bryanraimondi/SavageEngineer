# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all

block_cipher = None

# Collect data/hiddenimports/binaries for these packages
datas, binaries, hiddenimports = [], [], []
for m in ("tkinterdnd2", "fitz"):  # fitz = PyMuPDF
    try:
        m_datas, m_binaries, m_hidden = collect_all(m)
        datas += m_datas
        binaries += m_binaries
        hiddenimports += m_hidden
    except Exception:
        pass

a = Analysis(
    ['ecs_pdf_highlighter.py'],  # <<< CHANGE if your script name differs
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

# One-file EXE (no console window)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='ecs_pdf_highlighter',  # output: dist/ecs_pdf_highlighter.exe
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,        # safer on GitHub runners
    console=False
)
