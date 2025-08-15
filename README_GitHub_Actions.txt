GitHub Actions: Windows EXE Builder
==================================

This repository builds a Windows .exe for the ECS PDF Highlighter using GitHub-hosted Windows runners.
No local Python install is required.

How to use
----------
1) Create a new **private** GitHub repository.
2) Upload these three files to the root of the repo:
   - ecs_pdf_highlighter.py
   - requirements.txt
   - .github/workflows/build.yml
3) In GitHub, go to the repo's **Actions** tab. You should see "Build Windows EXE".
4) Click it, then click **Run workflow** (workflow_dispatch).
   - The job will install dependencies and run PyInstaller on a Windows VM.
5) When it finishes, open the job run → **Artifacts** → download **ECS_PDF_Highlighter**.
   - Inside is `ECS_PDF_Highlighter.exe`.
6) Use that .exe on any Windows machine (no Python needed).

Usage of the EXE
----------------
- Double-click `ECS_PDF_Highlighter.exe`.
- Enter week number.
- Drag & drop your Excel and PDF into the window when prompted.
- The result will be `<original_pdf>_WK<week>_priorities.pdf` with highlights for ECS Codes found in the spreadsheet.

Notes
-----
- If you want a console visible when the EXE runs, edit the workflow and remove `--noconsole`.
- If your header label is not "ECS Codes" or "ECS Code", edit `ecs_pdf_highlighter.py` and add it to `target_labels`.
