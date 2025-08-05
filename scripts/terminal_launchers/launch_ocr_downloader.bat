@echo off
title OCR_DOWNLOADER - Worker Terminal
color 0A
echo.
echo ================================
echo    OCR_DOWNLOADER WORKER
echo ================================
echo.
echo Starting ocr_downloader...
echo Worker file: ocr_downloader.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\ocr_downloader.py"

echo.
echo ================================
echo OCR_DOWNLOADER HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
