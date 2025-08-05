@echo off
title OCR_TEXT_PROCESSOR - Worker Terminal
color 0A
echo.
echo ================================
echo    OCR_TEXT_PROCESSOR WORKER
echo ================================
echo.
echo Starting ocr_text_processor...
echo Worker file: ocr_text_processor.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\ocr_text_processor.py"

echo.
echo ================================
echo OCR_TEXT_PROCESSOR HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
