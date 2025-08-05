@echo off
title OCR_CLASSIFICATION - Worker Terminal
color 0A
echo.
echo ================================
echo    OCR_CLASSIFICATION WORKER
echo ================================
echo.
echo Starting ocr_classification...
echo Worker file: ocr_classification.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\ocr_classification.py"

echo.
echo ================================
echo OCR_CLASSIFICATION HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
