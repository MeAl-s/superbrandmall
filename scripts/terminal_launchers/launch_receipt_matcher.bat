@echo off
title RECEIPT_MATCHER - Worker Terminal
color 0A
echo.
echo ================================
echo    RECEIPT_MATCHER WORKER
echo ================================
echo.
echo Starting receipt_matcher...
echo Worker file: receipt_matcher.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\receipt_matcher.py"

echo.
echo ================================
echo RECEIPT_MATCHER HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
