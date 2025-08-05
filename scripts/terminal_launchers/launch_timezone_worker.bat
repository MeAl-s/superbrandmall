@echo off
title TIMEZONE_WORKER - Worker Terminal
color 0A
echo.
echo ================================
echo    TIMEZONE_WORKER WORKER
echo ================================
echo.
echo Starting timezone_worker...
echo Worker file: timezone_worker.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\timezone_worker.py"

echo.
echo ================================
echo TIMEZONE_WORKER HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
