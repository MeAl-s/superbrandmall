@echo off
title REALTIME_DETECTOR - Worker Terminal
color 0A
echo.
echo ================================
echo    REALTIME_DETECTOR WORKER
echo ================================
echo.
echo Starting realtime_detector...
echo Worker file: realtime_detector.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\realtime_detector.py"

echo.
echo ================================
echo REALTIME_DETECTOR HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
