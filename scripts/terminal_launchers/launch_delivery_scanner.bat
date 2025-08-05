@echo off
title DELIVERY_SCANNER - Worker Terminal
color 0A
echo.
echo ================================
echo    DELIVERY_SCANNER WORKER
echo ================================
echo.
echo Starting delivery_scanner...
echo Worker file: delivery_scanner.py
echo Python: C:\Point Detection\.venv\Scripts\python.exe
echo.

cd /d "C:\Point Detection"

"C:\Point Detection\.venv\Scripts\python.exe" "C:\Point Detection\app\workers\delivery_scanner.py"

echo.
echo ================================
echo DELIVERY_SCANNER HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
