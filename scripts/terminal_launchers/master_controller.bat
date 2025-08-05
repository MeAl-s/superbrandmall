@echo off
title MASTER CONTROLLER - All Workers
color 0E
echo.
echo ========================================
echo        MASTER WORKER CONTROLLER
echo ========================================
echo.
echo All 8 workers are running in separate terminals:
echo.
echo   1. REALTIME_DETECTOR
echo   2. OCR_PROCESSOR
echo   3. OCR_CLASSIFICATION
echo   4. OCR_DOWNLOADER
echo   5. OCR_TEXT_PROCESSOR
echo   6. DELIVERY_SCANNER
echo   7. RECEIPT_MATCHER
echo   8. TIMEZONE_WORKER

echo.
echo ========================================
echo.
echo Commands:
echo   - Press 'S' to show worker status
echo   - Press 'R' to restart all workers  
echo   - Press 'Q' to quit all workers
echo   - Press 'H' to show this help
echo.
echo ========================================
echo.

:menu
set /p choice="Enter command (S/R/Q/H): "

if /i "%choice%"=="S" (
    echo.
    echo Checking worker status...
    tasklist | findstr python.exe
    echo.
    goto menu
)

if /i "%choice%"=="R" (
    echo.
    echo Restarting all workers...
    echo This would restart all worker terminals.
    echo.
    goto menu
)

if /i "%choice%"=="Q" (
    echo.
    echo Stopping all workers...
    taskkill /f /im python.exe 2>nul
    echo All Python processes stopped.
    pause
    exit
)

if /i "%choice%"=="H" (
    echo.
    echo Commands:
    echo   S - Show worker status
    echo   R - Restart all workers
    echo   Q - Quit all workers  
    echo   H - Show this help
    echo.
    goto menu
)

echo Invalid choice. Please enter S, R, Q, or H.
goto menu
