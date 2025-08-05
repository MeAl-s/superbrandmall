@echo off
echo Setting up Windows Task Scheduler for Midnight Worker Launcher...

schtasks /create ^
    /tn "MidnightWorkerLauncher" ^
    /tr "\"C:\Point Detection\.venv\Scripts\python.exe\" \"C:\Point Detection\scripts\midnight_staggered_launcher.py\"" ^
    /sc daily ^
    /st 00:00 ^
    /f

if %errorlevel% == 0 (
    echo ✅ Task scheduled successfully!
    echo Task will run daily at midnight
) else (
    echo ❌ Failed to create scheduled task
    echo Please run as Administrator or use manual setup
)

pause
