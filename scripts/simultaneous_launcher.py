#!/usr/bin/env python3
"""
Multi-Terminal Launcher - 8 Workers in 8 Terminals
Automatically opens 8 separate terminals and runs each worker
"""

import os
import sys
import subprocess
import time
from pathlib import Path
import platform

# Configuration
BASE_DIR = Path(r"C:\Point Detection")
PYTHON_PATH = BASE_DIR / ".venv" / "Scripts" / "python.exe"
APP_DIR = BASE_DIR / "app"
WORKERS_DIR = APP_DIR / "workers"

class MultiTerminalLauncher:
    """Launches workers in separate terminals based on OS"""
    
    def __init__(self):
        self.system = platform.system().lower()
        self.terminals = []
        
        # Worker configurations
        self.workers = [
            {"name": "realtime_detector", "file": "realtime_detector.py", "color": "red"},
            {"name": "ocr_processor", "file": "ocr_processor.py", "color": "green"},
            {"name": "ocr_classification", "file": "ocr_classification.py", "color": "blue"},
            {"name": "ocr_downloader", "file": "ocr_downloader.py", "color": "yellow"},
            {"name": "ocr_text_processor", "file": "ocr_text_processor.py", "color": "magenta"},
            {"name": "delivery_scanner", "file": "delivery_scanner.py", "color": "cyan"},
            {"name": "receipt_matcher", "file": "receipt_matcher.py", "color": "white"},
            {"name": "timezone_worker", "file": "timezone_worker.py", "color": "gray"}
        ]
    
    def create_worker_launcher_script(self, worker_name: str, worker_file: str):
        """Create a launcher script for individual worker"""
        
        script_content = f'''@echo off
title {worker_name.upper()} - Worker Terminal
color 0A
echo.
echo ================================
echo    {worker_name.upper()} WORKER
echo ================================
echo.
echo Starting {worker_name}...
echo Worker file: {worker_file}
echo Python: {PYTHON_PATH}
echo.

cd /d "{BASE_DIR}"

"{PYTHON_PATH}" "{WORKERS_DIR / worker_file}"

echo.
echo ================================
echo {worker_name.upper()} HAS STOPPED
echo ================================
echo Press any key to close this terminal...
pause >nul
'''
        
        # Create scripts directory
        scripts_dir = BASE_DIR / "scripts" / "terminal_launchers"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        
        # Save launcher script
        launcher_script = scripts_dir / f"launch_{worker_name}.bat"
        with open(launcher_script, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        return launcher_script
    
    def launch_windows_terminals(self):
        """Launch workers in separate Windows terminals"""
        
        print("🚀 LAUNCHING 8 WORKERS IN SEPARATE WINDOWS TERMINALS")
        print("=" * 60)
        
        for i, worker in enumerate(self.workers):
            worker_name = worker["name"]
            worker_file = worker["file"]
            worker_path = WORKERS_DIR / worker_file
            
            if not worker_path.exists():
                print(f"❌ Worker file not found: {worker_file}")
                continue
            
            # Create launcher script
            launcher_script = self.create_worker_launcher_script(worker_name, worker_file)
            
            try:
                # Launch in new terminal window
                if self.system == "windows":
                    # Method 1: Using cmd with start command
                    process = subprocess.Popen([
                        "cmd", "/c", "start", 
                        f"'{worker_name.upper()} Worker'",  # Window title
                        "cmd", "/k", str(launcher_script)
                    ], shell=True)
                
                else:  # Linux/Mac fallback
                    # Try various terminal emulators
                    terminal_commands = [
                        ["gnome-terminal", "--title", f"{worker_name.upper()}", "--", "bash", "-c", 
                         f"cd '{BASE_DIR}' && '{PYTHON_PATH}' '{worker_path}'; read -p 'Press Enter to close...'"],
                        ["xterm", "-title", f"{worker_name.upper()}", "-e", 
                         f"cd '{BASE_DIR}' && '{PYTHON_PATH}' '{worker_path}'; read -p 'Press Enter to close...'"],
                        ["konsole", "--title", f"{worker_name.upper()}", "-e", 
                         f"cd '{BASE_DIR}' && '{PYTHON_PATH}' '{worker_path}'; read -p 'Press Enter to close...'"]
                    ]
                    
                    for cmd in terminal_commands:
                        try:
                            process = subprocess.Popen(cmd)
                            break
                        except FileNotFoundError:
                            continue
                    else:
                        print(f"❌ No suitable terminal emulator found for {worker_name}")
                        continue
                
                self.terminals.append({
                    'name': worker_name,
                    'process': process,
                    'launcher': launcher_script
                })
                
                print(f"✅ Launched {worker_name} in terminal {i+1}/8")
                time.sleep(1)  # Small delay between launches
                
            except Exception as e:
                print(f"❌ Failed to launch {worker_name}: {e}")
        
        print(f"\n🎉 Launched {len(self.terminals)} workers in separate terminals!")
        return len(self.terminals)
    
    def launch_windows_terminal_modern(self):
        """Launch using Windows Terminal (modern approach)"""
        
        print("🚀 LAUNCHING USING WINDOWS TERMINAL")
        print("=" * 50)
        
        # Check if Windows Terminal is available
        try:
            subprocess.run(["wt", "--version"], capture_output=True, check=True)
            has_wt = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            has_wt = False
            print("⚠️  Windows Terminal not found, using classic cmd")
        
        if has_wt:
            # Create a single Windows Terminal with multiple tabs
            wt_command = ["wt"]
            
            for i, worker in enumerate(self.workers):
                worker_name = worker["name"]
                worker_file = worker["file"]
                worker_path = WORKERS_DIR / worker_file
                
                if not worker_path.exists():
                    continue
                
                if i > 0:
                    wt_command.extend([";", "new-tab"])
                
                wt_command.extend([
                    "--title", f"{worker_name.upper()}",
                    "cmd", "/k", 
                    f'cd /d "{BASE_DIR}" && "{PYTHON_PATH}" "{worker_path}"'
                ])
            
            try:
                subprocess.Popen(wt_command)
                print("✅ Launched all workers in Windows Terminal with multiple tabs")
                return True
            except Exception as e:
                print(f"❌ Windows Terminal launch failed: {e}")
                return False
        
        return False
    
    def launch_powershell_terminals(self):
        """Launch workers in separate PowerShell terminals"""
        
        print("🚀 LAUNCHING 8 WORKERS IN POWERSHELL TERMINALS")
        print("=" * 60)
        
        for i, worker in enumerate(self.workers):
            worker_name = worker["name"] 
            worker_file = worker["file"]
            worker_path = WORKERS_DIR / worker_file
            
            if not worker_path.exists():
                print(f"❌ Worker file not found: {worker_file}")
                continue
            
            try:
                # PowerShell command to launch in new window
                ps_command = f'''
                Start-Process powershell -ArgumentList @(
                    "-NoExit",
                    "-Command", 
                    "& {{
                        $Host.UI.RawUI.WindowTitle = '{worker_name.upper()} Worker';
                        Write-Host '===============================' -ForegroundColor Green;
                        Write-Host '   {worker_name.upper()} WORKER' -ForegroundColor Green;
                        Write-Host '===============================' -ForegroundColor Green;
                        Write-Host '';
                        Write-Host 'Starting {worker_name}...' -ForegroundColor Yellow;
                        Write-Host 'Worker file: {worker_file}' -ForegroundColor Gray;
                        Write-Host 'Python: {PYTHON_PATH}' -ForegroundColor Gray;
                        Write-Host '';
                        Set-Location '{BASE_DIR}';
                        & '{PYTHON_PATH}' '{worker_path}';
                        Write-Host '';
                        Write-Host '===============================' -ForegroundColor Red;
                        Write-Host '{worker_name.upper()} HAS STOPPED' -ForegroundColor Red;
                        Write-Host '===============================' -ForegroundColor Red;
                        Read-Host 'Press Enter to close';
                    }}"
                )
                '''
                
                # Execute PowerShell command
                process = subprocess.Popen([
                    "powershell", "-WindowStyle", "Normal", "-Command", ps_command
                ], shell=True)
                
                self.terminals.append({
                    'name': worker_name,
                    'process': process
                })
                
                print(f"✅ Launched {worker_name} in PowerShell terminal {i+1}/8")
                time.sleep(1.5)  # Slightly longer delay for PowerShell
                
            except Exception as e:
                print(f"❌ Failed to launch {worker_name}: {e}")
        
        print(f"\n🎉 Launched {len(self.terminals)} workers in PowerShell terminals!")
        return len(self.terminals)
    
    def create_master_controller(self):
        """Create a master controller terminal to manage all workers"""
        
        controller_script = f'''@echo off
title MASTER CONTROLLER - All Workers
color 0E
echo.
echo ========================================
echo        MASTER WORKER CONTROLLER
echo ========================================
echo.
echo All 8 workers are running in separate terminals:
echo.
'''
        
        for i, worker in enumerate(self.workers, 1):
            controller_script += f'echo   {i}. {worker["name"].upper()}\n'
        
        controller_script += f'''
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
'''
        
        # Save controller script
        scripts_dir = BASE_DIR / "scripts" / "terminal_launchers"
        controller_file = scripts_dir / "master_controller.bat"
        
        with open(controller_file, 'w', encoding='utf-8') as f:
            f.write(controller_script)
        
        return controller_file
    
    def launch_all(self):
        """Main launch method - tries different approaches"""
        
        print("🖥️  MULTI-TERMINAL WORKER LAUNCHER")
        print("=" * 50)
        print(f"System: {platform.system()}")
        print(f"Base Directory: {BASE_DIR}")
        print(f"Python: {PYTHON_PATH}")
        print()
        
        # Check if workers exist
        existing_workers = []
        for worker in self.workers:
            worker_path = WORKERS_DIR / worker["file"]
            if worker_path.exists():
                existing_workers.append(worker["name"])
                print(f"✅ Found: {worker['file']}")
            else:
                print(f"❌ Missing: {worker['file']}")
        
        if not existing_workers:
            print("\n❌ No worker files found!")
            return False
        
        print(f"\n🚀 Ready to launch {len(existing_workers)} workers")
        
        # Choose launch method
        print("\nChoose launch method:")
        print("1. Windows Terminal (modern, tabbed)")
        print("2. Separate CMD windows")
        print("3. Separate PowerShell windows")
        
        choice = input("\nEnter choice (1/2/3): ").strip()
        
        success = False
        
        if choice == "1":
            success = self.launch_windows_terminal_modern()
            if not success:
                print("Falling back to separate CMD windows...")
                success = self.launch_windows_terminals()
        elif choice == "2":
            success = self.launch_windows_terminals()
        elif choice == "3":
            success = self.launch_powershell_terminals()
        else:
            print("Invalid choice, using separate CMD windows...")
            success = self.launch_windows_terminals()
        
        if success:
            # Create master controller
            controller_file = self.create_master_controller()
            print(f"\n🎮 Master controller created: {controller_file}")
            
            # Launch master controller
            try:
                subprocess.Popen(["cmd", "/c", "start", "MASTER CONTROLLER", "cmd", "/k", str(controller_file)], shell=True)
                print("✅ Master controller launched!")
            except:
                print("⚠️  Could not launch master controller")
            
            print("\n🎉 ALL WORKERS LAUNCHED SUCCESSFULLY!")
            print("📋 Each worker is running in its own terminal")
            print("🎮 Use the Master Controller to manage all workers")
            
            return True
        else:
            print("\n❌ Failed to launch workers")
            return False


def main():
    """Main function"""
    launcher = MultiTerminalLauncher()
    launcher.launch_all()


if __name__ == "__main__":
    main()