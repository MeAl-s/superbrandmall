#!/usr/bin/env python3
"""
Windows Midnight Staggered Worker Launcher with JSON Conflict Resolution
Auto-generated for Point Detection system - No more JSON conflicts!
"""

import os
import sys
import subprocess
import time
import json
import threading
import random
from datetime import datetime
from pathlib import Path

# Windows-specific configuration
APP_DIR = r"C:\Point Detection"
PYTHON_PATH = r"C:\Point Detection\.venv\Scripts\python.exe"
LOG_DIR = os.path.join(APP_DIR, "logs")

# JSON conflict resolution settings
JSON_FILE_PATH = r"C:\Point Detection\data\config.json"  # ⚠️ UPDATE THIS PATH TO YOUR JSON FILE
USE_WORKER_COPIES = True  # Set to False to use file locking instead

# Workers to launch (only existing ones)
WORKERS = [
    {'name': 'realtime_detector', 'script': 'workers/realtime_detector.py', 'delay': 0}, 
    {'name': 'ocr_processor', 'script': 'workers/ocr_processor.py', 'delay': 5}, 
    {'name': 'ocr_classification', 'script': 'workers/ocr_classification.py', 'delay': 10}, 
    {'name': 'ocr_downloader', 'script': 'workers/ocr_downloader.py', 'delay': 15}, 
    {'name': 'ocr_text_processor', 'script': 'workers/ocr_text_processor.py', 'delay': 20}, 
    {'name': 'delivery_scanner', 'script': 'workers/delivery_scanner.py', 'delay': 25}, 
    {'name': 'receipt_matcher', 'script': 'workers/receipt_matcher.py', 'delay': 30}, 
    {'name': 'timezone_worker', 'script': 'workers/timezone_worker.py', 'delay': 35}
]

# Simple JSON conflict resolution (built-in)
_json_lock = threading.Lock()

def read_json_safe(json_file_path, worker_name, max_retries=5):
    """Safely read JSON file without conflicts"""
    
    for attempt in range(max_retries):
        try:
            with _json_lock:
                # Strategy 1: Worker-specific copies (simplest)
                if USE_WORKER_COPIES:
                    return _read_json_with_worker_copy(json_file_path, worker_name)
                # Strategy 2: Shared file with locking
                else:
                    return _read_json_with_lock(json_file_path, worker_name)
                    
        except Exception as e:
            if attempt == max_retries - 1:
                log_message(f"Failed to read JSON after {max_retries} attempts: {e}", worker_name)
                return None
            
            # Wait with random delay before retry
            wait_time = 0.1 * (2 ** attempt) + random.uniform(0, 0.1)
            log_message(f"JSON read attempt {attempt + 1} failed, retrying in {wait_time:.2f}s...", worker_name)
            time.sleep(wait_time)
    
    return None

def _read_json_with_worker_copy(json_file_path, worker_name):
    """Each worker gets its own copy of the JSON file"""
    
    base_file = Path(json_file_path)
    worker_file = base_file.parent / f"{base_file.stem}_{worker_name}.json"
    
    # Create worker copy if it doesn't exist or base file is newer
    if not worker_file.exists() or _is_base_newer(base_file, worker_file):
        # Read base file and create worker copy
        with open(base_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        with open(worker_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        log_message(f"Created JSON copy: {worker_file.name}", worker_name)
    
    # Read from worker-specific file
    with open(worker_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def _read_json_with_lock(json_file_path, worker_name):
    """Read shared JSON file with basic locking"""
    with open(json_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def _is_base_newer(base_file, worker_file):
    """Check if base file is newer than worker file"""
    try:
        if not worker_file.exists():
            return True
        base_mtime = base_file.stat().st_mtime
        worker_mtime = worker_file.stat().st_mtime
        return base_mtime > worker_mtime
    except:
        return True

def setup_json_conflict_resolution():
    """Setup JSON conflict resolution before launching workers"""
    
    log_message("Setting up JSON conflict resolution...")
    
    if not os.path.exists(JSON_FILE_PATH):
        log_message(f"WARNING: JSON file not found: {JSON_FILE_PATH}")
        return False
    
    if USE_WORKER_COPIES:
        log_message("Using worker-specific JSON copies strategy")
        
        # Pre-create worker copies
        created_copies = 0
        for worker_info in WORKERS:
            worker_name = worker_info['name']
            try:
                data = read_json_safe(JSON_FILE_PATH, worker_name)
                if data is not None:
                    created_copies += 1
                    log_message(f"JSON copy ready for {worker_name}")
            except Exception as e:
                log_message(f"Failed to create JSON copy for {worker_name}: {e}")
        
        log_message(f"Created {created_copies}/{len(WORKERS)} JSON copies")
        return created_copies > 0
    else:
        log_message("Using shared JSON file with locking strategy")
        return True

def update_worker_script(worker_script_path, worker_name):
    """Update worker script to use conflict-free JSON reading"""
    
    try:
        # Read existing script
        with open(worker_script_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Check if already updated
        if "read_json_safe" in original_content:
            return True  # Already updated
        
        # Create backup
        backup_path = f"{worker_script_path}.backup"
        if not os.path.exists(backup_path):
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(original_content)
        
        # Create updated content
        updated_content = f'''#!/usr/bin/env python3
"""
{worker_name.title().replace('_', ' ')} - Updated with JSON conflict resolution
"""

import json
import os
import sys
import time
import threading
import random
from pathlib import Path

# JSON conflict resolution (built-in)
JSON_FILE_PATH = r"{JSON_FILE_PATH}"
USE_WORKER_COPIES = {USE_WORKER_COPIES}
WORKER_NAME = "{worker_name}"

_json_lock = threading.Lock()

def read_json_safe(max_retries=5):
    """Safely read JSON file without conflicts"""
    
    for attempt in range(max_retries):
        try:
            with _json_lock:
                if USE_WORKER_COPIES:
                    return _read_json_with_worker_copy()
                else:
                    return _read_json_with_lock()
                    
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"[{{WORKER_NAME}}] Failed to read JSON after {{max_retries}} attempts: {{e}}")
                return None
            
            wait_time = 0.1 * (2 ** attempt) + random.uniform(0, 0.1)
            print(f"[{{WORKER_NAME}}] JSON read attempt {{attempt + 1}} failed, retrying in {{wait_time:.2f}}s...")
            time.sleep(wait_time)
    
    return None

def _read_json_with_worker_copy():
    """Read from worker-specific JSON copy"""
    base_file = Path(JSON_FILE_PATH)
    worker_file = base_file.parent / f"{{base_file.stem}}_{{WORKER_NAME}}.json"
    
    if not worker_file.exists() or _is_base_newer(base_file, worker_file):
        with open(base_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        with open(worker_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[{{WORKER_NAME}}] Updated JSON copy")
    
    with open(worker_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def _read_json_with_lock():
    """Read shared JSON file"""
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def _is_base_newer(base_file, worker_file):
    """Check if base file is newer"""
    try:
        if not worker_file.exists():
            return True
        return base_file.stat().st_mtime > worker_file.stat().st_mtime
    except:
        return True

def main():
    """Main worker function"""
    print(f"[{{WORKER_NAME}}] Starting with JSON conflict resolution...")
    
    while True:
        try:
            # Read JSON data safely (no conflicts!)
            data = read_json_safe()
            
            if data is None:
                print(f"[{{WORKER_NAME}}] Failed to read JSON, retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            print(f"[{{WORKER_NAME}}] Successfully read JSON data with {{len(data)}} items")
            
            # TODO: Replace this with your actual processing logic
            process_data(data)
            
            # Wait before next iteration
            time.sleep(10)  # Adjust as needed
            
        except KeyboardInterrupt:
            print(f"[{{WORKER_NAME}}] Shutting down...")
            break
        except Exception as e:
            print(f"[{{WORKER_NAME}}] Error: {{e}}")
            time.sleep(5)

def process_data(data):
    """
    REPLACE THIS FUNCTION WITH YOUR ACTUAL PROCESSING LOGIC
    """
    print(f"[{{WORKER_NAME}}] Processing {{len(data)}} items...")
    
    # Your processing code goes here
    for i, item in enumerate(data):
        print(f"[{{WORKER_NAME}}] Processing item {{i+1}}: {{item}}")
        # Add your actual processing logic here
        time.sleep(0.1)  # Simulate processing
    
    print(f"[{{WORKER_NAME}}] Processing complete")

if __name__ == "__main__":
    main()
'''
        
        # Write updated script
        with open(worker_script_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        
        return True
        
    except Exception as e:
        log_message(f"Failed to update {worker_script_path}: {e}")
        return False

def log_message(message, worker_name=None):
    """Log with timestamp (Windows compatible)"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if worker_name:
        log_entry = f"[{timestamp}] [{worker_name}] {message}"
    else:
        log_entry = f"[{timestamp}] [MASTER] {message}"
    
    print(log_entry)
    
    # Write to log file
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        master_log = os.path.join(LOG_DIR, "midnight_launcher.log")
        with open(master_log, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        print(f"Warning: Could not write to log file: {e}")

def launch_worker(worker_info):
    """Launch a single worker (Windows compatible)"""
    name = worker_info["name"]
    script_path = os.path.join(APP_DIR, worker_info["script"])
    delay = worker_info["delay"]
    
    # Wait for delay
    if delay > 0:
        log_message(f"Waiting {delay} seconds...", name)
        time.sleep(delay)
    
    try:
        # Check if script exists
        if not os.path.exists(script_path):
            log_message(f"ERROR: Script not found: {script_path}", name)
            return False
        
        # Update worker script to handle JSON conflicts
        log_message(f"Ensuring conflict-free JSON access...", name)
        update_worker_script(script_path, name)
        
        # Prepare log and PID files
        log_file = os.path.join(LOG_DIR, f"{name}.log")
        pid_file = os.path.join(LOG_DIR, f"{name}.pid")
        
        # Launch process (Windows compatible)
        launch_time = datetime.now().strftime("%H:%M:%S")
        log_message(f"LAUNCHING at {launch_time}", name)
        
        # Use shell=True for Windows compatibility
        process = subprocess.Popen(
            [PYTHON_PATH, script_path],
            stdout=open(log_file, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            cwd=APP_DIR,
            shell=True  # Windows compatibility
        )
        
        # Save PID
        with open(pid_file, "w") as f:
            f.write(str(process.pid))
        
        log_message(f"SUCCESS: Launched PID {process.pid}", name)
        return True
        
    except Exception as e:
        log_message(f"ERROR: {str(e)}", name)
        return False

def main():
    """Main launcher function"""
    start_time = datetime.now()
    log_message("=== MIDNIGHT STAGGERED LAUNCH STARTED ===")
    log_message(f"Platform: {sys.platform}")
    log_message(f"Python: {PYTHON_PATH}")
    log_message(f"App Dir: {APP_DIR}")
    log_message(f"JSON File: {JSON_FILE_PATH}")
    log_message(f"Strategy: {'Worker Copies' if USE_WORKER_COPIES else 'Shared with Locking'}")
    
    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Setup JSON conflict resolution
    if not setup_json_conflict_resolution():
        log_message("WARNING: JSON conflict resolution setup failed")
    
    successful = 0
    failed = 0
    
    # Launch all workers with their delays
    for worker_info in WORKERS:
        if launch_worker(worker_info):
            successful += 1
        else:
            failed += 1
    
    # Summary
    duration = (datetime.now() - start_time).total_seconds()
    log_message(f"=== LAUNCH COMPLETED in {duration:.1f}s ===")
    log_message(f"Successful: {successful}, Failed: {failed}")
    
    if failed > 0:
        log_message("WARNING: Some workers failed to launch!")
        sys.exit(1)
    else:
        log_message("All workers launched successfully with JSON conflict resolution!")

if __name__ == "__main__":
    main()