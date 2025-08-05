# realtime_classifier.py - Real-time file classification based on hddc01 URLs
import os
import json
import shutil
import re
import time
import signal
import sys
from datetime import datetime
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# DATA FOLDER SETUP
# ═══════════════════════════════════════════════════════════════

# Define paths
SOURCE_DIR = Path(__file__).parent / "data" / "receipt_files"
URL_DIR = Path(__file__).parent / "data" / "receipt_ocring"
NO_URL_DIR = Path(__file__).parent / "data" / "receipt_checked"

def setup_classification_folders():
    """Create classification folders"""
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    URL_DIR.mkdir(parents=True, exist_ok=True)
    NO_URL_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Classification folders ready:")
    print(f"   📂 Source: {SOURCE_DIR}")
    print(f"   ✅ With URLs: {URL_DIR}")
    print(f"   📋 No URLs: {NO_URL_DIR}")

# Setup folders on import
setup_classification_folders()

# ═══════════════════════════════════════════════════════════════
# CLASSIFICATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def contains_hddc01_url(text):
    """Check if text contains a URL that starts with https://hddc01.superbrandmall.com:443/"""
    hddc01_pattern = r'https://hddc01\.superbrandmall\.com:443/[^\s<>"{}|\\^`\[\]]+'
    return bool(re.search(hddc01_pattern, text))

def classify_single_file(file_path):
    """
    Classify a single file and move it to appropriate directory
    Returns: (success, destination_type, error_message)
    """
    try:
        # Read file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        has_url = False
        file_type = "unknown"
        
        # Try to parse as JSON first
        try:
            data = json.loads(content)
            file_type = "json"
            
            # Extract the data field if it exists
            if 'data' in data and data['data']:
                data_content = str(data['data'])
                has_url = contains_hddc01_url(data_content)
            else:
                # If no data field, check entire JSON content
                has_url = contains_hddc01_url(content)
                
        except json.JSONDecodeError:
            # If not valid JSON, check raw content
            file_type = "non-json"
            has_url = contains_hddc01_url(content)
        
        # Determine destination and move file
        if has_url:
            destination = URL_DIR / file_path.name
            destination_type = "receipt_ocring"
        else:
            destination = NO_URL_DIR / file_path.name
            destination_type = "receipt_checked"
        
        # Move the file
        shutil.move(str(file_path), str(destination))
        
        return True, destination_type, None
        
    except Exception as e:
        return False, None, str(e)

# ═══════════════════════════════════════════════════════════════
# REAL-TIME CLASSIFIER
# ═══════════════════════════════════════════════════════════════

class RealtimeFileClassifier:
    def __init__(self):
        self.processed_files = set()  # Track files we've seen (session only)
        self.classification_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "files_with_urls": 0,
            "files_without_urls": 0,
            "error_files": 0
        }
        
    def scan_for_new_files(self):
        """Scan source directory for new files"""
        if not SOURCE_DIR.exists():
            return []
        
        new_files = []
        
        # Get all files in source directory
        for file_path in SOURCE_DIR.iterdir():
            if file_path.is_file():
                file_key = f"{file_path.name}_{file_path.stat().st_mtime}"
                
                # Check if we've already processed this file
                if file_key not in self.processed_files:
                    new_files.append(file_path)
                    self.processed_files.add(file_key)
        
        return new_files
    
    def process_new_files(self, files):
        """Process and classify new files"""
        if not files:
            return
        
        print(f"\n🎯 Processing {len(files)} new files for classification...")
        
        for i, file_path in enumerate(files, 1):
            print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
            
            # Classify the file
            success, destination_type, error = classify_single_file(file_path)
            
            if success:
                if destination_type == "receipt_ocring":
                    print(f"    ✅ → receipt_ocring (contains hddc01 URL)")
                    self.classification_stats["files_with_urls"] += 1
                else:
                    print(f"    📋 → receipt_checked (no hddc01 URL)")
                    self.classification_stats["files_without_urls"] += 1
            else:
                print(f"    ❌ Error: {error}")
                self.classification_stats["error_files"] += 1
            
            self.classification_stats["total_processed"] += 1
        
        print(f"\n📊 Batch Classification Summary:")
        print(f"    ✅ With URLs: {self.classification_stats['files_with_urls']}")
        print(f"    📋 Without URLs: {self.classification_stats['files_without_urls']}")
        print(f"    ❌ Errors: {self.classification_stats['error_files']}")
    
    def run_realtime_monitor(self, check_interval=10):
        """Main real-time monitoring loop"""
        print(f"\n🚀 Starting Real-time File Classification Monitor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📂 Monitoring: {SOURCE_DIR}")
        print(f"✅ Files with URLs → {URL_DIR}")
        print(f"📋 Files without URLs → {NO_URL_DIR}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping file classifier...")
            print(f"🗑️ Session data cleared. Goodbye!")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            while True:
                print(f"\n🔍 Scanning for new files... {datetime.now().strftime('%H:%M:%S')}")
                
                # Scan for new files
                new_files = self.scan_for_new_files()
                
                if new_files:
                    self.process_new_files(new_files)
                else:
                    print("📭 No new files to classify")
                
                print(f"\n📊 Session Stats:")
                print(f"    🕐 Running since: {self.classification_stats['start_time']}")
                print(f"    🔄 Total processed: {self.classification_stats['total_processed']}")
                print(f"    ✅ With URLs: {self.classification_stats['files_with_urls']}")
                print(f"    📋 Without URLs: {self.classification_stats['files_without_urls']}")
                print(f"    ❌ Errors: {self.classification_stats['error_files']}")
                print(f"    📚 Files tracked: {len(self.processed_files)}")
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

# ═══════════════════════════════════════════════════════════════
# MANUAL CLASSIFICATION FUNCTION
# ═══════════════════════════════════════════════════════════════

def classify_existing_files():
    """Manually classify all existing files in source directory"""
    print(f"\n🔄 Classifying all existing files in {SOURCE_DIR}")
    
    if not SOURCE_DIR.exists():
        print(f"❌ Source directory {SOURCE_DIR} does not exist!")
        return
    
    files_with_url = 0
    files_without_url = 0
    error_files = 0
    
    # Get all files
    files = [f for f in SOURCE_DIR.iterdir() if f.is_file()]
    
    if not files:
        print("📭 No files found to classify")
        return
    
    print(f"📂 Found {len(files)} files to classify")
    print("-" * 60)
    
    for i, file_path in enumerate(files, 1):
        print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
        
        success, destination_type, error = classify_single_file(file_path)
        
        if success:
            if destination_type == "receipt_ocring":
                print(f"    ✅ → receipt_ocring (contains hddc01 URL)")
                files_with_url += 1
            else:
                print(f"    📋 → receipt_checked (no hddc01 URL)")
                files_without_url += 1
        else:
            print(f"    ❌ Error: {error}")
            error_files += 1
    
    # Print summary
    print("\n" + "=" * 60)
    print("CLASSIFICATION SUMMARY")
    print("=" * 60)
    print(f"✅ Files with hddc01 URLs → receipt_ocring: {files_with_url}")
    print(f"📋 Files without hddc01 URLs → receipt_checked: {files_without_url}")
    print(f"❌ Files with errors: {error_files}")
    print(f"📊 Total files processed: {files_with_url + files_without_url + error_files}")
    print("=" * 60)

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time File Classifier for hddc01 URLs")
    parser.add_argument("--interval", type=int, default=10,
                       help="Check interval in seconds")
    parser.add_argument("--classify-existing", action="store_true",
                       help="Classify all existing files once and exit")
    
    args = parser.parse_args()
    
    if args.classify_existing:
        # Classify all existing files once
        classify_existing_files()
    else:
        # Start real-time monitoring
        classifier = RealtimeFileClassifier()
        classifier.run_realtime_monitor(args.interval)

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

"""
REAL-TIME FILE CLASSIFIER

USAGE:

1. Start real-time monitoring (default 10-second intervals):
   python realtime_classifier.py

2. Custom monitoring interval:
   python realtime_classifier.py --interval 20

3. Classify all existing files once:
   python realtime_classifier.py --classify-existing

RUNNING THE FULL PIPELINE:

Terminal 1:
python realtime_detector.py

Terminal 2:  
python ocr_processor.py

Terminal 3:
python realtime_classifier.py

WORKFLOW:
1. realtime_detector.py → Detects new receipts → Saves to JSON
2. ocr_processor.py → Downloads receipt files → Saves to data/receipt_files/
3. realtime_classifier.py → Monitors data/receipt_files/ → Classifies files

FOLDER STRUCTURE:
C:\Point Detection\
├── realtime_detector.py
├── ocr_processor.py
├── realtime_classifier.py
└── data/
    ├── real_time_response/
    │   └── new_receipts_today_2025-07-22.json
    ├── receipt_files/           ← Monitors this folder
    ├── receipt_ocring/          ← Files WITH hddc01 URLs
    │   ├── file1.json
    │   └── file2.json
    └── receipt_checked/         ← Files WITHOUT hddc01 URLs
        ├── file3.json
        └── file4.json

FEATURES:
✅ Real-time monitoring every 10 seconds
✅ Automatic classification based on hddc01 URLs
✅ Session-only tracking (no history files)
✅ Handles both JSON and non-JSON files
✅ Error handling and reporting
✅ Graceful shutdown with Ctrl+C
✅ Manual classification option for existing files
"""