# realtime_delivery_scanner.py - Real-time delivery scanner with enhanced detection
import os
import json
import shutil
import re
import time
import signal
import sys
from pathlib import Path
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# DATA FOLDER SETUP
# ═══════════════════════════════════════════════════════════════

# Define paths - monitor worker data, save to main data folder
SOURCE_DIR = Path(r"C:\Point Detection\worker\data\receipt_checked")  # Monitor here
DELIVERY_DIR = Path(__file__).parent / "data" / "delivery_found"
NON_DELIVERY_DIR = Path(__file__).parent / "data" / "non_delivery"

def setup_delivery_folders():
    """Create delivery sQDcanning folders"""
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
    NON_DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Delivery scanning folders ready:")
    print(f"   📂 Source: {SOURCE_DIR}")
    print(f"   🚚 Delivery: {DELIVERY_DIR}")
    print(f"   📋 Non-delivery: {NON_DELIVERY_DIR}")

# Setup folders on import
setup_delivery_folders()

# ═══════════════════════════════════════════════════════════════
# FILE READING AND DETECTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def read_file(filepath):
    """Read file and extract text content."""
    try:
        # Read JSON files
        if filepath.suffix.lower() == '.json':
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict) and 'data' in data:
                    return data['data']
                return str(data)
        
        # Read text files with UTF-8
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
            
    except Exception as e:
        print(f"    ❌ Error reading file {filepath.name}: {e}")
        return ""

def detect_spaced_keyword(text, keyword):
    """Detect keyword with various separators, handling many OCR cases"""
    # Convert keyword to individual characters
    chars = list(keyword)
    
    # Enhanced separator patterns for OCR variations
    # Common OCR misreads: spaces, underscores, dashes, dots, commas, pipes, etc.
    separators = r'[\s_\-\.\,\|\:\;\!\?\*\+\=\(\)\[\]\{\}\<\>\~\`\^\&\%\$\#\@]{0,5}'  # Max 5 separator characters
    
    # Build pattern: char + separator + char + separator + ...
    pattern = separators.join(chars)
    
    # Multiple search strategies to catch different cases
    search_patterns = [
        # Strategy 1: With word boundaries (strict)
        f'(?:^|[\\s\\n\\r]){pattern}(?=[\\s\\n\\r]|$)',
        
        # Strategy 2: Looser boundaries (for embedded text)
        f'(?<![\\u4e00-\\u9fff\\w]){pattern}(?![\\u4e00-\\u9fff\\w])',
        
        # Strategy 3: Very loose (just the pattern)
        pattern
    ]
    
    for search_pattern in search_patterns:
        try:
            # Search for pattern (case insensitive)
            matches = re.findall(search_pattern, text, re.IGNORECASE | re.UNICODE)
            
            if matches:
                # Validate each match to avoid false positives
                for match in matches:
                    if validate_match(match, keyword):
                        return True, match
                        
        except re.error:
            # If regex fails, try next pattern
            continue
    
    return False, None

def validate_match(match, keyword):
    """Validate that the match is reasonable and not OCR noise"""
    if not match:
        return False
    
    # Clean the match for analysis
    match_clean = match.strip()
    
    # Basic length check - shouldn't be too long
    if len(match_clean) > len(keyword) * 6:  # Max 6x original length
        return False
    
    # Count different types of characters
    keyword_chars = set(keyword)
    separator_chars = set(' _-.,:;!?*+=()[]{}.<>~`^&%$#@|\\/')
    
    match_keyword_chars = [c for c in match_clean if c in keyword_chars]
    match_separator_chars = [c for c in match_clean if c in separator_chars]
    match_other_chars = [c for c in match_clean if c not in keyword_chars and c not in separator_chars]
    
    # Should contain all keyword characters
    if len(set(match_keyword_chars)) < len(keyword_chars):
        return False
    
    # Shouldn't have too many random characters
    if len(match_other_chars) > len(keyword) // 2:  # Max half as many random chars as keyword length
        return False
    
    # Separator to keyword ratio check
    if len(match_separator_chars) > len(keyword) * 3:  # Max 3x separators
        return False
    
    # Character order check - keyword characters should appear in correct order
    keyword_positions = []
    for kw_char in keyword:
        pos = match_clean.find(kw_char)
        if pos >= 0:
            keyword_positions.append(pos)
            # Remove found character to handle duplicates
            match_clean = match_clean[:pos] + ' ' + match_clean[pos+1:]
        else:
            return False
    
    # Positions should be in ascending order (characters appear in sequence)
    if keyword_positions != sorted(keyword_positions):
        return False
    
    return True

def check_delivery_keywords(text):
    """Check for delivery keywords with flexible spacing/separation detection"""
    if not text:
        return False, []
    
    # Only 4 keywords to check
    keywords = ['美团', '京东', '饿了么', '配送']
    found_keywords = []
    
    for keyword in keywords:
        # Method 1: Direct match (fastest)
        if keyword in text:
            found_keywords.append(f"{keyword} (direct)")
            continue
        
        # Method 2: Flexible spacing detection
        found, match = detect_spaced_keyword(text, keyword)
        if found:
            found_keywords.append(f"{keyword} (spaced: '{match.strip()}')")
    
    return len(found_keywords) > 0, found_keywords

# ═══════════════════════════════════════════════════════════════
# REAL-TIME DELIVERY SCANNER
# ═══════════════════════════════════════════════════════════════

class RealtimeDeliveryScanner:
    def __init__(self):
        self.processed_files = set()  # Track files we've processed (session only)
        self.scanning_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "delivery_found": 0,
            "non_delivery": 0,
            "processing_errors": 0
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
    
    def process_single_file(self, file_path):
        """Process a single file for delivery keywords"""
        try:
            print(f"📄 Processing: {file_path.name}")
            
            # Read file content
            text_content = read_file(file_path)
            
            if not text_content.strip():
                print(f"    📭 Empty file - moving to non-delivery")
                # Move to non-delivery folder
                target_file = NON_DELIVERY_DIR / file_path.name
                shutil.move(str(file_path), str(target_file))
                return True, "non_delivery", []
            
            print(f"    📖 Read {len(text_content)} characters")
            
            # Check for delivery keywords
            has_keywords, keyword_details = check_delivery_keywords(text_content)
            
            if has_keywords:
                # Move to delivery folder
                target_file = DELIVERY_DIR / file_path.name
                
                try:
                    shutil.move(str(file_path), str(target_file))
                    print(f"    🚚 → delivery_found")
                    print(f"    🔍 Keywords: {', '.join(keyword_details)}")
                    
                    # Log delivery detection
                    self.log_delivery_detection(file_path.name, keyword_details, text_content[:200])
                    
                    return True, "delivery", keyword_details
                    
                except Exception as e:
                    print(f"    ❌ Error moving to delivery folder: {e}")
                    return False, "error", []
            else:
                # Move to non-delivery folder
                target_file = NON_DELIVERY_DIR / file_path.name
                
                try:
                    shutil.move(str(file_path), str(target_file))
                    print(f"    📋 → non_delivery")
                    return True, "non_delivery", []
                    
                except Exception as e:
                    print(f"    ❌ Error moving to non-delivery folder: {e}")
                    return False, "error", []
                    
        except Exception as e:
            print(f"    ❌ Error processing {file_path.name}: {e}")
            return False, "error", []
    
    def log_delivery_detection(self, filename, keywords, text_preview):
        """Log delivery detection details"""
        log_file = DELIVERY_DIR / "delivery_detection_log.txt"
        
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*50}\n")
                f.write(f"Detection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"File: {filename}\n")
                f.write(f"Keywords Found: {', '.join(keywords)}\n")
                f.write(f"Text Preview: {text_preview}...\n")
                f.write(f"{'='*50}\n")
                
        except Exception as e:
            print(f"    ⚠️ Could not write to log file: {e}")
    
    def process_new_files(self, files):
        """Process batch of new files"""
        if not files:
            return
        
        print(f"\n🎯 Processing {len(files)} new files for delivery detection...")
        
        delivery_count = 0
        non_delivery_count = 0
        error_count = 0
        
        for i, file_path in enumerate(files, 1):
            percentage = (i / len(files)) * 100
            print(f"\n📄 [{i}/{len(files)}] ({percentage:.1f}%) {file_path.name}")
            
            success, result_type, keywords = self.process_single_file(file_path)
            
            if success:
                if result_type == "delivery":
                    delivery_count += 1
                    self.scanning_stats["delivery_found"] += 1
                elif result_type == "non_delivery":
                    non_delivery_count += 1
                    self.scanning_stats["non_delivery"] += 1
            else:
                error_count += 1
                self.scanning_stats["processing_errors"] += 1
            
            self.scanning_stats["total_processed"] += 1
        
        print(f"\n📊 Batch Processing Summary:")
        print(f"    🚚 Delivery receipts: {delivery_count}")
        print(f"    📋 Non-delivery: {non_delivery_count}")
        print(f"    ❌ Errors: {error_count}")
    
    def run_realtime_monitor(self, check_interval=30):
        """Main real-time monitoring loop"""
        print(f"\n🚀 Starting Real-time Delivery Receipt Scanner")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📂 Monitoring: {SOURCE_DIR}")
        print(f"🚚 Delivery receipts → {DELIVERY_DIR}")
        print(f"📋 Non-delivery → {NON_DELIVERY_DIR}")
        print(f"🔍 Keywords: 美团, 京东, 饿了么, 配送")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping delivery scanner...")
            print(f"📊 Final stats: {self.scanning_stats['total_processed']} files processed")
            print(f"🚚 Delivery found: {self.scanning_stats['delivery_found']}")
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
                    print("📭 No new files to scan")
                
                print(f"\n📊 Session Stats:")
                print(f"    🕐 Running since: {self.scanning_stats['start_time']}")
                print(f"    🔄 Total processed: {self.scanning_stats['total_processed']}")
                print(f"    🚚 Delivery found: {self.scanning_stats['delivery_found']}")
                print(f"    📋 Non-delivery: {self.scanning_stats['non_delivery']}")
                print(f"    ❌ Errors: {self.scanning_stats['processing_errors']}")
                print(f"    📚 Files tracked: {len(self.processed_files)}")
                
                # Show delivery detection rate
                if self.scanning_stats["total_processed"] > 0:
                    delivery_rate = (self.scanning_stats["delivery_found"] / 
                                   self.scanning_stats["total_processed"]) * 100
                    print(f"    📈 Delivery rate: {delivery_rate:.1f}%")
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

# ═══════════════════════════════════════════════════════════════
# MANUAL SCANNING FUNCTION
# ═══════════════════════════════════════════════════════════════

def scan_existing_files():
    """Manually scan all existing files in source directory"""
    print(f"\n🔄 Scanning all existing files in {SOURCE_DIR}")
    
    if not SOURCE_DIR.exists():
        print(f"❌ Source directory {SOURCE_DIR} does not exist!")
        return
    
    scanner = RealtimeDeliveryScanner()
    
    # Get all files
    files = [f for f in SOURCE_DIR.iterdir() if f.is_file()]
    
    if not files:
        print("📭 No files found to scan")
        return
    
    print(f"📂 Found {len(files)} files to scan")
    print("-" * 60)
    
    # Process all files
    scanner.process_new_files(files)
    
    print(f"\n✅ All files scanned!")
    print(f"📊 Results:")
    print(f"    🚚 Delivery receipts: {scanner.scanning_stats['delivery_found']}")
    print(f"    📋 Non-delivery: {scanner.scanning_stats['non_delivery']}")
    print(f"    ❌ Errors: {scanner.scanning_stats['processing_errors']}")
    
    # Show log file location
    log_file = DELIVERY_DIR / "delivery_detection_log.txt"
    if log_file.exists():
        print(f"\n📝 Delivery detection log: {log_file}")

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time Delivery Receipt Scanner")
    parser.add_argument("--interval", type=int, default=30,
                       help="Check interval in seconds (default: 30)")
    parser.add_argument("--scan-existing", action="store_true",
                       help="Scan all existing files once and exit")
    
    args = parser.parse_args()
    
    if args.scan_existing:
        # Scan all existing files
        scan_existing_files()
    else:
        # Start real-time monitoring
        scanner = RealtimeDeliveryScanner()
        scanner.run_realtime_monitor(args.interval)

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

"""
REAL-TIME DELIVERY RECEIPT SCANNER

USAGE:

1. Start real-time monitoring (default 30-second intervals):
   python realtime_delivery_scanner.py

2. Custom monitoring interval:
   python realtime_delivery_scanner.py --interval 45

3. Scan all existing files once:
   python realtime_delivery_scanner.py --scan-existing

RUNNING THE COMPLETE PIPELINE:

Terminal 1: python realtime_detector.py
Terminal 2: python ocr_processor.py  
Terminal 3: python realtime_classifier.py
Terminal 4: python realtime_downloader.py
Terminal 5: python realtime_ocr.py
Terminal 6: python realtime_delivery_scanner.py

COMPLETE WORKFLOW:
1. realtime_detector.py → Detects new receipts → JSON
2. ocr_processor.py → Downloads files → data/receipt_files/
3. realtime_classifier.py → Classifies files → data/receipt_ocring/
4. realtime_downloader.py → Downloads images → data/downloaded_receipts/
5. realtime_ocr.py → OCR processing → worker/data/receipt_checked/
6. realtime_delivery_scanner.py → Delivery detection → data/delivery_found/

FOLDER STRUCTURE:
C:\Point Detection\
├── worker\
│   └── data\
│       └── receipt_checked\        ← Monitors this folder
└── data\
    ├── delivery_found\             ← Delivery receipts
    │   ├── delivery_detection_log.txt
    │   ├── receipt1.json
    │   └── receipt2.json
    └── non_delivery\               ← Non-delivery receipts
        ├── receipt3.json
        └── receipt4.json

KEYWORD DETECTION:
✅ 美团, 京东, 饿了么, 配送
✅ Direct matches: '美团', '京东'
✅ Spaced matches: '美 团', '饿 了 么', '配.送'
✅ OCR variations: '美_团', '京|东', '配*送'
✅ Enhanced separator support: spaces, dots, underscores, pipes, etc.

FEATURES:
✅ Real-time monitoring every 30 seconds
✅ Enhanced OCR variation detection
✅ Automatic file organization
✅ Detailed logging of delivery detections
✅ Session-only tracking
✅ Performance statistics
✅ Graceful shutdown with Ctrl+C
"""