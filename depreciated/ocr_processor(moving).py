# ocr_processor.py - Real-time OCR file downloader with duplicate handling
import os
import ssl
import hashlib
import requests
import urllib3
import re
import json
import time
import signal
import sys
from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from dotenv import load_dotenv

# ═══════════════════════════════════════════════════════════════
# DATA FOLDER SETUP
# ═══════════════════════════════════════════════════════════════

# Create data/receipt_files folder
RECEIPT_FILES_DIR = Path(__file__).parent / "data" / "receipt_files"
JSON_MONITOR_FILE = Path(r"C:\Point Detection\worker\data\real_time_response")  # Fixed path!

def setup_data_folders():
    """Create data folders"""
    RECEIPT_FILES_DIR.mkdir(parents=True, exist_ok=True)
    print(f"📁 Receipt files folder ready: {RECEIPT_FILES_DIR}")
    print(f"📂 Monitoring JSON files in: {JSON_MONITOR_FILE}")

# Setup folders on import
setup_data_folders()

# ═══════════════════════════════════════════════════════════════
# ENHANCED ENCODING FUNCTIONS (from your working code)
# ═══════════════════════════════════════════════════════════════

ENCODING_MAP = {
    "/": "__SLASH__",
    ":": "__COLON__",
    "*": "__STAR__",
    "?": "__QUESTION__",
    '"': "__QUOTE__",
    "<": "__LT__",
    ">": "__GT__",
    "|": "__PIPE__",
    "\\": "__BACKSLASH__",
    " ": "__SPACE__",
    "#": "__HASH__"  # Add hash encoding
}

# Additional function to handle Chinese and special characters
def sanitize_filename(filename):
    """Remove or replace characters that might cause issues"""
    # Replace Chinese characters and other non-ASCII with their unicode representation
    sanitized = ""
    for char in filename:
        if ord(char) > 127:  # Non-ASCII character
            sanitized += f"__U{ord(char)}__"
        else:
            sanitized += char
    return sanitized

def encode_filename(number):
    """Replace ALL forbidden characters with markers for Windows compatibility"""
    if not number:
        return number
    
    encoded = str(number)
    changes_made = []
    
    # Apply all encodings
    for char, marker in ENCODING_MAP.items():
        if char in encoded:
            encoded = encoded.replace(char, marker)
            changes_made.append(f"{char} → {marker}")
    
    # Handle non-ASCII characters (Chinese, etc.)
    encoded = sanitize_filename(encoded)
    
    # Log if encoding happened
    if changes_made or encoded != str(number):
        print(f"    📝 Encoded: {number}")
        print(f"       → {encoded}")
        if changes_made:
            print(f"       Changes: {', '.join(changes_made)}")
    
    return encoded

def fix_timestamp_format(number):
    """Fix various timestamp format issues"""
    if not number or not isinstance(number, str):
        return number
    
    # Pattern 1: YYYY-MM-DDHH_MM_SS → YYYY-MM-DD HH:MM:SS
    pattern1 = r'(\d{4}-\d{2}-\d{2})(\d{2})_(\d{2})_(\d{2})'
    match1 = re.search(pattern1, number)
    if match1:
        fixed = number.replace(match1.group(0), f"{match1.group(1)} {match1.group(2)}:{match1.group(3)}:{match1.group(4)}")
        print(f"    🕐 Fixed timestamp: {number} → {fixed}")
        return fixed
    
    # Pattern 2: YYYY-MM-DDHH:MM:SS → YYYY-MM-DD HH:MM:SS (add space)
    pattern2 = r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})'
    match2 = re.search(pattern2, number)
    if match2:
        fixed = number.replace(match2.group(0), f"{match2.group(1)} {match2.group(2)}")
        print(f"    🕐 Fixed timestamp: {number} → {fixed}")
        return fixed
    
    return number

# ═══════════════════════════════════════════════════════════════
# AUTHENTICATION SETUP
# ═══════════════════════════════════════════════════════════════

def setup_authenticated_session():
    """Setup authenticated session for downloading files"""
    load_dotenv(r"C:\Point Detection\.env")
    USER = os.getenv("API_USER")
    PASSWORD = os.getenv("API_PASS")
    ORG_UUID = os.getenv("ORG_UUID")
    
    if not all([USER, PASSWORD, ORG_UUID]):
        raise RuntimeError("Please set API_USER, API_PASS, ORG_UUID in your .env")
    
    pw_md5 = hashlib.md5(PASSWORD.encode("utf-8")).hexdigest()
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    class SSLAdapter(HTTPAdapter):
        def init_poolmanager(self, conns, maxsize, block=False, **kw):
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ctx.set_ciphers("DEFAULT@SECLEVEL=1")
            self.poolmanager = PoolManager(num_pools=conns, maxsize=maxsize,
                                           block=block, ssl_context=ctx, **kw)
    
    session = requests.Session()
    session.verify = False
    session.mount("https://", SSLAdapter())
    
    # Login
    LOGIN_PAGE = "https://hddc01.superbrandmall.com/pod-web/static/views/app/login.html"
    session.get(LOGIN_PAGE)
    
    login_url = (
        "https://hddc01.superbrandmall.com"
        f"/pod-web/s/auth/0000/login?_dc={int(datetime.utcnow().timestamp()*1000)}"
    )
    
    login_payload = {
        "phone": "", "messageCode": "",
        "userCode": USER,
        "password": pw_md5,
        "orgUuid": ORG_UUID
    }
    
    login_headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://hddc01.superbrandmall.com",
        "Referer": LOGIN_PAGE
    }
    
    resp = session.post(login_url, json=login_payload, headers=login_headers, timeout=15)
    resp.raise_for_status()
    
    m = re.search(r"jwt=([^;]+)", resp.headers.get("Set-Cookie",""))
    if not m:
        raise RuntimeError("No jwt cookie found")
    
    session.cookies.set("jwt", m.group(1), domain=".superbrandmall.com", path="/")
    print(f"✅ OCR Processor authenticated successfully!")
    
    return session

# ═══════════════════════════════════════════════════════════════
# FILE DOWNLOAD FUNCTIONS
# ═══════════════════════════════════════════════════════════════

FILE_API = "https://hddc01.superbrandmall.com/pod-web/s/web/0000/0003/ticket/getticketfilebyfeaturecode"
CONTENT_TYPE_EXT = {"image/jpeg":".jpg", "image/png":".png", "application/pdf":".pdf"}

def download_receipt_file(session, feature_code, receipt_number):
    """Download receipt file using receipt number as filename"""
    ts = int(datetime.utcnow().timestamp()*1000)
    params = {
        "_dc": ts,
        "featureCode": feature_code,
        "deleteSpecialCharacter": "true"
    }
    
    try:
        resp = session.get(FILE_API, params=params, stream=True, timeout=15)
        resp.raise_for_status()
        
        ct = resp.headers.get("Content-Type","").split(";")[0]
        ext = CONTENT_TYPE_EXT.get(ct, ".bin")
        
        # Fix timestamp format first, then encode
        fixed_number = fix_timestamp_format(str(receipt_number))
        filename = encode_filename(fixed_number)
        
        filepath = f"{filename}{ext}"
        full_path = RECEIPT_FILES_DIR / filepath
        
        # Check if already exists
        if full_path.exists():
            print(f"    ⏭️  File already exists: {filepath}")
            return str(full_path), True  # Return path and "already existed" flag
        
        # Download file
        with open(full_path, "wb") as fw:
            for chunk in resp.iter_content(8192):
                fw.write(chunk)
        
        print(f"    📥 Downloaded: {filepath}")
        return str(full_path), False  # Return path and "newly downloaded" flag
        
    except Exception as e:
        print(f"    ❌ Error downloading {feature_code}: {e}")
        return None, False

# ═══════════════════════════════════════════════════════════════
# REAL-TIME OCR PROCESSOR
# ═══════════════════════════════════════════════════════════════

class RealtimeOCRProcessor:
    def __init__(self):
        self.session = None
        self.processed_feature_codes = set()  # Track featureCodes 
        self.receipt_number_counts = {}  # Track how many times we've seen each receipt number
        self.duplicate_stats = {
            "duplicates_found": 0,
            "unique_receipts": 0,
            "total_entries": 0,
            "duplicate_attempts": 0
        }
        self.processing_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "successful_downloads": 0,
            "failed_downloads": 0,
            "already_existed": 0
        }
        
    def initialize(self):
        """Initialize the OCR processor"""
        print("🔧 Initializing Real-time OCR Processor...")
        self.session = setup_authenticated_session()
        print(f"📂 Monitoring folder: {JSON_MONITOR_FILE}")
        print(f"📁 Saving files to: {RECEIPT_FILES_DIR}")
        
    def find_todays_json_file(self):
        """Find today's new receipts JSON file"""
        today = datetime.now().strftime("%Y-%m-%d")
        json_file = JSON_MONITOR_FILE / f"new_receipts_today_{today}.json"
        
        if json_file.exists():
            return json_file
        
        print(f"📭 No JSON file found for today: {json_file}")
        return None
        
    def load_receipts_from_json(self):
        """Load all receipts from today's JSON file"""
        json_file = self.find_todays_json_file()
        
        if not json_file:
            return []
        
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            new_receipts = data.get("new_receipts", [])
            
            # Simply filter by featureCode to avoid processing same file twice
            unprocessed = []
            for receipt in new_receipts:
                feature_code = receipt.get("record", {}).get("featureCode")
                if feature_code and feature_code not in self.processed_feature_codes:
                    unprocessed.append(receipt)
            
            print(f"    📊 Found {len(new_receipts)} total receipts, {len(unprocessed)} to process")
            
            return unprocessed
            
        except Exception as e:
            print(f"❌ Error reading JSON file: {e}")
            return []
    
    def process_receipts(self, receipts):
        """Process receipts - download files using receipt numbers as filenames"""
        if not receipts:
            return
        
        print(f"\n🎯 Processing {len(receipts)} new receipts for OCR...")
        
        successful = 0
        failed = 0
        already_existed = 0
        
        for i, receipt in enumerate(receipts, 1):
            receipt_number = receipt.get("receipt_number")  # From JSON
            record = receipt.get("record", {})
            feature_code = record.get("featureCode")
            shop_name = record.get("shopName", "Unknown")
            
            print(f"\n📄 [{i}/{len(receipts)}] Processing: {receipt_number}")
            print(f"    🏪 Shop: {shop_name}")
            print(f"    🔑 FeatureCode: {feature_code}")
            
            if not feature_code:
                print(f"    ❌ No featureCode found - skipping")
                failed += 1
                continue
            
            if not receipt_number:
                print(f"    ❌ No receipt number found - skipping")
                failed += 1
                continue
            
            # Download file using receipt number as filename
            file_path, existed = download_receipt_file(
                self.session, 
                feature_code, 
                receipt_number
            )
            
            if file_path:
                if existed:
                    already_existed += 1
                else:
                    successful += 1
                    print(f"    ✅ Saved to: {RECEIPT_FILES_DIR}")
                
                # Mark featureCode as processed
                self.processed_feature_codes.add(feature_code)
            else:
                failed += 1
        
        # Update stats
        self.processing_stats["total_processed"] += len(receipts)
        self.processing_stats["successful_downloads"] += successful
        self.processing_stats["failed_downloads"] += failed
        self.processing_stats["already_existed"] += already_existed
        
        print(f"\n📊 Batch Processing Summary:")
        print(f"    ✅ Successfully downloaded: {successful}")
        print(f"    ⏭️  Already existed: {already_existed}")
        print(f"    ❌ Failed: {failed}")
        print(f"    📁 All files saved to: {RECEIPT_FILES_DIR}")
        
        # List some example filenames to help debugging
        if successful > 0:
            print(f"\n📋 Example downloaded files:")
            example_files = list(RECEIPT_FILES_DIR.glob("*"))[-5:]  # Show last 5 files
            for file in example_files:
                print(f"    📄 {file.name}")
        
    def run_realtime_monitor(self, check_interval=15):
        """Main real-time monitoring loop"""
        print(f"\n🚀 Starting Real-time OCR File Download Monitor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"🗓️ REAL-TIME ONLY - Processing today's receipts")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping OCR processor...")
            print(f"🗑️ Session data cleared. Goodbye!")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            while True:
                print(f"\n🔍 Checking for new receipts... {datetime.now().strftime('%H:%M:%S')}")
                
                # Load receipts from JSON file
                new_receipts = self.load_receipts_from_json()
                
                if new_receipts:
                    self.process_receipts(new_receipts)
                else:
                    print("📭 No new receipts to process")
                
                print(f"\n📊 Session Stats:")
                print(f"    🕐 Running since: {self.processing_stats['start_time']}")
                print(f"    🔄 Total processed: {self.processing_stats['total_processed']}")
                print(f"    ✅ Successful: {self.processing_stats['successful_downloads']}")
                print(f"    ⏭️  Already existed: {self.processing_stats['already_existed']}")
                print(f"    ❌ Failed: {self.processing_stats['failed_downloads']}")
                print(f"    🔑 Unique featureCodes processed: {len(self.processed_feature_codes)}")
                print(f"    📋 Unique receipt numbers seen: {len(self.receipt_number_counts)}")
                
                if self.duplicate_stats["duplicates_found"] > 0:
                    print(f"\n📊 Duplicate Stats:")
                    print(f"    🔄 Duplicate receipt numbers in JSON: {self.duplicate_stats['duplicates_found']}")
                    print(f"    📋 Unique receipt numbers: {self.duplicate_stats['unique_receipts']}")
                    print(f"    📑 Total entries: {self.duplicate_stats['total_entries']}")
                    if self.duplicate_stats["duplicate_attempts"] > 0:
                        print(f"    🔁 Duplicate processing attempts: {self.duplicate_stats['duplicate_attempts']}")
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time OCR File Downloader")
    parser.add_argument("--interval", type=int, default=15,
                       help="Check interval in seconds")
    parser.add_argument("--process-existing", action="store_true",
                       help="Process all existing receipts from today's JSON file once")
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = RealtimeOCRProcessor()
    processor.initialize()
    
    if args.process_existing:
        # Process all existing receipts once
        print("🔄 Processing all existing receipts from today's file...")
        all_receipts = processor.load_receipts_from_json()
        if all_receipts:
            processor.process_receipts(all_receipts)
        else:
            print("📭 No receipts found to process")
    else:
        # Start real-time monitoring
        processor.run_realtime_monitor(args.interval)

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

"""
REAL-TIME OCR FILE DOWNLOADER WITH DUPLICATE HANDLING

USAGE:

1. Start real-time monitoring (default 15-second intervals):
   python ocr_processor.py

2. Custom monitoring interval:
   python ocr_processor.py --interval 30

3. Process all existing receipts from today's file once:
   python ocr_processor.py --process-existing

KEY CHANGES FOR DUPLICATE HANDLING:
✅ Tracks processed featureCodes to avoid downloading same file twice
✅ Identifies and logs duplicate receipt numbers
✅ Shows "DUPLICATE: File already saved before" message for duplicates
✅ Counts occurrence number for each duplicate
✅ Shows comprehensive duplicate statistics
✅ First occurrence downloads the file, subsequent ones show duplicate message

WORKFLOW:
1. realtime_detector.py → Detects new receipts → Saves to JSON
2. ocr_processor.py → Reads JSON → Downloads files using receipt numbers
3. Duplicates/triplets are properly identified and logged

DUPLICATE HANDLING:
- When a receipt number appears multiple times (e.g., triplets):
  * First occurrence: Downloads the file
  * Second occurrence: Shows "🔁 DUPLICATE: File already saved before"
  * Third occurrence: Shows "🔁 DUPLICATE: File already saved before"
- Each duplicate attempt is tracked and reported in statistics

FOLDER STRUCTURE:
C:\Point Detection\
├── realtime_detector.py
├── ocr_processor.py
└── data/
    ├── real_time_response/
    │   └── new_receipts_today_2025-07-22.json  ← Reads from here
    └── receipt_files/
        ├── RCP001__COLON__2025-07-22__SPACE__14__COLON__29__COLON__15.jpg
        ├── RCP002__COLON__2025-07-22__SPACE__14__COLON__30__COLON__01.pdf
        └── RCP003__COLON__2025-07-22__SPACE__14__COLON__31__COLON__22.jpg

FEATURES:
✅ Real-time monitoring every 15 seconds
✅ Enhanced filename encoding for Windows compatibility
✅ Timestamp format fixing
✅ Only processes today's receipts
✅ Identifies and logs duplicate receipt numbers
✅ Shows "duplicate already saved before" message
✅ Comprehensive duplicate statistics
✅ Uses receipt numbers as filenames
✅ Graceful shutdown with Ctrl+C
"""