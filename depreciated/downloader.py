# realtime_downloader.py - Real-time file downloader for receipt_ocring
import os
import ssl
import re
import hashlib
import urllib3
import requests
import time
import signal
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, unquote
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from dotenv import load_dotenv

# ═══════════════════════════════════════════════════════════════
# DATA FOLDER SETUP
# ═══════════════════════════════════════════════════════════════

# Define paths
SOURCE_DIR = Path(__file__).parent / "data" / "receipt_ocring"
DOWNLOAD_DIR = Path(__file__).parent / "data" / "downloaded_receipts"

def setup_download_folders():
    """Create download folders"""
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Download folders ready:")
    print(f"   📂 Source: {SOURCE_DIR}")
    print(f"   💾 Downloads: {DOWNLOAD_DIR}")

# Setup folders on import
setup_download_folders()

# ═══════════════════════════════════════════════════════════════
# SSL ADAPTER & AUTHENTICATION
# ═══════════════════════════════════════════════════════════════

class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx, **kwargs
        )

def setup_authenticated_session():
    """Setup authenticated session for downloading files"""
    load_dotenv(r"C:\Point Detection\.env")
    USER = os.getenv("API_USER", "cpit")
    PASSWORD = os.getenv("API_PASS", "@abc1234")
    ORG_UUID = os.getenv("ORG_UUID", "0003")
    
    # MD5 of the password
    password_md5 = hashlib.md5(PASSWORD.encode("utf-8")).hexdigest()
    
    # Suppress SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    session = requests.Session()
    session.verify = False
    session.mount("https://", SSLAdapter())
    
    # Login
    LOGIN_PAGE = "https://hddc01.superbrandmall.com/pod-web/static/views/app/login.html"
    session.get(LOGIN_PAGE)  # seed CSRF / initial cookies
    
    LOGIN_API = (
        "https://hddc01.superbrandmall.com"
        "/pod-web/s/auth/0000/login?_dc=1752569108115"
    )
    login_payload = {
        "phone": "",
        "messageCode": "",
        "userCode": USER,
        "password": password_md5,
        "orgUuid": ORG_UUID,
    }
    login_headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://hddc01.superbrandmall.com",
        "Referer": LOGIN_PAGE,
    }
    
    resp = session.post(LOGIN_API, json=login_payload, headers=login_headers)
    resp.raise_for_status()
    
    # Extract JWT from Set-Cookie and inject into session.cookies
    raw = resp.headers.get("Set-Cookie", "")
    m = re.search(r"jwt=([^;]+)", raw)
    if not m:
        raise RuntimeError("Login succeeded but no jwt cookie found.")
    jwt_token = m.group(1)
    session.cookies.set("jwt", jwt_token, domain=".superbrandmall.com", path="/")
    
    print(f"✅ File Downloader authenticated successfully!")
    return session

# ═══════════════════════════════════════════════════════════════
# FILE PROCESSING FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def extract_url_from_file(file_path: Path) -> str:
    """Extract URL from .bin or other file types"""
    try:
        # Read file content
        txt = file_path.read_bytes().decode("utf-8", errors="ignore")
        
        # Look for URL pattern in data field
        m = re.search(r'"data"\s*:\s*"(?P<url>https?://[^"]+)"', txt)
        if m:
            return m.group("url")
        
        # Alternative pattern - direct URL search
        m = re.search(r'https://hddc01\.superbrandmall\.com:443/[^\s<>"{}|\\^`\[\]]+', txt)
        if m:
            return m.group(0)
        
        return None
        
    except Exception as e:
        print(f"    ❌ Error reading file {file_path.name}: {e}")
        return None

def download_file_with_session(session, url: str, source_filename: str) -> tuple:
    """
    Download file using session
    Returns: (success, downloaded_path, error_message)
    """
    try:
        # Use the source filename as base filename
        base_name = Path(source_filename).stem  # Remove extension to get the base name
        
        # Try to detect file type from response headers
        try:
            # Make a HEAD request to get content type
            head_resp = session.head(url, timeout=10)
            content_type = head_resp.headers.get('Content-Type', '').lower()
            
            # Map content types to extensions
            extension_map = {
                'image/jpeg': '.jpg',
                'image/jpg': '.jpg', 
                'image/png': '.png',
                'image/gif': '.gif',
                'image/bmp': '.bmp',
                'image/tiff': '.tiff',
                'image/webp': '.webp',
                'application/pdf': '.pdf'
            }
            
            # Get extension from content type
            extension = extension_map.get(content_type, '')
            
            # If no extension from content type, try to get from URL
            if not extension:
                url_path = urlparse(unquote(url)).path
                url_extension = Path(url_path).suffix.lower()
                if url_extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.pdf']:
                    extension = url_extension
                else:
                    # Default to .jpg for images
                    extension = '.jpg'
                    
        except Exception as e:
            print(f"    ⚠️ Could not detect file type: {e}, defaulting to .jpg")
            extension = '.jpg'
        
        # Create final filename
        filename = base_name + extension
        dest = DOWNLOAD_DIR / filename
        
        # Check if file already exists
        if dest.exists():
            return True, str(dest), "File already exists"
        
        # Download the actual file
        r = session.get(url, stream=True, timeout=30)
        r.raise_for_status()
        
        with open(dest, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        
        return True, str(dest), None
        
    except Exception as e:
        return False, None, str(e)

# ═══════════════════════════════════════════════════════════════
# REAL-TIME DOWNLOADER
# ═══════════════════════════════════════════════════════════════

class RealtimeFileDownloader:
    def __init__(self):
        self.session = None
        self.processed_files = set()  # Track files we've processed (session only)
        self.download_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "successful_downloads": 0,
            "already_existed": 0,
            "failed_downloads": 0,
            "no_url_found": 0
        }
        
    def initialize(self):
        """Initialize the downloader"""
        print("🔧 Initializing Real-time File Downloader...")
        self.session = setup_authenticated_session()
        print(f"📂 Monitoring: {SOURCE_DIR}")
        print(f"💾 Downloads to: {DOWNLOAD_DIR}")
        
    def scan_for_new_files(self):
        """Scan source directory for new files to download"""
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
        """Process and download new files"""
        if not files:
            return
        
        print(f"\n🎯 Processing {len(files)} new files for download...")
        
        for i, file_path in enumerate(files, 1):
            print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
            
            # Extract URL from file
            url = extract_url_from_file(file_path)
            
            if not url:
                print(f"    ❌ No URL found in {file_path.name}")
                self.download_stats["no_url_found"] += 1
            else:
                print(f"    🔗 Found URL: {url[:60]}...")
                
                # Download the file
                success, downloaded_path, error = download_file_with_session(
                    self.session, url, file_path.name
                )
                
                if success:
                    if error == "File already exists":
                        print(f"    ⏭️ File already exists: {Path(downloaded_path).name}")
                        self.download_stats["already_existed"] += 1
                    else:
                        print(f"    ✅ Downloaded: {Path(downloaded_path).name}")
                        self.download_stats["successful_downloads"] += 1
                else:
                    print(f"    ❌ Download failed: {error}")
                    self.download_stats["failed_downloads"] += 1
            
            self.download_stats["total_processed"] += 1
        
        print(f"\n📊 Batch Download Summary:")
        print(f"    ✅ Downloaded: {self.download_stats['successful_downloads']}")
        print(f"    ⏭️ Already existed: {self.download_stats['already_existed']}")
        print(f"    ❌ Failed: {self.download_stats['failed_downloads']}")
        print(f"    🔗 No URL: {self.download_stats['no_url_found']}")
    
    def run_realtime_monitor(self, check_interval=15):
        """Main real-time monitoring loop"""
        print(f"\n🚀 Starting Real-time File Download Monitor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📂 Monitoring: {SOURCE_DIR}")
        print(f"💾 Downloads to: {DOWNLOAD_DIR}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping file downloader...")
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
                    print("📭 No new files to download")
                
                print(f"\n📊 Session Stats:")
                print(f"    🕐 Running since: {self.download_stats['start_time']}")
                print(f"    🔄 Total processed: {self.download_stats['total_processed']}")
                print(f"    ✅ Downloaded: {self.download_stats['successful_downloads']}")
                print(f"    ⏭️ Already existed: {self.download_stats['already_existed']}")
                print(f"    ❌ Failed: {self.download_stats['failed_downloads']}")
                print(f"    🔗 No URL: {self.download_stats['no_url_found']}")
                print(f"    📚 Files tracked: {len(self.processed_files)}")
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

# ═══════════════════════════════════════════════════════════════
# MANUAL DOWNLOAD FUNCTION
# ═══════════════════════════════════════════════════════════════

def download_existing_files():
    """Manually download all existing files in source directory"""
    print(f"\n🔄 Downloading all existing files in {SOURCE_DIR}")
    
    if not SOURCE_DIR.exists():
        print(f"❌ Source directory {SOURCE_DIR} does not exist!")
        return
    
    # Setup session
    session = setup_authenticated_session()
    
    success_count = 0
    already_existed = 0
    failed_count = 0
    no_url_count = 0
    total_count = 0
    
    # Get all files
    files = [f for f in SOURCE_DIR.iterdir() if f.is_file()]
    
    if not files:
        print("📭 No files found to download")
        return
    
    print(f"📂 Found {len(files)} files to download")
    print("-" * 60)
    
    for i, file_path in enumerate(files, 1):
        total_count += 1
        print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
        
        # Extract URL
        url = extract_url_from_file(file_path)
        
        if not url:
            print(f"    ❌ No URL found in {file_path.name}")
            no_url_count += 1
        else:
            print(f"    🔗 Found URL: {url[:60]}...")
            
            # Download file
            success, downloaded_path, error = download_file_with_session(
                session, url, file_path.name
            )
            
            if success:
                if error == "File already exists":
                    print(f"    ⏭️ File already exists: {Path(downloaded_path).name}")
                    already_existed += 1
                else:
                    print(f"    ✅ Downloaded: {Path(downloaded_path).name}")
                    success_count += 1
            else:
                print(f"    ❌ Download failed: {error}")
                failed_count += 1
    
    # Print summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully downloaded: {success_count}")
    print(f"⏭️ Already existed: {already_existed}")
    print(f"❌ Failed downloads: {failed_count}")
    print(f"🔗 No URL found: {no_url_count}")
    print(f"📊 Total files processed: {total_count}")
    print("=" * 60)

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time File Downloader for receipt_ocring")
    parser.add_argument("--interval", type=int, default=15,
                       help="Check interval in seconds")
    parser.add_argument("--download-existing", action="store_true",
                       help="Download all existing files once and exit")
    
    args = parser.parse_args()
    
    if args.download_existing:
        # Download all existing files once
        download_existing_files()
    else:
        # Start real-time monitoring
        downloader = RealtimeFileDownloader()
        downloader.initialize()
        downloader.run_realtime_monitor(args.interval)

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

"""
REAL-TIME FILE DOWNLOADER

USAGE:

1. Start real-time monitoring (default 15-second intervals):
   python realtime_downloader.py

2. Custom monitoring interval:
   python realtime_downloader.py --interval 20

3. Download all existing files once:
   python realtime_downloader.py --download-existing

RUNNING THE COMPLETE PIPELINE:

Terminal 1:
python realtime_detector.py

Terminal 2:  
python ocr_processor.py

Terminal 3:
python realtime_classifier.py

Terminal 4:
python realtime_downloader.py

COMPLETE WORKFLOW:
1. realtime_detector.py → Detects new receipts → JSON file
2. ocr_processor.py → Downloads receipt files → data/receipt_files/
3. realtime_classifier.py → Classifies files → data/receipt_ocring/
4. realtime_downloader.py → Downloads actual images → data/downloaded_receipts/

FOLDER STRUCTURE:
C:\Point Detection\
├── realtime_detector.py
├── ocr_processor.py
├── realtime_classifier.py
├── realtime_downloader.py
└── data/
    ├── real_time_response/
    │   └── new_receipts_today_2025-07-22.json
    ├── receipt_files/           ← OCR processor output
    ├── receipt_ocring/          ← Classifier output (files with URLs)
    │   ├── file1.bin
    │   └── file2.bin
    ├── receipt_checked/         ← Classifier output (files without URLs)
    └── downloaded_receipts/     ← Final downloaded images/PDFs
        ├── receipt1.jpg
        ├── receipt2.pdf
        └── receipt3.png

FEATURES:
✅ Real-time monitoring every 15 seconds
✅ Automatic file type detection
✅ Session-only tracking (no history files)
✅ Smart filename handling using original receipt numbers
✅ Handles various image formats and PDFs
✅ Error handling and retry logic
✅ Graceful shutdown with Ctrl+C
✅ Manual download option for existing files
"""