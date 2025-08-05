# app/workers/ocr_downloader.py - Main OCR downloader orchestrator (organized from your original)
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# Add directories to Python path  
current_dir = Path(__file__).parent  # app/workers/
app_dir = current_dir.parent         # app/
project_root = app_dir.parent        # C:\Point Detection

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

# Import our organized services
from config.settings import settings
from services.ocr_downloader.api_service import APIService
from services.ocr_downloader.file_service import FileService
from services.ocr_downloader.processing_service import ProcessingService

class RealtimeFileDownloader:
    """Main downloader orchestrator - organized from your original RealtimeFileDownloader class"""
    
    def __init__(self):
        self.api_service = APIService()
        self.file_service = FileService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
    def initialize(self):
        """Initialize the downloader - enhanced with date organization"""
        print("🔧 Initializing Real-time File Downloader...")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"💾 Downloads to: {self.file_service.get_download_directory()}")
        print(f"🗓️ Processing files for: {datetime.now().strftime('%Y-%m-%d')}")
        
    def run_realtime_monitor(self, check_interval: int = 15):
        """Main real-time monitoring loop - exact logic from your original"""
        print(f"\n🚀 Starting Real-time File Download Monitor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"💾 Downloads to: {self.file_service.get_download_directory()}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping file downloader...")
            print(f"🗑️ Session data cleared. Goodbye!")
            self.is_running = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        self.is_running = True
        
        try:
            while self.is_running:
                print(f"\n🔍 Scanning for new files... {datetime.now().strftime('%H:%M:%S')}")
                
                # Scan for new files
                processed_files = self.processing_service.get_processed_files()
                new_files = self.file_service.scan_for_new_files(processed_files)
                
                if new_files:
                    self.processing_service.process_new_files(new_files, self.file_service, self.api_service)
                else:
                    print("📭 No new files to download")
                
                # Print session stats - exact format from your original
                self.processing_service.print_session_stats()
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def download_all_existing(self):
        """Download all existing files once - from your original --download-existing logic"""
        return self.processing_service.download_existing_files(self.file_service, self.api_service)
    
    def get_download_summary(self):
        """Get today's download summary"""
        summary = self.file_service.get_download_summary()
        
        print(f"\n📅 TODAY'S DOWNLOAD SUMMARY ({summary['date']}):")
        print(f"    💾 Downloaded files: {summary['downloaded_files']}")
        print(f"    📁 Download folder: {summary['download_dir']}")
        print(f"    📂 Source folder: {summary['source_dir']}")
        
        return summary

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION - exactly like your original
# ═══════════════════════════════════════════════════════════════

def main():
    """Main execution function - matches your original __main__ block"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time File Downloader for receipt_ocring with Date Organization")
    parser.add_argument("--interval", type=int, default=15,
                       help="Check interval in seconds")
    parser.add_argument("--download-existing", action="store_true",
                       help="Download all existing files once and exit")
    parser.add_argument("--summary", action="store_true",
                       help="Show today's download summary and exit")
    
    args = parser.parse_args()
    
    # Initialize downloader
    downloader = RealtimeFileDownloader()
    downloader.initialize()
    
    if args.summary:
        downloader.get_download_summary()
        sys.exit(0)
    elif args.download_existing:
        downloader.download_all_existing()
    else:
        # Start real-time monitoring
        downloader.run_realtime_monitor(args.interval)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES - exactly like your original
# ═══════════════════════════════════════════════════════════════

"""
ORGANIZED REAL-TIME FILE DOWNLOADER WITH DATE ORGANIZATION

USAGE:

1. Start real-time monitoring (default 15-second intervals):
   python app/workers/ocr_downloader.py

2. Custom monitoring interval:
   python app/workers/ocr_downloader.py --interval 20

3. Download all existing files once:
   python app/workers/ocr_downloader.py --download-existing

4. Show today's download summary:
   python app/workers/ocr_downloader.py --summary

RUNNING THE COMPLETE PIPELINE:

Terminal 1:
python app/workers/realtime_detector.py

Terminal 2:  
python app/workers/ocr_processor.py

Terminal 3:
python app/workers/ocr_classification.py

Terminal 4:
python app/workers/ocr_downloader.py

COMPLETE WORKFLOW:
1. realtime_detector.py → Detects new receipts → JSON file
2. ocr_processor.py → Downloads receipt files → worker/data/receipt_files/2025-07-23/
3. ocr_classification.py → Classifies files → worker/data/receipt_ocring/2025-07-23/
4. ocr_downloader.py → Downloads actual images → worker/data/downloaded_receipts/2025-07-23/

ENHANCED FOLDER STRUCTURE:
C:\\Point Detection\\worker\\data\\
├── real_time_response/
│   └── receipts_2025-07-23.json
├── receipt_files/
│   └── 2025-07-23/              ← OCR processor saves here
│       ├── receipt1.jpg
│       └── receipt2.pdf
├── receipt_ocring/
│   └── 2025-07-23/              ← Classification results (files with URLs)
│       ├── file1.bin
│       └── file2.bin
├── receipt_checked/
│   └── 2025-07-23/              ← Classification results (files without URLs)
│       └── file3.bin
└── downloaded_receipts/
    └── 2025-07-23/              ← Final downloaded images/PDFs ← NEW
        ├── receipt1.jpg
        ├── receipt2.pdf
        └── receipt3.png

FEATURES:
✅ Same functionality as original - just organized by date
✅ Date-based folder organization for easy daily processing
✅ Real-time monitoring every 15 seconds
✅ Automatic file type detection from content headers
✅ Session-only tracking (no history files)
✅ Smart filename handling using original receipt numbers
✅ Handles various image formats and PDFs
✅ Error handling and retry logic
✅ Graceful shutdown with Ctrl+C
✅ Manual download option for existing files
✅ Summary command to check today's results
"""