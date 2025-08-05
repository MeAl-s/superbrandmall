# app/workers/ocr_classification.py - Main OCR classification orchestrator (organized from your original)
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
from services.ocr_classification.file_service import FileService
from services.ocr_classification.processing_service import ProcessingService

class RealtimeFileClassifier:
    """Main classification orchestrator - organized from your original RealtimeFileClassifier class"""
    
    def __init__(self):
        self.file_service = FileService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
    def initialize(self):
        """Initialize the file classifier"""
        print("🔧 Initializing Real-time File Classifier...")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"✅ Files with URLs → {self.file_service.today_url_dir}")
        print(f"📋 Files without URLs → {self.file_service.today_no_url_dir}")
        print(f"🗓️ Processing files for: {datetime.now().strftime('%Y-%m-%d')}")
        
    def run_realtime_monitor(self, check_interval: int = 10):
        """Main real-time monitoring loop - exact logic from your original"""
        print(f"\n🚀 Starting Real-time File Classification Monitor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"✅ Files with URLs → {self.file_service.today_url_dir}")
        print(f"📋 Files without URLs → {self.file_service.today_no_url_dir}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping file classifier...")
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
                    self.processing_service.process_new_files(new_files, self.file_service)
                else:
                    print("📭 No new files to classify")
                
                # Print session stats - exact format from your original
                self.processing_service.print_session_stats()
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def classify_all_existing(self):
        """Classify all existing files once - from your original --classify-existing logic"""
        return self.processing_service.classify_existing_files(self.file_service)
    
    def get_classification_summary(self):
        """Get today's classification summary"""
        summary = self.file_service.get_classification_summary()
        
        print(f"\n📅 TODAY'S CLASSIFICATION SUMMARY ({summary['date']}):")
        print(f"    ✅ Files with URLs: {summary['files_with_urls']}")
        print(f"    📋 Files without URLs: {summary['files_without_urls']}")
        print(f"    📊 Total classified: {summary['total_classified']}")
        print(f"    📁 URL folder: {summary['url_dir']}")
        print(f"    📁 No-URL folder: {summary['no_url_dir']}")
        
        return summary

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION - exactly like your original
# ═══════════════════════════════════════════════════════════════

def main():
    """Main execution function - matches your original __main__ block"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time File Classifier for hddc01 URLs with Date Organization")
    parser.add_argument("--interval", type=int, default=10,
                       help="Check interval in seconds")
    parser.add_argument("--classify-existing", action="store_true",
                       help="Classify all existing files once and exit")
    parser.add_argument("--summary", action="store_true",
                       help="Show today's classification summary and exit")
    
    args = parser.parse_args()
    
    # Initialize classifier
    classifier = RealtimeFileClassifier()
    classifier.initialize()
    
    if args.summary:
        classifier.get_classification_summary()
        sys.exit(0)
    elif args.classify_existing:
        classifier.classify_all_existing()
    else:
        # Start real-time monitoring
        classifier.run_realtime_monitor(args.interval)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES - exactly like your original
# ═══════════════════════════════════════════════════════════════

"""
ORGANIZED REAL-TIME FILE CLASSIFIER WITH DATE ORGANIZATION

USAGE:

1. Start real-time monitoring (default 10-second intervals):
   python app/workers/ocr_classification.py

2. Custom monitoring interval:
   python app/workers/ocr_classification.py --interval 20

3. Classify all existing files once:
   python app/workers/ocr_classification.py --classify-existing

4. Show today's classification summary:
   python app/workers/ocr_classification.py --summary

RUNNING THE FULL PIPELINE:

Terminal 1:
python app/workers/realtime_detector.py

Terminal 2:  
python app/workers/ocr_processor.py

Terminal 3:
python app/workers/ocr_classification.py

WORKFLOW:
1. realtime_detector.py → Detects new receipts → Saves to JSON
2. ocr_processor.py → Downloads receipt files → Saves to worker/data/receipt_files/2025-07-23/
3. ocr_classification.py → Monitors worker/data/receipt_files/2025-07-23/ → Classifies files by date

ENHANCED FOLDER STRUCTURE:
C:\Point Detection\worker\data\
├── real_time_response/
│   └── receipts_2025-07-23.json
├── receipt_files/
│   └── 2025-07-23/              ← OCR processor saves here
│       ├── receipt1.jpg
│       ├── receipt2.pdf
│       └── receipt3.json
└── ../data/                     ← Classification results (outside worker/)
    ├── receipt_ocring/
    │   └── 2025-07-23/          ← Files WITH hddc01 URLs
    │       ├── receipt1.json
    │       └── receipt2.json
    └── receipt_checked/
        └── 2025-07-23/          ← Files WITHOUT hddc01 URLs
            ├── receipt3.json
            └── receipt4.json

FEATURES:
✅ Same functionality as original - just organized
✅ Date-based folder organization
✅ Enhanced classification with today's date tracking
✅ Real-time monitoring every 10 seconds
✅ Automatic classification based on hddc01 URLs
✅ Session-only tracking (no history files)
✅ Handles both JSON and non-JSON files
✅ Error handling and reporting
✅ Graceful shutdown with Ctrl+C
✅ Manual classification option for existing files
✅ Summary command to check today's results
"""