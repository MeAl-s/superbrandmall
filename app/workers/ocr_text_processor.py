# app/workers/ocr_text_processor.py - Main OCR text processor orchestrator (organized from your original)
import signal
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

# Add directories to Python path  
current_dir = Path(__file__).parent  # app/workers/
app_dir = current_dir.parent         # app/
project_root = app_dir.parent        # C:\Point Detection

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

# Configure logging - exactly like your original
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import our organized services
from config.settings import settings
from services.ocr_text_processor.file_service import FileService
from services.ocr_text_processor.ocr_service import OCRService
from services.ocr_text_processor.processing_service import ProcessingService

class RealtimeOCRProcessor:
    """Main OCR text processor orchestrator - organized from your original RealtimeOCRProcessor class"""
    
    def __init__(self):
        self.file_service = FileService()
        self.ocr_service = OCRService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
    def initialize(self):
        """Initialize the OCR processor - enhanced with date organization"""
        print("🔧 Initializing Real-time OCR Processor...")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"📝 Output to: {self.file_service.get_output_directory()}")
        print(f"🗓️ Processing files for: {datetime.now().strftime('%Y-%m-%d')}")
        
        # Check Tesseract installation
        if not self.ocr_service.check_tesseract_installation():
            print("❌ Tesseract not properly installed. Please install Tesseract OCR.")
            return False
        
        return True
        
    def run_realtime_monitor(self, check_interval: int = 120):
        """Main real-time monitoring loop - exact logic from your original"""
        print(f"\n🚀 Starting Real-time OCR Processor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📦 Batch size: {self.processing_service.batch_size} files")
        print(f"⏰ Max batch time: {self.processing_service.max_processing_time} seconds")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"📝 Output to: {self.file_service.get_output_directory()}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping OCR processor...")
            stats = self.processing_service.get_ocr_stats()
            print(f"📊 Final stats: {stats['total_processed']} files processed")
            self.is_running = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        self.is_running = True
        
        try:
            while self.is_running:
                print(f"\n🔍 Scanning for new images... {datetime.now().strftime('%H:%M:%S')}")
                
                # Scan for new files
                processed_files = self.processing_service.get_processed_files()
                new_files = self.file_service.scan_for_new_files(processed_files)
                
                if new_files:
                    # Process in batches - exactly like your original
                    for i in range(0, len(new_files), self.processing_service.batch_size):
                        batch = new_files[i:i + self.processing_service.batch_size]
                        self.processing_service.process_batch(batch, self.file_service, self.ocr_service)
                        
                        # Small break between batches - exactly like your original
                        if i + self.processing_service.batch_size < len(new_files):
                            print("😴 Brief pause between batches...")
                            time.sleep(5)
                else:
                    print("📭 No new images to process")
                
                # Print session stats - exact format from your original
                self.processing_service.print_session_stats()
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def process_all_existing(self):
        """Process all existing files once - from your original --process-existing logic"""
        return self.processing_service.process_existing_files(self.file_service, self.ocr_service)
    
    def get_ocr_summary(self):
        """Get today's OCR summary"""
        summary = self.file_service.get_ocr_summary()
        
        print(f"\n📅 TODAY'S OCR SUMMARY ({summary['date']}):")
        print(f"    📷 Images available: {summary['images_available']}")
        print(f"    📝 OCR completed: {summary['ocr_completed']}")
        print(f"    ⏳ Remaining: {summary['remaining']}")
        print(f"    📁 Output folder: {summary['output_dir']}")
        print(f"    📂 Source folder: {summary['source_dir']}")
        
        return summary

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION - exactly like your original
# ═══════════════════════════════════════════════════════════════

def main():
    """Main execution function - matches your original __main__ block"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time OCR Processor with Date Organization")
    parser.add_argument("--interval", type=int, default=120,
                       help="Check interval in seconds (default: 120)")
    parser.add_argument("--batch-size", type=int, default=10,
                       help="Files to process per batch (default: 10)")
    parser.add_argument("--process-existing", action="store_true",
                       help="Process all existing files once and exit")
    parser.add_argument("--summary", action="store_true",
                       help="Show today's OCR summary and exit")
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = RealtimeOCRProcessor()
    
    if not processor.initialize():
        print("❌ Initialization failed. Exiting.")
        sys.exit(1)
    
    # Set batch size if specified
    if args.batch_size:
        processor.processing_service.batch_size = args.batch_size
    
    if args.summary:
        processor.get_ocr_summary()
        sys.exit(0)
    elif args.process_existing:
        processor.process_all_existing()
    else:
        # Start real-time monitoring
        processor.run_realtime_monitor(args.interval)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES - exactly like your original
# ═══════════════════════════════════════════════════════════════

"""
ORGANIZED REAL-TIME OCR PROCESSOR WITH DATE ORGANIZATION

USAGE:

1. Start real-time monitoring (default 2-minute intervals):
   python app/workers/ocr_text_processor.py

2. Custom settings:
   python app/workers/ocr_text_processor.py --interval 180 --batch-size 5

3. Process all existing files once:
   python app/workers/ocr_text_processor.py --process-existing

4. Show today's OCR summary:
   python app/workers/ocr_text_processor.py --summary

PERFORMANCE OPTIMIZATION:

Your Performance: 100 receipts = 10 minutes (6 seconds/receipt)

Recommended Settings:
- Check interval: 120 seconds (2 minutes)
- Batch size: 10 files
- Max batch time: 300 seconds (5 minutes)

This means:
✅ Light load (10 files): Processes in ~60s, ready for next batch
✅ Heavy load (20+ files): Processes in batches, may queue but won't crash
⚠️ Very heavy load (100+ files): Will fall behind but keep processing

RUNNING THE COMPLETE PIPELINE:

Terminal 1: python app/workers/realtime_detector.py
Terminal 2: python app/workers/ocr_processor.py  
Terminal 3: python app/workers/ocr_classification.py
Terminal 4: python app/workers/ocr_downloader.py
Terminal 5: python app/workers/ocr_text_processor.py

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
├── downloaded_receipts/
│   └── 2025-07-23/              ← Downloaded images/PDFs
│       ├── receipt1.jpg
│       ├── receipt2.pdf
│       └── receipt3.png
└── receipt_ocr_text/
    └── 2025-07-23/              ← OCR text results ← NEW
        ├── receipt1.json        ← Contains extracted text
        ├── receipt2.json
        └── receipt3.json

FEATURES:
✅ Same functionality as original - just organized by date
✅ Date-based folder organization for easy daily processing
✅ Smart batching for performance optimization
✅ Time limits to prevent hanging
✅ Performance monitoring and statistics
✅ Chinese + English OCR support
✅ Real-time processing stats
✅ Graceful handling of heavy loads
✅ Session-only tracking (no history files)
✅ Summary command to check today's results
✅ Tesseract installation verification
✅ Image preprocessing for better accuracy
✅ Comprehensive error handling and logging
"""