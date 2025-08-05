# app/workers/ocr_processor.py - PRODUCTION VERSION (All files, no debug limits)
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
from app.services.ocr_processor.api_service import APIService
from app.services.ocr_processor.file_service import FileService  
from app.services.ocr_processor.processing_service import ProcessingService

class RealtimeOCRProcessor:
    """Main OCR orchestrator - PRODUCTION VERSION"""
    
    def __init__(self):
        self.api_service = APIService()
        self.file_service = FileService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
    def initialize(self):
        """Initialize the OCR processor"""
        print("🔧 Initializing Real-time OCR Processor...")
        print(f"📂 Monitoring folder: {settings.OCR_MONITOR_DIR}")
        print(f"📁 Base OCR folder: {settings.OCR_FILES_DIR}")
        print(f"📅 Today's OCR folder: {self.file_service.get_today_ocr_directory()}")
        print(f"🗓️ Processing files for: {datetime.now().strftime('%Y-%m-%d')}")
        
    def process_receipts(self, receipts):
        """Process receipts - handles all receipts efficiently"""
        if not receipts:
            return
        
        print(f"\n🎯 Processing {len(receipts)} new receipts for OCR...")
        
        successful = 0
        failed = 0
        already_existed = 0
        
        for i, receipt in enumerate(receipts, 1):
            # Extract receipt information (handles both old and new formats)
            receipt_number, feature_code, shop_name = self.processing_service.extract_receipt_info(receipt)
            
            # Progress indicator for large batches
            if i % 100 == 0 or i <= 10:
                print(f"\n📄 [{i}/{len(receipts)}] Processing: {receipt_number}")
                print(f"    🏪 Shop: {shop_name}")
                print(f"    🔑 FeatureCode: {feature_code}")
            elif i % 50 == 0:
                print(f"📄 [{i}/{len(receipts)}] {receipt_number} (batch progress)")
            
            # Validate receipt data
            is_valid, error_msg = self.processing_service.validate_receipt_data(receipt_number, feature_code)
            if not is_valid:
                failed += 1
                continue
            
            # Download file using receipt number as filename
            file_path, existed = self._download_and_save_file(feature_code, receipt_number)
            
            if file_path:
                if existed:
                    already_existed += 1
                    if i <= 10:  # Only show details for first 10
                        print(f"    ⏭️  File already exists")
                else:
                    successful += 1
                    if i <= 10:  # Only show details for first 10
                        print(f"    ✅ Downloaded successfully")
                
                # Mark featureCode as processed
                self.processing_service.mark_feature_code_processed(feature_code)
            else:
                failed += 1
                if i <= 10:  # Show errors for first 10
                    print(f"    ❌ Download failed")
            
            # Progress update every 100 files
            if i % 100 == 0:
                print(f"📊 Progress: {i}/{len(receipts)} - ✅{successful} ⏭️{already_existed} ❌{failed}")
        
        # Update stats and print summary
        self.processing_service.update_processing_stats(len(receipts), successful, failed, already_existed)
        self.processing_service.print_batch_summary(successful, failed, already_existed, self.file_service.get_today_ocr_directory())
        
        # Show final file count
        if successful > 0:
            today_count = self.file_service.get_today_files_count()
            print(f"\n📋 Today's Files ({datetime.now().strftime('%Y-%m-%d')}):")
            print(f"    📊 Total files in today's folder: {today_count}")
            
            # Show some example files
            example_files = self.file_service.get_example_files(5)
            if example_files:
                print(f"    📄 Recent files:")
                for filename in example_files:
                    print(f"       • {filename}")
    
    def _download_and_save_file(self, feature_code: str, receipt_number: str):
        """Download and save file - optimized for production"""
        # Download file content using featureCode
        file_content, content_type = self.api_service.download_receipt_file(feature_code)
        
        if not file_content:
            return None, False
        
        # Get file extension from content type
        ext = settings.ocr_processor.content_types.get(content_type, ".bin")
        
        # Fix timestamp format first, then encode
        fixed_number = self.file_service.fix_timestamp_format(str(receipt_number))
        filename = self.file_service.encode_filename(fixed_number)
        
        filepath = f"{filename}{ext}"
        full_path = self.file_service.today_ocr_dir / filepath
        
        # Check if already exists
        if full_path.exists():
            return str(full_path), True  # Return path and "already existed" flag
        
        # Save file content
        try:
            with open(full_path, "wb") as fw:
                fw.write(file_content)
            
            return str(full_path), False  # Return path and "newly downloaded" flag
            
        except Exception as e:
            print(f"    ❌ Error saving file: {e}")
            return None, False
    
    def run_realtime_monitor(self, check_interval: int = 15):
        """Main real-time monitoring loop"""
        print(f"\n🚀 Starting Real-time OCR File Download Monitor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"🗓️ REAL-TIME ONLY - Processing today's receipts")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping OCR processor...")
            print(f"📊 Final stats:")
            self.processing_service.print_session_stats()
            print(f"🗑️ Session data cleared. Goodbye!")
            self.is_running = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        self.is_running = True
        
        try:
            while self.is_running:
                print(f"\n🔍 Checking for new receipts... {datetime.now().strftime('%H:%M:%S')}")
                
                # Load receipts from JSON file
                all_receipts = self.file_service.load_receipts_from_json()
                
                # Filter to unprocessed receipts
                new_receipts = self.processing_service.filter_unprocessed_receipts(all_receipts)
                
                if new_receipts:
                    self.process_receipts(new_receipts)
                else:
                    print("📭 No new receipts to process")
                
                # Print session stats
                self.processing_service.print_session_stats()
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def process_all_existing(self):
        """Process all existing receipts once"""
        print("🔄 Processing all existing receipts from today's file...")
        
        # Load all receipts
        all_receipts = self.file_service.load_receipts_from_json()
        
        if all_receipts:
            print(f"📊 Found {len(all_receipts)} total receipts")
            self.process_receipts(all_receipts)
        else:
            print("📭 No receipts found to process")
    
    def get_today_summary(self):
        """Get today's OCR file summary"""
        today = datetime.now().strftime('%Y-%m-%d')
        today_count = self.file_service.get_today_files_count()
        
        print(f"\n📅 TODAY'S OCR SUMMARY ({today}):")
        print(f"    📁 Folder: {self.file_service.get_today_ocr_directory()}")
        print(f"    📊 Total files: {today_count}")
        
        if today_count > 0:
            print(f"    📄 Recent files:")
            example_files = self.file_service.get_example_files(10)
            for filename in example_files:
                print(f"       • {filename}")
        
        all_folders = self.file_service.get_all_date_folders()
        if len(all_folders) > 1:
            print(f"\n📂 All date folders: {', '.join(all_folders)}")
        
        return {"today": today, "count": today_count, "folder": str(self.file_service.get_today_ocr_directory())}

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time OCR File Downloader")
    parser.add_argument("--interval", type=int, default=15,
                       help="Check interval in seconds")
    parser.add_argument("--process-existing", action="store_true",
                       help="Process all existing receipts from today's JSON file once")
    parser.add_argument("--summary", action="store_true",
                       help="Show today's OCR file summary and exit")
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = RealtimeOCRProcessor()
    processor.initialize()
    
    if args.summary:
        processor.get_today_summary()
        sys.exit(0)
    elif args.process_existing:
        processor.process_all_existing()
    else:
        # Start real-time monitoring
        processor.run_realtime_monitor(args.interval)

if __name__ == "__main__":
    main()