# app/workers/delivery_scanner.py - Enhanced with dual directory monitoring
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

# Import our enhanced services
from config.settings import settings
from app.services.delivery_scanner.file_service import FileService
from app.services.delivery_scanner.detection_service import DetectionService
from app.services.delivery_scanner.processing_service import ProcessingService

class RealtimeDeliveryScanner:
    """Enhanced delivery scanner with dual directory monitoring"""
    
    def __init__(self):
        self.file_service = FileService()
        self.detection_service = DetectionService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
    def initialize(self):
        """Initialize the enhanced delivery scanner"""
        print("🔧 Initializing Enhanced Real-time Delivery Scanner...")
        
        # Check for dual directory support (your FileService now has this)
        if hasattr(self.file_service, 'today_primary_source') and hasattr(self.file_service, 'today_secondary_source'):
            print(f"📂 Monitoring TWO source directories:")
            print(f"   📂 Primary: {self.file_service.today_primary_source}")
            print(f"   📂 Secondary: {self.file_service.today_secondary_source}")
        else:
            # Fallback (shouldn't happen with your new FileService)
            print(f"📂 Monitoring source directory:")
            print(f"   📂 Primary: {self.file_service.get_source_directory()}")
        
        print(f"🚚 Delivery → {self.file_service.get_delivery_directory()}")
        print(f"📋 Non-delivery → {self.file_service.get_non_delivery_directory()}")
        print(f"🔍 Keywords: {', '.join(self.detection_service.get_keywords())}")
        print(f"🗓️ Processing files for: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"🗑️  Auto-cleanup: Files removed from source after processing")
        
    def run_realtime_monitor(self, check_interval: int = 30):
        """Enhanced real-time monitoring loop with dual directory support"""
        print(f"\n🚀 Starting Enhanced Real-time Delivery Receipt Scanner")
        print(f"⏱️  Check interval: {check_interval} seconds")
        
        # Your FileService now has dual directory support
        if hasattr(self.file_service, 'today_primary_source') and hasattr(self.file_service, 'today_secondary_source'):
            print(f"📂 Monitoring directories:")
            print(f"   📂 Primary: {self.file_service.today_primary_source}")
            print(f"   📂 Secondary: {self.file_service.today_secondary_source}")
            dual_mode = True
        else:
            print(f"📂 Monitoring directory:")
            print(f"   📂 Source: {self.file_service.get_source_directory()}")
            dual_mode = False
        
        print(f"🚚 Delivery receipts → {self.file_service.get_delivery_directory()}")
        print(f"📋 Non-delivery → {self.file_service.get_non_delivery_directory()}")
        print(f"🔍 Keywords: {', '.join(self.detection_service.get_keywords())}")
        print(f"⚡ Processing up to 20 files per batch for faster throughput")
        print(f"🗑️  Auto-cleanup enabled: Source files removed after processing")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping enhanced delivery scanner...")
            stats = self.processing_service.get_scanning_stats()
            print(f"📊 Final stats:")
            print(f"   📁 Total processed: {stats['total_processed']} files")
            print(f"   🚚 Delivery found: {stats['delivery_found']}")
            
            # Show dual directory stats
            if dual_mode and stats.get('secondary_source_processed', 0) > 0:
                print(f"   📂 Primary source: {stats['primary_source_processed']}")
                print(f"   📂 Secondary source: {stats['secondary_source_processed']}")
            else:
                print(f"   📂 Files processed: {stats['primary_source_processed']}")
                
            self.is_running = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        self.is_running = True
        
        try:
            while self.is_running:
                print(f"\n🔍 Scanning for new files... {datetime.now().strftime('%H:%M:%S')}")
                
                # Scan for new files from both directories
                processed_files = self.processing_service.get_processed_files()
                new_files = self.file_service.scan_for_new_files(processed_files)
                
                if new_files:
                    self.processing_service.process_new_files(new_files, self.file_service, self.detection_service)
                else:
                    if dual_mode:
                        print("📭 No new files to scan in either directory")
                    else:
                        print("📭 No new files to scan")
                
                # Print session stats
                self.processing_service.print_session_stats()
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def scan_all_existing(self):
        """Scan all existing files from both directories"""
        result = self.processing_service.scan_existing_files(self.file_service, self.detection_service)
        
        # Show enhanced log file location
        summary = self.file_service.get_delivery_summary()
        if summary["log_file"]:
            print(f"\n📝 Delivery detection log: {summary['log_file']}")
        
        return result
    
    def get_delivery_summary(self):
        """Get delivery scanning summary with dual directory support"""
        summary = self.file_service.get_delivery_summary()
        
        # Your enhanced FileService now provides dual directory info
        print(f"\n📅 TODAY'S ENHANCED DELIVERY SUMMARY ({summary['date']}):")
        print(f"    📂 Source directories:")
        print(f"       Primary (receipt_checked): {summary['primary_source_files']} files")
        print(f"       Secondary (receipt_ocr_text): {summary['secondary_source_files']} files")
        print(f"       Total source files: {summary['source_files']} files")
        
        print(f"    📊 Processing results:")
        print(f"       🚚 Delivery found: {summary['delivery_found']}")
        print(f"       📋 Non-delivery: {summary['non_delivery']}")
        print(f"       📁 Total processed: {summary['total_processed']}")
        print(f"       ⏳ Remaining: {summary['remaining']}")
        
        print(f"    📁 Output directories:")
        print(f"       🚚 Delivery folder: {summary['delivery_dir']}")
        print(f"       📋 Non-delivery folder: {summary['non_delivery_dir']}")
        
        if summary.get('log_file'):
            print(f"    📝 Detection log: {summary['log_file']}")
        
        # Calculate delivery rate
        if summary['total_processed'] > 0:
            delivery_rate = (summary['delivery_found'] / summary['total_processed']) * 100
            print(f"    📈 Delivery rate: {delivery_rate:.1f}%")
        
        return summary

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

def main():
    """Main execution function with enhanced dual directory support"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Real-time Delivery Receipt Scanner with Dual Directory Monitoring")
    parser.add_argument("--interval", type=int, default=30,
                       help="Check interval in seconds (default: 30)")
    parser.add_argument("--scan-existing", action="store_true",
                       help="Scan all existing files from both directories once and exit")
    parser.add_argument("--summary", action="store_true",
                       help="Show today's enhanced delivery summary and exit")
    
    args = parser.parse_args()
    
    # Initialize enhanced scanner
    scanner = RealtimeDeliveryScanner()
    scanner.initialize()
    
    if args.summary:
        scanner.get_delivery_summary()
        sys.exit(0)
    elif args.scan_existing:
        scanner.scan_all_existing()
    else:
        # Start enhanced real-time monitoring
        scanner.run_realtime_monitor(args.interval)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════════
# ENHANCED USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

"""
ENHANCED REAL-TIME DELIVERY RECEIPT SCANNER WITH DUAL DIRECTORY MONITORING

NEW FEATURES:
✅ Monitors TWO source directories simultaneously:
   - C:\\Point Detection\\worker\\data\\receipt_checked\\2025-07-30\\
   - C:\\Point Detection\\worker\\data\\receipt_ocr_text\\2025-07-30\\

✅ Automatic date folder detection for different dates
✅ Source file cleanup after processing (removes from both directories)
✅ Enhanced logging with source directory information
✅ Dual directory statistics and reporting

USAGE:

1. Start enhanced real-time monitoring:
   python app/workers/delivery_scanner.py

2. Custom monitoring interval:
   python app/workers/delivery_scanner.py --interval 45

3. Scan all existing files from both directories:
   python app/workers/delivery_scanner.py --scan-existing

4. Show enhanced summary with dual directory info:
   python app/workers/delivery_scanner.py --summary

ENHANCED WORKFLOW:

The scanner now monitors BOTH directories:
1. receipt_checked/2025-07-30/ (primary source)
2. receipt_ocr_text/2025-07-30/ (secondary source)

When a file is processed:
1. ✅ File content is analyzed for delivery keywords
2. ✅ File is moved to delivery_found/ or non_delivery/
3. ✅ Original file is removed from source directory
4. ✅ If same filename exists in other source, it's also removed
5. ✅ Source directory is logged in detection log

ENHANCED FOLDER STRUCTURE:

C:\\Point Detection\\worker\\data\\
├── receipt_checked/
│   └── 2025-07-30/              ← PRIMARY SOURCE (monitored)
│       ├── file1.json
│       └── file2.json
├── receipt_ocr_text/
│   └── 2025-07-30/              ← SECONDARY SOURCE (monitored) ← NEW
│       ├── file3.json
│       └── file4.json
├── delivery_found/
│   └── 2025-07-30/              ← Delivery receipts
│       ├── delivery_detection_log.txt  ← Enhanced with source info
│       ├── delivery_file1.json
│       └── delivery_file3.json
└── non_delivery/
    └── 2025-07-30/              ← Non-delivery receipts
        ├── regular_file2.json
        └── regular_file4.json

ENHANCED STATISTICS:
📊 SESSION STATS:
   📁 Total processed: 150
   🚚 Delivery found: 45
   📋 Non-delivery: 105
   📂 Primary source: 80        ← NEW
   📂 Secondary source: 70      ← NEW
   ⏱️  Session time: 25.3 minutes
   📈 Delivery rate: 30.0%

KEY ENHANCEMENTS:
✅ Dual directory monitoring
✅ Automatic source file cleanup
✅ Enhanced statistics tracking
✅ Source information in logs
✅ Date folder auto-detection
✅ Backward compatibility maintained
"""