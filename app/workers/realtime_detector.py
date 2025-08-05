# app/workers/realtime_detector.py - FIXED VERSION - Main worker orchestrator
import signal
import sys
import time
from datetime import datetime
from typing import Optional
from pathlib import Path

# Add directories to Python path
current_dir = Path(__file__).parent  # app/workers/
app_dir = current_dir.parent         # app/
project_root = app_dir.parent        # C:\Point Detection

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

# Import our organized services
from config.settings import settings
from app.services.realtime_detector.receipt_api_service import ReceiptAPIService
from app.services.realtime_detector.receipt_processing_service import ReceiptProcessingService
from app.services.realtime_detector.file_service import FileService

class RealtimeReceiptDetector:
    """Main orchestrator - refactored from your original run_realtime_detection function"""
    
    def __init__(self):
        self.api_service = ReceiptAPIService()
        self.processing_service = ReceiptProcessingService()
        self.file_service = FileService()
        self.is_running = False
    
    def run_realtime_detection(self, refresh_interval: Optional[int] = None):
        """Main detection loop - exact logic from your original run_realtime_detection"""
        if refresh_interval:
            settings.realtime_detector.refresh_interval = refresh_interval
        
        # Your original print statements
        print("🚀 Starting Real-Time Receipt Detection System")
        print("="*60)
        print(f"⏱️  Refresh interval: {settings.realtime_detector.refresh_interval} seconds")
        print(f"🗓️ FETCHING TODAY ONLY - No historical data!")
        print("💡 Press Ctrl+C to stop gracefully")
        print("="*60)
        
        # Setup - exactly like your original
        self.processing_service.set_session_start_time(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Register signal handler for graceful shutdown - exactly like your original
        signal.signal(signal.SIGINT, self._signal_handler)
        
        self.is_running = True
        batch_number = 1
        
        try:
            while self.is_running:
                print(f"\n⏰ Fetch #{batch_number} starting at {datetime.now().strftime('%H:%M:%S')}")
                
                # Process single batch
                self._process_single_batch(batch_number)
                
                batch_number += 1
                
                # Wait for next refresh - exactly like your original
                if self.is_running:
                    print(f"😴 Waiting {settings.realtime_detector.refresh_interval} seconds until next fetch...")
                    time.sleep(settings.realtime_detector.refresh_interval)
                
        except KeyboardInterrupt:
            self._signal_handler(None, None)
        except Exception as e:
            # Your original error messages
            print(f"❌ Unexpected error: {e}")
            print(f"🗑️ Session ending - no data saved to history")
            raise
    
    def _process_single_batch(self, batch_number: int):
        """Process a single batch - combines your original fetch, analyze, save logic"""
        try:
            # Fetch recent receipts - from your fetch_recent_receipts
            records = self.api_service.fetch_recent_receipts()
            
            if records:
                # Analyze new vs duplicate - from your analyze_new_vs_duplicate
                analysis = self.processing_service.analyze_new_vs_duplicate(records)
                
                # Print status - from your print_realtime_status
                self.processing_service.print_realtime_status(analysis, batch_number)
                
                # Save data - from your save_realtime_batch
                self.file_service.save_realtime_batch(analysis, batch_number)
                
            else:
                # Your original message
                print(f"📭 No receipts found for today yet")
                
        except Exception as e:
            print(f"❌ Error processing batch #{batch_number}: {e}")
    
    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully - exact logic from your signal_handler"""
        print(f"\n🛑 Stopping real-time detection...")
        print(f"💾 Session complete. No historical data saved.")
        print(f"🗑️ Session data cleared. Goodbye!")
        self.is_running = False
        sys.exit(0)
    
    def get_today_summary(self):
        """Get today's summary - calls your original get_today_summary logic"""
        return self.file_service.get_today_summary()

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION - exactly like your original
# ═══════════════════════════════════════════════════════════════

def main():
    """Main execution function - matches your original __main__ block"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time receipt detection system")
    parser.add_argument("--interval", type=int, default=10, help="Refresh interval in seconds")
    parser.add_argument("--summary", action="store_true", help="Show today's summary and exit")
    
    args = parser.parse_args()
    
    # Create detector instance
    detector = RealtimeReceiptDetector()
    
    if args.summary:
        detector.get_today_summary()
        sys.exit(0)
    
    # Start detection with custom interval
    detector.run_realtime_detection(args.interval)

if __name__ == "__main__":
    main()