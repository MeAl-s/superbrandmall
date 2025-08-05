# app/workers/enhanced_realtime_database_inserter.py - Docker-Ready Version
import sys
import os
import time
import signal
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import argparse
from queue import Empty
import threading
import logging

# Add project paths
current_dir = Path(__file__).parent
app_dir = current_dir.parent
project_root = app_dir.parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

# Import services - with better error handling and debugging
print(f"🐍 Python path entries:")
for i, path in enumerate(sys.path[:5]):  # Show first 5 entries
    print(f"   {i}: {path}")

print(f"\n📁 Current working directory: {Path.cwd()}")
print(f"📁 App directory: {app_dir}")
print(f"📁 Project root: {project_root}")

print("\n🔄 Attempting imports...")

try:
    from services.database.receipt_service import ReceiptService
    print("✅ Successfully imported ReceiptService")
except ImportError as e:
    print(f"❌ Failed to import ReceiptService: {e}")
    ReceiptService = None

try:
    from services.database.file_processing_service import FileProcessingService
    print("✅ Successfully imported FileProcessingService")
except ImportError as e:
    print(f"❌ Failed to import FileProcessingService: {e}")
    FileProcessingService = None

try:
    from config.settings import settings
    print("✅ Successfully imported settings")
except ImportError as e:
    print(f"⚠️  Settings import warning: {e}")
    settings = None

print("🔄 Import phase complete\n")


class BatchProcessor(threading.Thread):
    """Processes files in batches for better performance"""
    
    def __init__(self, inserter: 'DatabaseInserter', batch_size: int = 10):
        super().__init__(daemon=True, name="BatchProcessor")
        self.inserter = inserter
        self.batch_size = batch_size
        self.running = True
        self.logger = logging.getLogger(f"{__name__}.BatchProcessor")
        
    def run(self) -> None:
        """Main processing loop"""
        batch = []
        consecutive_empty = 0
        
        while self.running:
            try:
                # Adaptive timeout based on recent activity
                timeout = 5.0 if consecutive_empty > 5 else 2.0
                file_path = self.inserter.file_service.get_queued_file(timeout=timeout)
                
                if file_path:
                    batch.append(file_path)
                    consecutive_empty = 0
                    
                    # Process batch when it reaches target size
                    if len(batch) >= self.batch_size:
                        self._process_batch(batch)
                        batch = []
                else:
                    consecutive_empty += 1
                    
            except Exception as e:
                self.logger.error(f"Batch processor error: {e}")
                consecutive_empty += 1
            
            # Process remaining batch if queue is empty
            if consecutive_empty > 0 and batch:
                self._process_batch(batch)
                batch = []
                consecutive_empty = 0
    
    def _process_batch(self, batch: List[Path]) -> None:
        """Process a batch of files"""
        start_time = time.time()
        successful = 0
        
        print(f"⚡ Processing batch of {len(batch)} files...")
        
        for file_path in batch:
            if self.inserter.process_file(file_path):
                successful += 1
        
        elapsed = time.time() - start_time
        self.inserter.update_batch_stats(successful, len(batch), elapsed)
        
        print(f"✅ Batch complete: {successful}/{len(batch)} files in {elapsed:.2f}s")
    
    def stop(self) -> None:
        """Stop the processor"""
        self.running = False


class FileScanner(threading.Thread):
    """Periodically scans for missed files"""
    
    def __init__(self, inserter: 'DatabaseInserter', scan_interval: int = 30):
        super().__init__(daemon=True, name="FileScanner")
        self.inserter = inserter
        self.scan_interval = scan_interval
        self.running = True
        self.logger = logging.getLogger(f"{__name__}.FileScanner")
        
    def run(self) -> None:
        """Scanning loop"""
        scan_count = 0
        
        while self.running:
            try:
                time.sleep(self.scan_interval)
                if not self.running:
                    break
                    
                scan_count += 1
                print(f"🔍 Scanner #{scan_count}: Checking for missed files...")
                
                unprocessed = self.inserter.file_service.find_unprocessed_files()
                if unprocessed:
                    print(f"🚨 Found {len(unprocessed)} unprocessed files!")
                    for file_path in unprocessed:
                        self.inserter.file_service.queue_file(file_path)
                else:
                    print("✅ No missed files found")
                        
            except Exception as e:
                self.logger.error(f"Scanner error: {e}")
    
    def stop(self) -> None:
        """Stop the scanner"""
        self.running = False


class ProcessingStats:
    """Thread-safe statistics tracking"""
    
    def __init__(self):
        self.processed = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.start_time: Optional[datetime] = None
        self.files_per_second = 0.0
        self.db_total_rows = 0
        self.db_today_rows = 0
        self._lock = threading.Lock()
    
    def increment_processed(self) -> None:
        with self._lock:
            self.processed += 1
    
    def increment_successful(self) -> None:
        with self._lock:
            self.successful += 1
    
    def increment_failed(self) -> None:
        with self._lock:
            self.failed += 1
    
    def increment_skipped(self) -> None:
        with self._lock:
            self.skipped += 1
    
    def update_batch_stats(self, successful: int, total: int, elapsed: float) -> None:
        with self._lock:
            self.files_per_second = total / elapsed if elapsed > 0 else 0
    
    def update_database_stats(self, total_rows: int, today_rows: int) -> None:
        with self._lock:
            self.db_total_rows = total_rows
            self.db_today_rows = today_rows
    
    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'processed': self.processed,
                'successful': self.successful,
                'failed': self.failed,
                'skipped': self.skipped,
                'start_time': self.start_time,
                'files_per_second': self.files_per_second,
                'db_total_rows': self.db_total_rows,
                'db_today_rows': self.db_today_rows
            }


class DatabaseInserter:
    """Docker-ready database inserter using the service architecture"""
    
    def __init__(self, watch_dir: str = None):
        # Check if services are available
        if FileProcessingService is None:
            raise ImportError("FileProcessingService could not be imported. Check your Python path and service files.")
        
        # Initialize services
        self.receipt_service: Optional[ReceiptService] = None
        self.file_service: Optional[FileProcessingService] = None
        self.stats = ProcessingStats()
        
        # Threading components
        self.batch_processor: Optional[BatchProcessor] = None
        self.file_scanner: Optional[FileScanner] = None
        self.is_running = False
        
        # Configuration from environment variables (Docker-friendly)
        self.batch_size = int(os.getenv('WORKER_BATCH_SIZE', os.getenv('BATCH_SIZE', 10)))
        self.scan_interval = int(os.getenv('WORKER_SCAN_INTERVAL', os.getenv('SCAN_INTERVAL', 30)))
        
        # Docker paths configuration
        if os.name == 'nt':  # Windows
            default_watch = Path(r"C:\Point Detection\worker\data\converted_tz")
        else:  # Docker/Linux
            default_watch = Path("/app/worker/data/converted_tz")
        
        # Use environment variable or provided path or default
        watch_path = (
            watch_dir or 
            os.getenv('WATCH_DIRECTORY') or 
            os.getenv('WATCH_DIR') or 
            str(default_watch)
        )
        
        # Initialize file service (no processed directory needed in Docker mode)
        try:
            self.file_service = FileProcessingService(watch_path, None)  # No processed dir
            print("✅ File service initialized successfully")
            print(f"📁 Watch directory: {self.file_service.watch_path}")
        except Exception as e:
            print(f"❌ Failed to initialize file service: {e}")
            raise
        
        # Logging
        self.logger = logging.getLogger(f"{__name__}.DatabaseInserter")
    
    def initialize(self) -> bool:
        """Initialize the database inserter with all services"""
        worker_name = os.getenv('WORKER_NAME', 'database-inserter')
        print(f"🚀 Initializing {worker_name.upper()} (Docker Mode)...")
        print(f"📁 Watch: {self.file_service.watch_path}")
        print(f"🐳 Docker mode: Files stay in place (no movement)")
        print(f"⚡ Batch size: {self.batch_size}")
        print(f"🔧 Scan interval: {self.scan_interval}s")
        
        # Debug environment variables
        if os.getenv('DEBUG', '').lower() in ('true', '1', 'yes'):
            print(f"\n🐛 DEBUG INFO:")
            print(f"   WORKER_NAME: {os.getenv('WORKER_NAME')}")
            print(f"   WATCH_DIRECTORY: {os.getenv('WATCH_DIRECTORY')}")
            print(f"   DATABASE_URL: {os.getenv('DATABASE_URL', '').replace(os.getenv('POSTGRES_PASSWORD', ''), '***') if os.getenv('DATABASE_URL') else 'Not set'}")
            print(f"   PYTHONPATH: {os.getenv('PYTHONPATH')}")
        
        # Validate file paths
        if not self.file_service.validate_paths():
            return False
        
        # Initialize receipt service
        if ReceiptService is None:
            print("❌ ReceiptService not available - check imports")
            return False
        
        self.receipt_service = ReceiptService()
        
        if not self.receipt_service.connect():
            print("❌ Failed to connect to database")
            return False
        
        # Setup database schema
        if not self.receipt_service.create_tables():
            print("❌ Failed to create database schema")
            return False
        
        # Get initial database statistics
        self._update_database_stats()
        
        print(f"📊 Database: {self.stats.db_total_rows:,} total, {self.stats.db_today_rows:,} today")
        print("✅ Initialization complete")
        
        return True
    
    def scan_existing_files(self) -> Dict[str, List[Path]]:
        """Scan for existing files using file service"""
        return self.file_service.scan_existing_files()
    
    def process_existing_files(self) -> None:
        """Process all existing files found in watch directory"""
        print("\n🔍 Processing all existing files...")
        
        date_files = self.scan_existing_files()
        if not date_files:
            print("📭 No files to process")
            return
        
        self.stats.start_time = datetime.now()
        
        # Process each date folder
        for date_str, files in sorted(date_files.items()):
            print(f"\n📅 Processing {date_str}: {len(files)} files")
            
            # Mark files as processed to avoid reprocessing
            for file_path in files:
                self.file_service.mark_file_processed(file_path)
            
            date_successful = 0
            date_failed = 0
            date_skipped = 0
            
            # Process files
            for file_path in files:
                try:
                    if not file_path.exists():
                        continue
                    
                    result = self._process_single_file(file_path, date_str)
                    if result == "success":
                        date_successful += 1
                    elif result == "skipped":
                        date_skipped += 1
                    else:
                        date_failed += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing {file_path.name}: {e}")
                    date_failed += 1
            
            # Update daily stats
            if date_successful > 0:
                self.receipt_service.update_daily_stats(datetime.strptime(date_str, '%Y-%m-%d').date())
            
            print(f"   ✅ Successful: {date_successful}")
            print(f"   ⏭️ Skipped: {date_skipped}")
            print(f"   ❌ Failed: {date_failed}")
        
        # Final summary
        elapsed = (datetime.now() - self.stats.start_time).total_seconds()
        stats = self.stats.get_snapshot()
        
        print(f"\n✅ PROCESSING COMPLETE!")
        print(f"📊 Processed: {stats['processed']}")
        print(f"✅ Successful: {stats['successful']}")
        print(f"⏭️ Skipped: {stats['skipped']}")
        print(f"❌ Failed: {stats['failed']}")
        print(f"⏱️ Time: {elapsed:.1f}s")
        
        self._update_database_stats()
        print(f"📊 Database now has: {self.stats.db_total_rows:,} total receipts")
    
    def process_file(self, file_path: Path) -> bool:
        """Process a single file (public interface)"""
        try:
            if not file_path.exists():
                return False
            
            # Extract date from parent directory
            date_str = file_path.parent.name
            
            result = self._process_single_file(file_path, date_str)
            return result == "success"
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
            return False
    
    def _process_single_file(self, file_path: Path, date_str: str) -> str:
        """Process a single file (internal implementation) - Docker version"""
        try:
            # Validate date folder
            try:
                folder_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                print(f"❌ Invalid date folder: {date_str}")
                self.stats.increment_failed()
                return "failed"
            
            # Parse JSON using receipt service
            receipt_data = self.receipt_service.parse_json_file_for_processing(file_path)
            if receipt_data is None:
                print(f"❌ Failed to parse: {file_path.name}")
                self.stats.increment_failed()
                return "failed"
            
            # Use folder date for consistency
            receipt_data['processing_date'] = folder_date
            
            # Check for duplicates using receipt service
            if self.receipt_service.check_duplicate_receipt(receipt_data['receipt_number'], folder_date):
                print(f"⏭️ Duplicate: {receipt_data['receipt_number']}")
                self.stats.increment_skipped()
                # Docker mode: Don't move files, just mark as processed
                return "skipped"
            
            # Insert into database using receipt service
            receipt_id = self.receipt_service.insert_receipt(receipt_data)
            
            if receipt_id:
                self.stats.increment_successful()
                self._display_success(receipt_data, file_path, date_str, receipt_id)
                # Docker mode: Don't move files, just mark as processed
                return "success"
            else:
                print(f"❌ Insert failed: {receipt_data['receipt_number']}")
                self.stats.increment_failed()
                return "failed"
                
        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
            self.stats.increment_failed()
            return "failed"
    
    def _display_success(self, receipt_data: Dict, file_path: Path, date_str: str, receipt_id: int) -> None:
        """Display success notification"""
        store_name = receipt_data.get('store_name') or 'Unknown'
        amount = receipt_data.get('ticket_amount')
        amount_str = f"{float(amount):.2f}" if amount is not None else "0.00"
        
        print_time = receipt_data.get('print_time')
        time_str = print_time.strftime('%H:%M:%S') if print_time else 'Unknown'
        
        print(f"\n🆕 NEW RECEIPT PROCESSED!")
        print(f"   📄 File: {file_path.name}")
        print(f"   🧾 Receipt: {receipt_data['receipt_number']}")
        print(f"   🏪 Store: {store_name}")
        print(f"   💰 Amount: {amount_str}")
        print(f"   📅 Date: {date_str}")
        print(f"   ⏰ Time: {time_str}")
        print(f"   🔑 DB ID: {receipt_id}")
        if receipt_data.get('store_id'):
            print(f"   🏷️ Store ID: {receipt_data['store_id']}")
        print()
    
    def _update_database_stats(self) -> None:
        """Update database statistics using receipt service"""
        try:
            total_rows = self.receipt_service.get_receipt_count()
            today_rows = self.receipt_service.get_receipt_count_by_date(datetime.now().date())
            self.stats.update_database_stats(total_rows, today_rows)
            
        except Exception as e:
            self.logger.error(f"Could not update DB stats: {e}")
    
    def update_batch_stats(self, successful: int, total: int, elapsed: float) -> None:
        """Update batch processing statistics"""
        self.stats.update_batch_stats(successful, total, elapsed)
    
    def start_monitoring(self) -> None:
        """Start real-time file monitoring and processing"""
        print("\n👁️ Starting real-time monitoring...")
        print(f"📁 Monitoring: {self.file_service.watch_path}")
        print(f"🔧 Scan interval: {self.scan_interval}s")
        print(f"⚡ Batch size: {self.batch_size}")
        print(f"🐳 Docker mode: Files stay in place (no movement)")
        
        self.is_running = True
        self.stats.start_time = datetime.now()
        
        # Start file monitoring
        if not self.file_service.start_monitoring():
            print("❌ Failed to start file monitoring")
            return
        print("✅ File observer started")
        
        # Start batch processor
        self.batch_processor = BatchProcessor(self, self.batch_size)
        self.batch_processor.start()
        print("✅ Batch processor started")
        
        # Start file scanner
        self.file_scanner = FileScanner(self, self.scan_interval)
        self.file_scanner.start()
        print("✅ File scanner started")
        
        print("✅ Real-time monitoring active")
        print("   Press Ctrl+C to stop\n")
        
        try:
            self._monitoring_loop()
        except KeyboardInterrupt:
            print("\n⚠️ Stopping monitoring...")
            self.stop_monitoring()
    
    def _monitoring_loop(self) -> None:
        """Main monitoring loop with status updates"""
        while self.is_running:
            time.sleep(10)  # Status update every 10 seconds
            
            self._update_database_stats()
            stats = self.stats.get_snapshot()
            file_stats = self.file_service.get_file_stats()
            
            now = datetime.now()
            elapsed = (now - stats['start_time']).total_seconds()
            speed = stats['processed'] / elapsed if elapsed > 0 else 0
            
            print(f"\n{'='*60}")
            print(f"🐳 DATABASE INSERTER (DOCKER) - {now.strftime('%H:%M:%S')}")
            print(f"{'='*60}")
            print(f"⏱️  Uptime: {str(timedelta(seconds=int(elapsed)))}")
            print(f"📈 Processed: {stats['processed']} files")
            print(f"✅ Successful: {stats['successful']}")
            print(f"⏭️  Skipped: {stats['skipped']}")
            print(f"❌ Failed: {stats['failed']}")
            print(f"⚡ Speed: {speed:.1f} files/sec")
            print(f"📥 Queue: {file_stats['queue_size']} pending")
            print(f"🗄️  DB: {stats['db_total_rows']:,} total, {stats['db_today_rows']:,} today")
            print(f"👁️  Monitoring: {'Active' if file_stats['monitoring'] else 'Inactive'}")
            print(f"{'='*60}")
    
    def stop_monitoring(self) -> None:
        """Stop all monitoring components"""
        print("🛑 Stopping monitoring...")
        self.is_running = False
        
        # Stop file monitoring
        if self.file_service:
            self.file_service.stop_monitoring()
            print("✅ File observer stopped")
        
        # Stop batch processor
        if self.batch_processor:
            self.batch_processor.stop()
            print("✅ Batch processor stopped")
        
        # Stop file scanner
        if self.file_scanner:
            self.file_scanner.stop()
            print("✅ File scanner stopped")
        
        print("✅ Monitoring stopped")
    
    def cleanup(self) -> None:
        """Cleanup all resources"""
        print("🧹 Starting cleanup...")
        
        # Stop monitoring
        self.stop_monitoring()
        
        # Cleanup services
        if self.file_service:
            self.file_service.cleanup()
            print("✅ File service cleaned up")
        
        if self.receipt_service:
            self.receipt_service.disconnect()
            print("✅ Database connection closed")
        
        print("🧹 Cleanup completed")
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health_status = {
            'database': False,
            'file_service': False,
            'monitoring': False,
            'services_initialized': False,
            'errors': []
        }
        
        try:
            # Check receipt service
            if self.receipt_service:
                db_health = self.receipt_service.health_check()
                health_status['database'] = db_health['connected'] and db_health['tables_exist']
                if not health_status['database']:
                    health_status['errors'].extend(db_health['errors'])
            else:
                health_status['errors'].append("Receipt service not initialized")
            
            # Check file service
            if self.file_service:
                health_status['file_service'] = self.file_service.validate_paths()
                file_stats = self.file_service.get_file_stats()
                health_status['monitoring'] = file_stats['monitoring']
            else:
                health_status['errors'].append("File service not initialized")
            
            # Overall service status
            health_status['services_initialized'] = (
                self.receipt_service is not None and 
                self.file_service is not None
            )
            
        except Exception as e:
            health_status['errors'].append(f"Health check failed: {e}")
            self.logger.error(f"Health check error: {e}")
        
        return health_status


def main():
    """Main entry point with Docker-friendly argument handling (matches your worker pattern)"""
    parser = argparse.ArgumentParser(description="Database Inserter Worker - Point Detection Pipeline")
    parser.add_argument("--process-all", action="store_true", help="Process all existing files")
    parser.add_argument("--monitor", action="store_true", help="Start real-time monitoring")
    parser.add_argument("--monitor-only", action="store_true", help="Only monitor (skip batch)")
    parser.add_argument("--health-check", action="store_true", help="Perform health check and exit")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--watch-dir", type=str, help="Custom watch directory")
    
    args = parser.parse_args()
    
    # Setup logging (match your worker pattern)
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    if args.debug or os.getenv('DEBUG', '').lower() in ('true', '1', 'yes'):
        log_level = 'DEBUG'
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Worker identification
    worker_name = os.getenv('WORKER_NAME', 'database-inserter')
    print(f"🔵 Starting {worker_name.upper()} Worker")
    print(f"🐳 Docker Environment: {os.getenv('WORKER_NAME', 'Unknown')}")
    
    # Create inserter with Docker-compatible paths
    watch_dir = args.watch_dir or os.getenv('WATCH_DIRECTORY') or os.getenv('WATCH_DIR')
    inserter = DatabaseInserter(watch_dir)
    
    # Setup signal handler for graceful shutdown (important for Docker)
    def signal_handler(signum, frame):
        print(f"\n🛑 {worker_name.upper()} received shutdown signal...")
        inserter.cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize services
        if not inserter.initialize():
            print(f"❌ {worker_name.upper()} failed to initialize")
            return 1
        
        # Health check mode
        if args.health_check:
            health = inserter.health_check()
            print(f"\n🏥 {worker_name.upper()} HEALTH CHECK:")
            print(f"📊 Database: {'✅' if health['database'] else '❌'}")
            print(f"📁 File Service: {'✅' if health['file_service'] else '❌'}")
            print(f"👁️  Monitoring: {'✅' if health['monitoring'] else '❌'}")
            print(f"🔧 Services: {'✅' if health['services_initialized'] else '❌'}")
            
            if health['errors']:
                print("\n❌ ERRORS:")
                for error in health['errors']:
                    print(f"   • {error}")
            else:
                print(f"\n✅ {worker_name.upper()} operational!")
            
            return 0 if not health['errors'] else 1
        
        # Process existing files if requested
        if args.process_all or (not args.monitor_only):
            inserter.process_existing_files()
        
        # Start monitoring if requested (default behavior for Docker)
        if args.monitor or args.monitor_only or (not args.process_all):
            inserter.start_monitoring()
        
        return 0
        
    except Exception as e:
        print(f"\n❌ {worker_name.upper()} unexpected error: {e}")
        if os.getenv('DEBUG', '').lower() in ('true', '1', 'yes'):
            import traceback
            traceback.print_exc()
        return 1
        
    finally:
        inserter.cleanup()


if __name__ == "__main__":
    exit(main())