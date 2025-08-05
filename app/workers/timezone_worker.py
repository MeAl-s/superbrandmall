# app/workers/focused_timezone_worker.py - FIXED with relaxed validation and format conversion
import os
import sys
import time
import json
import signal
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add paths
current_dir = Path(__file__).parent  # app/workers/
app_dir = current_dir.parent         # app/
project_root = app_dir.parent        # C:\Point Detection

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

# Simple built-in format converter (no external dependencies)
def simple_format_converter(data: Dict) -> Optional[Dict]:
    """Simple built-in format converter for common receipt field variations"""
    
    # Field mapping for common variations
    field_mappings = {
        'number': ['number', 'receipt_number', 'receiptNumber', 'ticket_number', 'id'],
        'store_name': ['store_name', 'storeName', 'shop_name', 'shopName', 'merchant_name'],
        'store_id': ['store_id', 'storeId', 'shop_code', 'shopCode', 'merchant_id'],
        'ticketAmount': ['ticketAmount', 'totalAmount', 'total_amount', 'total', 'amount'],
        'print_time': ['print_time', 'printTime', 'timestamp', 'dateTime', 'created_at']
    }
    
    converted = {}
    conversion_made = False
    
    # Try to map each standard field
    for standard_field, variants in field_mappings.items():
        converted[standard_field] = 'unknown'
        
        for variant in variants:
            if variant in data and data[variant] not in [None, '', 'unknown', 'null']:
                converted[standard_field] = data[variant]
                if variant != standard_field:  # Only count if we actually converted something
                    conversion_made = True
                break
    
    # Check if we have enough valid fields
    valid_fields = sum(1 for v in converted.values() if v != 'unknown')
    
    if valid_fields >= 3 and conversion_made:  # Need at least 3 fields and some conversion
        converted['_converted_by'] = 'simple_converter'
        return converted
    
    return None

class FocusedTimezoneHandler(FileSystemEventHandler):
    """Focused file handler - ONLY for matched_non_delivery files"""
    
    def __init__(self, processor):
        self.processor = processor
        self.logger = logging.getLogger(f"{__name__}.FocusedTimezoneHandler")
        
    def on_created(self, event):
        """Handle new file creation events"""
        if not event.is_directory and event.src_path.endswith('.json'):
            file_path = Path(event.src_path)
            
            # SECURITY CHECK: Ensure file is from matched_non_delivery
            if "matched_non_delivery" not in str(file_path):
                self.logger.warning(f"🚨 BLOCKED: File not from matched_non_delivery: {file_path}")
                return
                
            self.logger.info(f"📥 New matched file detected: {file_path.name}")
            time.sleep(0.2)  # Small delay to ensure file is fully written
            self.processor.process_matched_file(file_path)
    
    def on_modified(self, event):
        """Handle file modification events"""
        if not event.is_directory and event.src_path.endswith('.json'):
            file_path = Path(event.src_path)
            
            # SECURITY CHECK: Ensure file is from matched_non_delivery
            if "matched_non_delivery" not in str(file_path):
                self.logger.warning(f"🚨 BLOCKED: File not from matched_non_delivery: {file_path}")
                return
                
            self.logger.info(f"📝 Matched file modified: {file_path.name}")
            time.sleep(0.2)
            self.processor.process_matched_file(file_path)

class FocusedTimezoneWorker:
    """Focused Timezone Worker - ONLY processes matched_non_delivery files"""
    
    def __init__(self):
        # HARDCODED PATHS - NO CONFUSION
        self.watch_dir = project_root / "worker" / "data" / "matched_non_delivery"
        self.output_dir = project_root / "worker" / "data" / "converted_tz"
        
        # Components
        self.observer = None
        self.is_running = False
        
        # Statistics
        self.stats = {
            'processed': 0, 
            'failed': 0,
            'blocked_files': 0,  # Files from wrong directories
            'already_converted_skipped': 0,  # Track already converted files
            'format_converted': 0,  # Track files that needed format conversion
            'ocr_files_moved': 0,  # NEW: Track OCR files moved back to non_delivery
            'start_time': datetime.now()
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self._initialize()
    
    def _initialize(self):
        """Initialize the focused timezone worker"""
        print("🎯 Focused Timezone Worker - ONLY matched_non_delivery (ENHANCED)")
        print("="*60)
        print(f"📂 WATCH: {self.watch_dir}")
        print(f"📁 OUTPUT: {self.output_dir}")
        print(f"🚨 SECURITY: Only processes files from matched_non_delivery")
        print(f"✅ VALIDATION: Enhanced with format conversion")
        print("="*60)
        
        # Create directories
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Validate directories
        if not self.watch_dir.exists():
            raise Exception(f"Watch directory does not exist: {self.watch_dir}")
            
        self.logger.info("✅ Focused Timezone Worker initialized")
    
    def validate_file_source(self, file_path: Path) -> bool:
        """Validate that file is from matched_non_delivery"""
        if "matched_non_delivery" not in str(file_path):
            self.stats['blocked_files'] += 1
            self.logger.error(f"🚨 SECURITY VIOLATION: Attempted to process file not from matched_non_delivery: {file_path}")
            return False
        return True
    
    def detect_and_move_ocr_file(self, data: Dict, file_path: Path) -> bool:
        """Enhanced OCR detection and automatic file moving"""
        
        # Enhanced OCR detection for multiple OCR formats
        ocr_indicators = {
            'has_ocr_metadata': 'ocr_metadata' in data,
            'has_success_field': 'success' in data and isinstance(data.get('success'), bool),
            'has_message_field': 'message' in data,
            'has_fields_field': 'fields' in data,
            'has_total_field': 'total' in data,
            'has_large_data_field': 'data' in data and isinstance(data.get('data'), str) and len(data.get('data', '')) > 200,
            'has_chinese_text': 'data' in data and isinstance(data.get('data'), str) and self._contains_chinese_text(data.get('data', '')),
            'has_processing_metadata': any(key in data for key in ['processed_at', 'processing_time', 'source_file', 'confidence', 'language'])
        }
        
        # Count OCR indicators
        ocr_count = sum(1 for indicator in ocr_indicators.values() if indicator)
        
        # If it has 3+ OCR indicators, it's definitely an OCR file
        if ocr_count >= 3:
            print(f"   🚨 OCR FILE DETECTED: {file_path.name}")
            print(f"      Indicators found: {ocr_count}/8")
            
            # Log specific indicators found
            found_indicators = [name for name, found in ocr_indicators.items() if found]
            print(f"      Details: {', '.join(found_indicators)}")
            
            # Show sample of OCR data for confirmation
            if 'data' in data and isinstance(data['data'], str):
                sample_text = data['data'][:100].replace('\r\n', ' ').replace('\n', ' ')
                print(f"      Sample text: {sample_text}...")
            
            # Move file back to non_delivery
            if self.move_file_to_non_delivery(file_path):
                self.stats['ocr_files_moved'] += 1
                print(f"   ↩️  MOVED: File moved back to non_delivery folder")
                return True
            else:
                print(f"   ❌ FAILED: Could not move file to non_delivery")
                return False
        
        return False
    
    def _contains_chinese_text(self, text: str) -> bool:
        """Check if text contains Chinese characters"""
        if not text:
            return False
        
        # Check for Chinese characters (CJK unified ideographs)
        chinese_chars = 0
        for char in text[:200]:  # Check first 200 characters for performance
            if '\u4e00' <= char <= '\u9fff':  # Chinese character range
                chinese_chars += 1
                if chinese_chars >= 5:  # If we find 5+ Chinese chars, it's likely OCR
                    return True
        
        return False
    
    def move_file_to_non_delivery(self, file_path: Path) -> bool:
        """Move OCR file back to non_delivery folder with enhanced error handling"""
        try:
            # Determine the date folder from the file path
            if file_path.parent.name.count('-') == 2:  # YYYY-MM-DD format
                date_folder = file_path.parent.name
            else:
                date_folder = datetime.now().strftime('%Y-%m-%d')
            
            # Create non_delivery path
            non_delivery_dir = project_root / "worker" / "data" / "non_delivery" / date_folder
            non_delivery_dir.mkdir(parents=True, exist_ok=True)
            
            # Create destination path
            dest_path = non_delivery_dir / file_path.name
            
            # Handle duplicate filenames
            if dest_path.exists():
                timestamp = datetime.now().strftime("%H%M%S%f")[:9]
                stem = file_path.stem
                suffix = file_path.suffix
                dest_path = non_delivery_dir / f"{stem}_ocr_moved_{timestamp}{suffix}"
            
            # Move the file using shutil
            import shutil
            shutil.move(str(file_path), str(dest_path))
            
            print(f"      📂 Moved to: {dest_path}")
            print(f"      🎯 Destination: non_delivery/{date_folder}/")
            return True
            
        except Exception as e:
            self.logger.error(f"Error moving OCR file to non_delivery: {e}")
            print(f"      💥 Move failed: {e}")
            return False
    def validate_and_convert_format(self, data: Dict, file_path: Path) -> Optional[Dict]:
        """Validate file content and convert format if needed"""
        
        # CHECK 1: Detect and move OCR files FIRST
        if self.detect_and_move_ocr_file(data, file_path):
            return None  # File was moved, don't process further
        
        # CHECK 2: Block already converted files
        timezone_indicators = [
            'timezone_conversion',
            'original_print_time', 
            'converted_time',
            'utc_time',
            'utc_converted'
        ]
        
        for indicator in timezone_indicators:
            if indicator in data:
                self.logger.info(f"⏭️  ALREADY CONVERTED: File contains '{indicator}' field")
                self.stats['already_converted_skipped'] += 1
                return None
        
        # CHECK 3: Additional OCR blocking (fallback)
        if 'ocr_metadata' in data:
            self.logger.error(f"🚨 RAW OCR DATA DETECTED: File contains 'ocr_metadata' field")
            return None
        
        if 'data' in data and isinstance(data['data'], str) and len(data['data']) > 500:
            self.logger.error(f"🚨 RAW OCR DATA DETECTED: File contains large 'data' field with text")
            return None
        
        # CHECK 4: Try to validate current format
        if self.validate_standard_format(data):
            return data  # Already in correct format
        
        # CHECK 5: Attempt simple format conversion
        print(f"   📝 Attempting format conversion for: {file_path.name}")
        converted_data = simple_format_converter(data)
        
        if converted_data:
            self.stats['format_converted'] += 1
            print(f"   ✅ Successfully converted to standard format")
            return converted_data
        else:
            print(f"   ❌ Format conversion failed")
        
        # CHECK 6: Final validation - reject if can't convert
        self.logger.warning(f"⚠️  File does not have valid receipt format: {file_path.name}")
        return None
    
    def validate_standard_format(self, data: Dict) -> bool:
        """Check if data is already in standard timezone worker format"""
        
        # Look for standard receipt fields
        standard_indicators = [
            'number', 'store_name', 'store_id', 'ticketAmount', 'print_time'
        ]
        
        found_standard = 0
        for indicator in standard_indicators:
            if indicator in data and data[indicator] not in [None, '', 'unknown', 'null']:
                found_standard += 1
        
        # Need at least 3 standard fields including number and print_time
        has_required = ('number' in data and data['number'] not in [None, '', 'unknown', 'null'] and
                       'print_time' in data and data['print_time'] not in [None, '', 'unknown', 'null'])
        
        # Additional check: make sure it's not an OCR file disguised as matched data
        if has_required and found_standard >= 3:
            # Double-check it's not OCR data
            if 'ocr_metadata' in data or ('data' in data and isinstance(data.get('data'), str) and len(data.get('data', '')) > 100):
                return False  # It's OCR data, not matched data
            return True
        
        return False
    
    def extract_time_field(self, data: Dict) -> str:
        """Extract time field from various possible locations"""
        time_fields = [
            'print_time', 'printTime', 'timestamp', 'dateTime', 'date_time',
            'created_at', 'createdAt', 'transaction_time', 'transactionTime',
            'purchase_time', 'purchaseTime', 'sale_time', 'saleTime',
            'issued_at', 'issuedAt', 'receipt_time', 'receiptTime'
        ]
        
        for field in time_fields:
            if field in data and data[field] not in [None, '', 'unknown', 'null']:
                return str(data[field])
        
        return 'unknown'
    
    def process_matched_file(self, file_path: Path) -> bool:
        """Process a single MATCHED file (clean data only) with race condition protection"""
        try:
            # CRITICAL: Check if file still exists before processing
            if not file_path.exists():
                print(f"⚠️  FILE MISSING: {file_path.name} no longer exists (already processed)")
                return False
            
            # SECURITY CHECK 1: Validate file source
            if not self.validate_file_source(file_path):
                return False
            
            # CRITICAL: Double-check file exists after validation
            if not file_path.exists():
                print(f"⚠️  FILE DISAPPEARED: {file_path.name} was removed during validation")
                return False
            
            print(f"📄 PROCESSING: {file_path.name}")
            
            # Read JSON file with error handling
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except FileNotFoundError:
                print(f"   ⚠️  FILE NOT FOUND: {file_path.name} was removed while reading")
                return False
            except Exception as e:
                print(f"   ❌ READ ERROR: Could not read {file_path.name}: {e}")
                return False
            
            # ENHANCED VALIDATION & CONVERSION (includes OCR detection and moving)
            validated_data = self.validate_and_convert_format(data, file_path)
            
            if validated_data is None:
                # File was either moved (OCR) or invalid - check if file still exists
                if not file_path.exists():
                    print(f"   ✅ FILE HANDLED: {file_path.name} was moved or processed by validation")
                return False
            
            # CRITICAL: Final existence check before timezone conversion
            if not file_path.exists():
                print(f"   ⚠️  FILE REMOVED: {file_path.name} was processed by another worker")
                return False
            
            # Use the validated/converted data for processing
            data = validated_data
            
            # Process timezone conversion
            converted_data = self.convert_timezone(data)
            
            # Determine output date folder using flexible time extraction
            print_time_str = self.extract_time_field(converted_data)
            if print_time_str and print_time_str.lower() != 'unknown':
                try:
                    # Try multiple time formats
                    time_formats = [
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%d %H:%M:%S.%f',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S.%f',
                        '%Y-%m-%dT%H:%M:%SZ',
                        '%Y%m%d_%H%M%S',
                        '%Y-%m-%d',
                        '%m/%d/%Y %H:%M:%S',
                        '%d/%m/%Y %H:%M:%S'
                    ]
                    
                    print_datetime = None
                    for fmt in time_formats:
                        try:
                            print_datetime = datetime.strptime(print_time_str, fmt)
                            break
                        except ValueError:
                            continue
                    
                    if print_datetime:
                        date_folder = print_datetime.date().strftime('%Y-%m-%d')
                    else:
                        date_folder = datetime.now().strftime('%Y-%m-%d')
                        
                except Exception as e:
                    self.logger.warning(f"Could not parse time '{print_time_str}': {e}")
                    date_folder = datetime.now().strftime('%Y-%m-%d')
            else:
                date_folder = datetime.now().strftime('%Y-%m-%d')
            
            # Create output directory
            output_date_dir = self.output_dir / date_folder
            output_date_dir.mkdir(parents=True, exist_ok=True)
            
            # Create output file path
            output_file = output_date_dir / file_path.name
            
            # CRITICAL: Final check before file operations
            if not file_path.exists():
                print(f"   ⚠️  FILE VANISHED: {file_path.name} disappeared before final processing")
                return False
            
            # Write converted JSON
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(converted_data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"   ❌ WRITE ERROR: Could not write output file: {e}")
                return False
            
            # Remove original file with error handling
            try:
                file_path.unlink()
                print(f"   🧹 Removed from source")
            except FileNotFoundError:
                print(f"   ⚠️  SOURCE ALREADY REMOVED: {file_path.name} was already deleted")
            except Exception as e:
                print(f"   ⚠️  REMOVAL ERROR: Could not remove source file: {e}")
            
            # Update stats
            self.stats['processed'] += 1
            
            # Log conversion
            original_time = self.extract_time_field(data) if 'original_print_time' not in converted_data else data.get('print_time', 'Unknown')
            converted_time = converted_data.get('print_time', 'Unknown')
            
            print(f"✅ CONVERTED: {file_path.name}")
            print(f"   📍 Source: matched_non_delivery/{file_path.parent.name}")
            print(f"   🕐 Time: {original_time} → {converted_time}")
            print(f"   📁 Output: converted_tz/{date_folder}/")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error processing {file_path.name}: {e}")
            self.stats['failed'] += 1
            return False
    
    def convert_timezone(self, data: Dict) -> Dict:
        """Convert timezone from UTC+8 to UTC+0 with flexible time field detection"""
        try:
            print_time_str = self.extract_time_field(data)
            if not print_time_str or str(print_time_str).lower() == 'unknown':
                return data
            
            # Try to parse datetime with multiple formats
            time_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y%m%d_%H%M%S'
            ]
            
            original_time = None
            for fmt in time_formats:
                try:
                    original_time = datetime.strptime(print_time_str, fmt)
                    break
                except ValueError:
                    continue
            
            if not original_time:
                self.logger.warning(f"Could not parse time format: {print_time_str}")
                return data
            
            # Convert from UTC+8 to UTC+0 (subtract 8 hours)
            converted_time = original_time - timedelta(hours=8)
            
            # Update data
            data_copy = data.copy()
            data_copy['print_time'] = converted_time.strftime('%Y-%m-%d %H:%M:%S')
            data_copy['original_print_time'] = print_time_str
            data_copy['timezone_conversion'] = "UTC+8 -> UTC+0"
            
            return data_copy
            
        except Exception as e:
            self.logger.error(f"Error converting timezone: {e}")
            return data
    
    def process_existing_files(self) -> int:
        """Process all existing files in matched_non_delivery"""
        print(f"\n🔍 Scanning for existing files in: {self.watch_dir}")
        
        if not self.watch_dir.exists():
            print("❌ Watch directory does not exist!")
            return 0
        
        # Get all JSON files recursively
        json_files = list(self.watch_dir.rglob("*.json"))
        
        if not json_files:
            print("📭 No existing files to process")
            return 0
        
        print(f"📊 Found {len(json_files)} files to process")
        
        # Show which date folders contain files
        date_folders = set()
        for file_path in json_files:
            if file_path.parent.name.count('-') == 2:  # YYYY-MM-DD format
                date_folders.add(file_path.parent.name)
        
        if date_folders:
            print(f"📅 Date folders with files: {', '.join(sorted(date_folders))}")
        
        processed_count = 0
        for i, file_path in enumerate(json_files, 1):
            print(f"\n📄 [{i}/{len(json_files)}] Processing: {file_path.name}")
            if self.process_matched_file(file_path):
                processed_count += 1
        
        print(f"\n✅ Processed {processed_count} existing files")
        return processed_count
    
    def start_monitoring(self):
        """Start real-time monitoring"""
        if self.is_running:
            print("⚠️  Already monitoring")
            return
        
        print("🚀 Starting focused real-time monitoring...")
        self.is_running = True
        
        # Setup file system observer
        handler = FocusedTimezoneHandler(self)
        self.observer = Observer()
        self.observer.schedule(handler, str(self.watch_dir), recursive=True)
        
        try:
            self.observer.start()
            print(f"👁️  Monitoring: {self.watch_dir}")
            print("✅ Real-time monitoring active!")
        except Exception as e:
            self.logger.error(f"❌ Could not start monitoring: {e}")
            return
    
    def stop_monitoring(self):
        """Stop monitoring"""
        if not self.is_running:
            return
        
        print("🛑 Stopping monitoring...")
        self.is_running = False
        
        if self.observer:
            try:
                self.observer.stop()
                self.observer.join(timeout=5)
                print("✅ Monitoring stopped")
            except Exception as e:
                self.logger.error(f"Error stopping observer: {e}")
    
    def print_stats(self):
        """Print current statistics"""
        elapsed = datetime.now() - self.stats['start_time']
        elapsed_minutes = elapsed.total_seconds() / 60
        
        print(f"\n📊 FOCUSED TIMEZONE WORKER STATS:")
        print(f"   ✅ Files processed: {self.stats['processed']}")
        print(f"   ❌ Files failed: {self.stats['failed']}")
        print(f"   🚨 Files blocked: {self.stats['blocked_files']}")
        print(f"   ⏭️  Already converted (skipped): {self.stats['already_converted_skipped']}")
        print(f"   🔄 Format converted: {self.stats['format_converted']}")
        print(f"   ↩️  OCR files moved to non_delivery: {self.stats['ocr_files_moved']}")
        print(f"   ⏱️  Running time: {elapsed_minutes:.1f} minutes")
        print(f"   📂 Watch: {self.watch_dir}")
        print(f"   📁 Output: {self.output_dir}")
        print(f"   🔄 Monitoring: {self.is_running}")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Focused Timezone Worker - ONLY matched_non_delivery")
    parser.add_argument('--process-existing', action='store_true',
                       help="Process existing files once")
    parser.add_argument('--stats', action='store_true',
                       help="Show stats and exit")
    
    args = parser.parse_args()
    
    # Create focused worker
    worker = FocusedTimezoneWorker()
    
    # Signal handler
    def signal_handler(signum, frame):
        print("\n🛑 Stopping focused timezone worker...")
        worker.print_stats()
        worker.stop_monitoring()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.stats:
            worker.print_stats()
            return 0
        
        if args.process_existing:
            processed_count = worker.process_existing_files()
            print(f"📊 Processed {processed_count} existing files")
            worker.print_stats()
            return 0
        
        # Process existing files first
        processed_count = worker.process_existing_files()
        print(f"📊 Processed {processed_count} existing files")
        
        # Start monitoring
        worker.start_monitoring()
        
        # Status loop
        print(f"\n💡 Press Ctrl+C to stop")
        print(f"🎯 Monitoring ONLY: {worker.watch_dir}")
        
        last_processed = worker.stats['processed']
        while worker.is_running:
            time.sleep(30)
            current = worker.stats['processed']
            
            if current != last_processed:
                print(f"📊 New files processed: +{current - last_processed} (Total: {current})")
                last_processed = current
            else:
                print(f"📊 Status: {current} files processed, monitoring...")
        
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt")
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        worker.stop_monitoring()
        worker.print_stats()
        print("🧹 Focused timezone worker stopped")
    
    return 0

if __name__ == "__main__":
    exit(main())