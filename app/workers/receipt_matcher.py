# app/workers/receipt_matcher.py - FIXED comprehensive receipt matcher
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
from services.receipt_matcher.file_service import FileService
from services.receipt_matcher.matching_service import MatchingService

class ProcessingService:
    """Enhanced receipt matcher processing logic - ensures EVERY file is processed"""
    
    def __init__(self):
        # Enhanced tracking with more detailed statistics
        self.processed_files: set = set()  # Track processed files (session only)
        self.all_files_seen: set = set()   # Track ALL files we've seen
        self.stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_matched": 0,
            "total_unmatched": 0,
            "total_processed": 0,
            "session_files": 0,
            "response_files_loaded": 0,
            "already_matched_skipped": 0,  # Files already processed
            "invalid_files_skipped": 0,    # Invalid source files
            "files_without_numbers": 0,    # Files with no extractable numbers
            "total_files_in_directory": 0  # Total files found in scan
        }
    
    def comprehensive_file_scan(self, file_service) -> list:
        """Comprehensive scan to find ALL files in non_delivery directory"""
        
        source_dir = file_service.get_source_directory()
        print(f"\n🔍 COMPREHENSIVE SCAN: {source_dir}")
        
        if not source_dir.exists():
            print(f"   ❌ Directory does not exist: {source_dir}")
            return []
        
        # Get ALL files (not just specific extensions)
        all_files = []
        for item in source_dir.iterdir():
            if item.is_file():
                all_files.append(item)
        
        print(f"   📊 TOTAL FILES FOUND: {len(all_files)}")
        
        # Group by extension for analysis
        extensions = {}
        for file_path in all_files:
            ext = file_path.suffix.lower()
            if ext not in extensions:
                extensions[ext] = 0
            extensions[ext] += 1
        
        print(f"   📋 FILE TYPES:")
        for ext, count in sorted(extensions.items()):
            print(f"      {ext or '(no extension)'}: {count} files")
        
        self.stats["total_files_in_directory"] = len(all_files)
        return all_files
    
    def is_valid_receipt_file(self, file_path: Path) -> bool:
        """Check if this is a valid receipt file to process"""
        
        # Accept common receipt file formats
        valid_extensions = {'.jpg', '.jpeg', '.png', '.pdf', '.bin', '.bmp', '.json'}
        if file_path.suffix.lower() not in valid_extensions:
            return False
        
        # For JSON files, do basic validation
        if file_path.suffix.lower() == '.json':
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Skip obvious OCR files
                if 'ocr_metadata' in data:
                    print(f"   🚨 SKIPPING OCR FILE: {file_path.name}")
                    return False
                
                # Skip already processed files
                if all(field in data for field in ['number', 'store_name', 'ticketAmount']) and data.get('number') != 'unknown':
                    print(f"   ⏭️  SKIPPING PROCESSED FILE: {file_path.name}")
                    return False
                    
            except Exception:
                # If we can't read the JSON, still try to process it
                pass
        
        return True
    
    def process_single_file(self, file_path: Path, response_map: dict, file_service, matching_service):
        """Process a single receipt file with enhanced logging"""
        try:
            print(f"\n📄 PROCESSING: {file_path.name}")
            
            # Validate file type first
            if not self.is_valid_receipt_file(file_path):
                print(f"   ⏭️  SKIPPED: Not a valid receipt file")
                self.stats["invalid_files_skipped"] += 1
                return None
            
            # Extract number from filename
            number = file_service.extract_number_from_filename(file_path)
            
            if not number:
                print(f"   ⚠️  NO NUMBER: Could not extract receipt number from filename")
                self.stats["files_without_numbers"] += 1
                return None
            
            print(f"   🔢 EXTRACTED NUMBER: '{number}'")
            
            # Find matching response data
            print(f"   🔍 SEARCHING: Looking for matches in {len(response_map)} response variants...")
            match = matching_service.find_matching_response(number, response_map, file_service)
            
            if match:
                response_data = match['data']
                
                # Extract all required fields
                receipt_fields = matching_service.extract_receipt_fields(response_data)
                
                # Validate extracted data
                if receipt_fields['number'] == 'unknown':
                    print(f"   ❌ EXTRACTION FAILED: Could not extract receipt number from response")
                    return None
                
                print(f"   ✅ MATCH FOUND!")
                print(f"      📊 Receipt Number: {number}")
                print(f"      🔗 Response Number: {match['original_number']}")
                print(f"      🏪 Store: {receipt_fields['store_name']} ({receipt_fields['store_id']})")
                print(f"      💰 Ticket Amount: {receipt_fields['ticketAmount']}")
                print(f"      🕐 Print Time: {receipt_fields['print_time']}")
                print(f"      📄 Response file: {match['json_file']}")
                
                # Add metadata to track this came from receipt matcher
                receipt_fields['_matched_by'] = 'receipt_matcher'
                receipt_fields['_matched_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                receipt_fields['_original_file'] = file_path.name
                
                # Save individual JSON file
                print(f"   💾 SAVING: Creating matched receipt JSON...")
                if file_service.save_individual_json(receipt_fields):
                    # Move the matched file to processed directory
                    print(f"   📦 MOVING: Moving source file to processed...")
                    file_service.move_to_processed(file_path)
                    print(f"   ✅ SUCCESS: File fully processed!")
                    return {'matched': True}
                else:
                    print(f"   ❌ SAVE FAILED: Could not save matched data")
                    return None
            else:
                print(f"   ❌ NO MATCH: No response found for number '{number}'")
                return None
                
        except Exception as e:
            print(f"   💥 ERROR: Exception during processing: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def process_all_files_comprehensive(self, response_map: dict, file_service, matching_service) -> int:
        """Process ALL files found with comprehensive logging"""
        
        # Get comprehensive file list
        all_files = self.comprehensive_file_scan(file_service)
        
        if not all_files:
            print("📭 NO FILES: No files found to process")
            return 0
        
        print(f"\n🎯 PROCESSING ALL FILES: {len(all_files)} files found")
        print("="*80)
        
        matched_count = 0
        unmatched_count = 0
        skipped_count = 0
        error_count = 0
        
        for i, file_path in enumerate(all_files, 1):
            print(f"\n📄 [{i}/{len(all_files)}] FILE: {file_path.name}")
            
            # Track that we've seen this file
            self.all_files_seen.add(str(file_path))
            
            try:
                matched_data = self.process_single_file(file_path, response_map, file_service, matching_service)
                
                if matched_data and matched_data.get('matched'):
                    matched_count += 1
                    self.stats["total_matched"] += 1
                    print(f"   🟢 RESULT: MATCHED")
                elif self.stats["invalid_files_skipped"] > 0 or self.stats["files_without_numbers"] > 0:
                    skipped_count += 1
                    print(f"   🟡 RESULT: SKIPPED")
                else:
                    unmatched_count += 1
                    self.stats["total_unmatched"] += 1
                    print(f"   🔴 RESULT: UNMATCHED")
                
            except Exception as e:
                error_count += 1
                print(f"   💥 RESULT: ERROR - {e}")
            
            # Mark as processed
            self.processed_files.add(str(file_path))
            self.stats["total_processed"] += 1
        
        # Print comprehensive summary
        print(f"\n" + "="*80)
        print(f"COMPREHENSIVE PROCESSING SUMMARY")
        print(f"="*80)
        print(f"📊 TOTAL FILES FOUND: {len(all_files)}")
        print(f"✅ MATCHED: {matched_count}")
        print(f"❌ UNMATCHED: {unmatched_count}")
        print(f"⏭️  SKIPPED: {skipped_count}")
        print(f"💥 ERRORS: {error_count}")
        print(f"📋 PROCESSED: {len(self.processed_files)}")
        
        if len(all_files) > 0:
            success_rate = (matched_count / len(all_files)) * 100
            print(f"📈 SUCCESS RATE: {success_rate:.1f}%")
        
        return len(all_files)
    
    def update_response_files_loaded(self, count: int):
        """Update the count of response files loaded"""
        self.stats["response_files_loaded"] = count
    
    def get_processed_files(self) -> set:
        """Get processed files set"""
        return self.processed_files
    
    def print_session_stats(self):
        """Print enhanced session statistics"""
        print(f"\n📊 ENHANCED SESSION STATS:")
        print(f"   🕐 Running since: {self.stats['start_time']}")
        print(f"   📂 Response files loaded: {self.stats['response_files_loaded']}")
        print(f"   📁 Total files in directory: {self.stats['total_files_in_directory']}")
        print(f"   🔄 Total processed: {self.stats['total_processed']}")
        print(f"   ✅ Total matched: {self.stats['total_matched']}")
        print(f"   ❌ Total unmatched: {self.stats['total_unmatched']}")
        print(f"   ⏭️  Invalid files skipped: {self.stats['invalid_files_skipped']}")
        print(f"   🔢 Files without numbers: {self.stats['files_without_numbers']}")
        
        if self.stats['total_processed'] > 0:
            match_rate = (self.stats['total_matched'] / self.stats['total_processed']) * 100
            print(f"   📈 Match rate: {match_rate:.1f}%")
    
    def print_final_stats(self, file_service):
        """Print final comprehensive statistics"""
        print(f"\n{'='*60}")
        print("COMPREHENSIVE FINAL STATISTICS")
        print(f"{'='*60}")
        print(f"🕐 Session: {self.stats['start_time']} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📂 Response files loaded: {self.stats['response_files_loaded']}")
        print(f"📁 Total files found: {self.stats['total_files_in_directory']}")
        print(f"✅ Total matched: {self.stats['total_matched']}")
        print(f"❌ Total unmatched: {self.stats['total_unmatched']}")
        print(f"⏭️  Invalid files skipped: {self.stats['invalid_files_skipped']}")
        print(f"📊 Total processed: {self.stats['total_processed']}")
        print(f"🔢 Files without numbers: {self.stats['files_without_numbers']}")
        
        if self.stats['total_processed'] > 0:
            match_rate = (self.stats['total_matched'] / self.stats['total_processed']) * 100
            print(f"📈 Overall match rate: {match_rate:.1f}%")
        
        if self.stats['total_files_in_directory'] > 0:
            processing_rate = (self.stats['total_processed'] / self.stats['total_files_in_directory']) * 100
            print(f"🎯 Processing coverage: {processing_rate:.1f}%")
        
        print(f"📁 Matched files saved in: {file_service.get_output_directory()}")
        print(f"{'='*60}")

class ComprehensiveReceiptMatcher:
    """Enhanced receipt matcher that processes EVERY single file"""
    
    def __init__(self):
        self.file_service = FileService()
        self.matching_service = MatchingService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
        # Enhanced timing configuration
        self.last_reload_time = time.time()
        self.reload_interval = 180  # Reload response files every 3 minutes
        self.response_map = {}
        
    def initialize(self):
        """Initialize the comprehensive receipt matcher"""
        print("🔧 Initializing COMPREHENSIVE Receipt Matcher...")
        print(f"📂 Monitoring: {self.file_service.get_source_directory()}")
        print(f"📂 Response files: {self.file_service.response_files_dir}")
        print(f"💾 Output: {self.file_service.get_output_directory()}")
        print(f"📦 Processed: {self.file_service.today_processed_dir}")
        print(f"🗓️ Processing files for: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"🎯 GOAL: Process EVERY SINGLE FILE in non_delivery")
        print("="*60)
        
        # Initial load of response files
        self.response_map = self.file_service.load_response_files()
        self.processing_service.update_response_files_loaded(len(self.response_map))
        
        if len(self.response_map) == 0:
            print("❌ WARNING: No response files loaded! Matching will fail.")
            print(f"   Check directory: {self.file_service.response_files_dir}")
        else:
            print(f"✅ LOADED: {len(self.response_map)} response variants")
        
    def process_every_single_file(self):
        """Process EVERY SINGLE file in the non_delivery directory"""
        print("\n🎯 COMPREHENSIVE FILE PROCESSING - Every Single File!")
        print("="*80)
        
        # Load response files first
        self.response_map = self.file_service.load_response_files()
        self.processing_service.update_response_files_loaded(len(self.response_map))
        
        if len(self.response_map) == 0:
            print("❌ NO RESPONSE FILES: Cannot match without response data!")
            print(f"   Expected location: {self.file_service.response_files_dir}")
            return 0
        
        print(f"✅ LOADED: {len(self.response_map)} response variants")
        
        # Process ALL files comprehensively
        total_processed = self.processing_service.process_all_files_comprehensive(
            self.response_map, self.file_service, self.matching_service
        )
        
        # Print final comprehensive stats
        self.processing_service.print_final_stats(self.file_service)
        
        return total_processed
    
    def run_realtime_monitor_comprehensive(self, check_interval: int = 15):
        """Enhanced real-time monitoring that catches EVERY file"""
        print(f"\n🚀 Starting COMPREHENSIVE Real-time Receipt Matcher")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📁 Monitoring: {self.file_service.get_source_directory()}")
        print(f"📂 Matching with: {self.file_service.response_files_dir}")
        print(f"💾 Saving to: {self.file_service.get_output_directory()}")
        print(f"🎯 GOAL: Process EVERY SINGLE FILE")
        print("="*80)
        
        def signal_handler(sig, frame):
            print(f"\n\n🛑 Stopping comprehensive receipt matcher...")
            self.processing_service.print_final_stats(self.file_service)
            print(f"🗑️ Session data cleared. Goodbye!")
            self.is_running = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        self.is_running = True
        
        try:
            while self.is_running:
                # Reload response files periodically
                current_time = time.time()
                if current_time - self.last_reload_time > self.reload_interval:
                    print(f"\n🔄 Reloading response files...")
                    old_count = len(self.response_map)
                    self.response_map = self.file_service.load_response_files()
                    new_count = len(self.response_map)
                    
                    if new_count != old_count:
                        print(f"📊 Response map updated: {old_count} → {new_count} variants")
                    
                    self.processing_service.update_response_files_loaded(new_count)
                    self.last_reload_time = current_time
                
                print(f"\n🔍 COMPREHENSIVE SCAN... {datetime.now().strftime('%H:%M:%S')}")
                
                # Get ALL files in directory
                all_files = self.processing_service.comprehensive_file_scan(self.file_service)
                
                # Filter out already processed files
                new_files = [f for f in all_files if str(f) not in self.processing_service.get_processed_files()]
                
                if new_files:
                    print(f"\n🎯 Found {len(new_files)} NEW files to process...")
                    
                    matched_count = 0
                    unmatched_count = 0
                    
                    for i, file_path in enumerate(new_files, 1):
                        print(f"\n📄 [{i}/{len(new_files)}] PROCESSING: {file_path.name}")
                        
                        try:
                            matched_data = self.processing_service.process_single_file(
                                file_path, self.response_map, self.file_service, self.matching_service
                            )
                            
                            if matched_data and matched_data.get('matched'):
                                matched_count += 1
                                self.processing_service.stats["total_matched"] += 1
                                print(f"   🟢 RESULT: MATCHED")
                            else:
                                unmatched_count += 1
                                self.processing_service.stats["total_unmatched"] += 1
                                print(f"   🔴 RESULT: UNMATCHED")
                            
                        except Exception as e:
                            print(f"   💥 ERROR: {e}")
                        
                        # Mark as processed
                        self.processing_service.processed_files.add(str(file_path))
                        self.processing_service.stats["total_processed"] += 1
                    
                    if matched_count > 0 or unmatched_count > 0:
                        print(f"\n📊 Batch Results:")
                        print(f"    ✅ Matched: {matched_count}")
                        print(f"    ❌ Unmatched: {unmatched_count}")
                        
                        if matched_count > 0:
                            success_rate = (matched_count / (matched_count + unmatched_count)) * 100
                            print(f"    📈 Batch success rate: {success_rate:.1f}%")
                else:
                    print("📭 No new files to process")
                
                # Print session stats
                self.processing_service.print_session_stats()
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def get_comprehensive_summary(self):
        """Get comprehensive matching summary"""
        summary = self.file_service.get_matcher_summary()
        
        print(f"\n📅 COMPREHENSIVE MATCHING SUMMARY ({summary['date']}):")
        print(f"    📂 Source files: {summary['source_files']}")
        print(f"    ✅ Matched files: {summary['matched_files']}")
        print(f"    📦 Processed files: {summary['processed_files']}")
        print(f"    📁 Output folder: {summary['output_dir']}")
        print(f"    📁 Processed folder: {summary['processed_dir']}")
        
        # Additional comprehensive stats
        print(f"    🔍 Files tracked: {len(self.processing_service.processed_files)}")
        print(f"    👀 Files seen: {len(self.processing_service.all_files_seen)}")
        
        # Calculate match rate
        if summary['processed_files'] > 0:
            match_rate = (summary['matched_files'] / summary['processed_files']) * 100
            print(f"    📈 Match rate: {match_rate:.1f}%")
        
        return summary

def main():
    """Main execution function with comprehensive options"""
    import argparse
    
    parser = argparse.ArgumentParser(description="COMPREHENSIVE Receipt Matcher - Processes EVERY File")
    parser.add_argument("--interval", type=int, default=15,
                       help="Check interval in seconds (default: 15)")
    parser.add_argument("--process-all", action="store_true",
                       help="Process ALL files in non_delivery once and exit")
    parser.add_argument("--summary", action="store_true",
                       help="Show comprehensive matching summary and exit")
    parser.add_argument("--realtime", action="store_true",
                       help="Start comprehensive real-time monitoring")
    
    args = parser.parse_args()
    
    # Initialize comprehensive matcher
    matcher = ComprehensiveReceiptMatcher()
    matcher.initialize()
    
    if args.summary:
        matcher.get_comprehensive_summary()
        sys.exit(0)
    elif args.process_all:
        print("\n🎯 PROCESSING ALL FILES MODE")
        print("This will process EVERY SINGLE file in non_delivery folder")
        total_processed = matcher.process_every_single_file()
        print(f"\n✅ COMPLETE: Processed {total_processed} files")
    elif args.realtime:
        print("\n👁️ COMPREHENSIVE REAL-TIME MODE")
        print("This will monitor and process EVERY file as it appears")
        matcher.run_realtime_monitor_comprehensive(args.interval)
    else:
        # Default: Process all existing files first, then start monitoring
        print("\n🎯 COMPREHENSIVE MODE: Process existing + real-time monitoring")
        
        print("\n1️⃣ STEP 1: Processing all existing files...")
        matcher.process_every_single_file()
        
        print("\n2️⃣ STEP 2: Starting real-time monitoring...")
        matcher.run_realtime_monitor_comprehensive(args.interval)

if __name__ == "__main__":
    main()