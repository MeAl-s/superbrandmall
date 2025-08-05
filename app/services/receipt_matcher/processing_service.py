# app/services/receipt_matcher/processing_service.py - OPTIMIZED for speed
import time
import json
from datetime import datetime
from typing import Set, Dict, Any, List, Optional
from pathlib import Path

class ProcessingService:
    """Optimized receipt matcher processing logic - fast and simple"""
    
    def __init__(self):
        # Simplified tracking - no duplicate detection
        self.processed_files: Set[str] = set()
        self.stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_matched": 0,
            "total_unmatched": 0,
            "total_processed": 0,
            "files_without_numbers": 0,
            "total_files_in_directory": 0,
            "response_files_loaded": 0
        }
    
    def is_valid_source_file(self, file_path: Path) -> bool:
        """Quick validation - only check file extension"""
        valid_extensions = {'.jpg', '.jpeg', '.png', '.pdf', '.bin', '.bmp', '.json'}
        return file_path.suffix.lower() in valid_extensions
    
    def process_single_file(self, file_path: Path, response_map: Dict[str, Any], 
                           file_service, matching_service) -> Optional[Dict[str, Any]]:
        """Fast processing with detailed retry logging and move verification"""
        
        try:
            print(f"\n📄 PROCESSING: {file_path.name}")
            
            # Quick validation
            if not self.is_valid_source_file(file_path):
                print(f"    ❌ Invalid file type")
                return None
            
            # Extract number from filename (now handles _nondelivery_ suffix)
            number = file_service.extract_number_from_filename(file_path)
            if not number:
                print(f"    ❌ Could not extract number from filename")
                self.stats["files_without_numbers"] += 1
                return None
            
            print(f"    📝 Extracted number: '{number}'")
            if '_nondelivery_' in file_path.name:
                print(f"    🔧 Removed suffix from: '{file_path.stem}' → '{number}'")
            
            # Try matching with enhanced retry logic
            match = matching_service.find_matching_response(number, response_map, file_service)
            
            if match:
                response_data = match['data']
                receipt_fields = matching_service.extract_receipt_fields(response_data)
                
                # Quick validation
                if receipt_fields['number'] == 'unknown':
                    print(f"    ❌ Failed to extract receipt number from response data")
                    return None
                
                print(f"    🎯 FINAL MATCH: '{number}' → '{match['original_number']}'")
                print(f"       📊 Store: {receipt_fields.get('store_name', 'unknown')}")
                print(f"       💰 Amount: {receipt_fields.get('ticketAmount', 'unknown')}")
                
                # Add minimal metadata
                receipt_fields['_matched_by'] = 'receipt_matcher'
                receipt_fields['_matched_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                receipt_fields['_original_file'] = file_path.name
                
                # Save JSON first
                print(f"    💾 SAVING: Creating matched receipt JSON...")
                if file_service.save_individual_json(receipt_fields):
                    print(f"    ✅ JSON SAVED successfully")
                    
                    # Then move the source file
                    print(f"    📦 MOVING: Moving source file to processed...")
                    if file_service.move_to_processed(file_path):
                        print(f"    ✅ SUCCESS: File processed and moved successfully")
                        
                        # Double-check the file was actually moved
                        if not file_path.exists():
                            print(f"    ✅ VERIFIED: Source file no longer exists in original location")
                            return {'matched': True}
                        else:
                            print(f"    ⚠️  WARNING: Source file still exists after move operation!")
                            return {'matched': True}  # Still count as success since JSON was saved
                    else:
                        print(f"    ❌ MOVE FAILED: Could not move source file")
                        return {'matched': True}  # Still count as success since JSON was saved
                else:
                    print(f"    ❌ SAVE FAILED: Could not save JSON file")
                    return None
            else:
                print(f"    💥 UNMATCHED: Could not find any match for '{number}'")
            
            return None
                
        except Exception as e:
            print(f"    💥 ERROR: Exception during processing: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def fast_file_scan(self, file_service) -> List[Path]:
        """Fast scan - just get all files quickly"""
        source_dir = file_service.get_source_directory()
        
        if not source_dir.exists():
            return []
        
        # Simple file collection
        all_files = [f for f in source_dir.iterdir() if f.is_file()]
        self.stats["total_files_in_directory"] = len(all_files)
        
        print(f"Found {len(all_files)} files to process")
        return all_files
    
    def process_all_files_fast(self, response_map: Dict[str, Any], file_service, matching_service) -> int:
        """Fast processing of all files"""
        
        all_files = self.fast_file_scan(file_service)
        
        if not all_files:
            print("No files found to process")
            return 0
        
        print(f"Processing {len(all_files)} files...")
        
        matched_count = 0
        processed_count = 0
        
        for i, file_path in enumerate(all_files, 1):
            # Minimal progress reporting
            if i % 50 == 0 or i == len(all_files):
                print(f"Progress: {i}/{len(all_files)} ({(i/len(all_files)*100):.1f}%)")
            
            matched_data = self.process_single_file(file_path, response_map, file_service, matching_service)
            
            if matched_data and matched_data.get('matched'):
                matched_count += 1
                self.stats["total_matched"] += 1
            else:
                self.stats["total_unmatched"] += 1
            
            self.processed_files.add(str(file_path))
            processed_count += 1
            self.stats["total_processed"] += 1
        
        # Simple summary
        print(f"\nProcessing Complete:")
        print(f"  Total: {processed_count}")
        print(f"  Matched: {matched_count}")
        print(f"  Unmatched: {processed_count - matched_count}")
        
        if processed_count > 0:
            success_rate = (matched_count / processed_count) * 100
            print(f"  Success Rate: {success_rate:.1f}%")
        
        return processed_count
    
    def update_response_files_loaded(self, count: int):
        """Update response file count"""
        self.stats["response_files_loaded"] = count
    
    def get_processed_files(self) -> Set[str]:
        """Get processed files set"""
        return self.processed_files
    
    def print_simple_stats(self):
        """Simple stats output"""
        print(f"Session Stats:")
        print(f"  Running since: {self.stats['start_time']}")
        print(f"  Response files: {self.stats['response_files_loaded']}")
        print(f"  Files found: {self.stats['total_files_in_directory']}")
        print(f"  Processed: {self.stats['total_processed']}")
        print(f"  Matched: {self.stats['total_matched']}")
        print(f"  Unmatched: {self.stats['total_unmatched']}")


# app/services/receipt_matcher/matching_service.py - OPTIMIZED for speed
class MatchingService:
    """Fast receipt matching service"""
    
    def __init__(self):
        self.debug_mode = False  # Disable debug for speed
        
    def extract_receipt_fields(self, response_data: Dict[str, Any]) -> Dict[str, str]:
        """Fast field extraction - prioritize common fields"""
        fields = {
            'number': 'unknown',
            'store_name': 'unknown', 
            'store_id': 'unknown',
            'ticketAmount': 'unknown',
            'print_time': 'unknown'
        }
        
        # Handle different JSON structures quickly
        data_to_check = response_data
        if isinstance(response_data, dict) and 'record' in response_data:
            data_to_check = response_data['record']
        
        if isinstance(data_to_check, dict):
            # Fast number extraction - try most common fields first
            for field in ['number', 'receipt_number', 'receiptNumber', 'id']:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown']:
                    fields['number'] = str(data_to_check[field])
                    break
            
            # Fast store name extraction
            for field in ['shopName', 'store_name', 'storeName', 'merchant_name']:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown']:
                    fields['store_name'] = str(data_to_check[field])
                    break
            
            # Fast store ID extraction
            for field in ['shopCode', 'store_id', 'storeId', 'storeCode']:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown']:
                    fields['store_id'] = str(data_to_check[field])
                    break
            
            # Fast amount extraction
            for field in ['totalAmount', 'total_amount', 'total', 'amount', 'ticketAmount']:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown']:
                    amount_value = data_to_check[field]
                    if isinstance(amount_value, (int, float)):
                        fields['ticketAmount'] = str(amount_value)
                        break
                    elif isinstance(amount_value, str) and amount_value.strip():
                        fields['ticketAmount'] = amount_value.strip()
                        break
            
            # Fast time extraction
            for field in ['printTime', 'print_time', 'timestamp', 'dateTime', 'created_at']:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown']:
                    fields['print_time'] = str(data_to_check[field])
                    break
        
        return fields
    
    def find_matching_response(self, number: str, response_map: Dict[str, Any], file_service) -> Optional[Dict[str, Any]]:
        """Enhanced matching with detailed retry logging"""
        
        print(f"    🔍 MATCHING: Looking for '{number}' in {len(response_map)} variants...")
        
        # Strategy 1: Direct exact match (fastest)
        if number in response_map:
            print(f"    ✅ DIRECT MATCH: Found exact match for '{number}'")
            return response_map[number]
        
        print(f"    ❌ No direct match for '{number}'")
        
        # Strategy 2: Try filename variants
        print(f"    🔄 RETRY 1: Trying filename variants...")
        variants = file_service.create_filename_variants(number)
        print(f"       Generated {len(variants)} variants: {variants[:5]}{'...' if len(variants) > 5 else ''}")
        
        for i, variant in enumerate(variants):
            if variant in response_map:
                print(f"    ✅ VARIANT MATCH: Found match with variant #{i+1}: '{variant}'")
                return response_map[variant]
        
        print(f"    ❌ No variant matches found")
        
        # Strategy 3: Enhanced fuzzy matching for unmatched files
        print(f"    🔄 RETRY 2: Trying fuzzy matching (OCR corrections)...")
        fuzzy_matches = self._try_enhanced_fuzzy_matching(number, response_map)
        if fuzzy_matches:
            print(f"    ✅ FUZZY MATCH: Found fuzzy match: '{number}' → '{fuzzy_matches[0]}'")
            return response_map[fuzzy_matches[0]]
        
        print(f"    ❌ No fuzzy matches found")
        
        # Strategy 4: Pattern-based matching for complex numbers
        print(f"    🔄 RETRY 3: Trying pattern matching (numeric-only)...")
        pattern_matches = self._try_pattern_matching(number, response_map)
        if pattern_matches:
            print(f"    ✅ PATTERN MATCH: Found pattern match: '{number}' → '{pattern_matches[0]}'")
            return response_map[pattern_matches[0]]
        
        print(f"    ❌ No pattern matches found")
        
        # Strategy 5: Substring matching for partial receipt numbers
        print(f"    🔄 RETRY 4: Trying substring matching (partial)...")
        substring_matches = self._try_substring_matching(number, response_map)
        if substring_matches:
            print(f"    ✅ SUBSTRING MATCH: Found substring match: '{number}' → '{substring_matches[0]}'")
            return response_map[substring_matches[0]]
        
        print(f"    ❌ No substring matches found")
        print(f"    💥 FINAL RESULT: No matches found for '{number}' after all retry strategies")
        
        return None
    
    def _try_enhanced_fuzzy_matching(self, number: str, response_map: Dict[str, Any]) -> List[str]:
        """Enhanced fuzzy matching with detailed logging"""
        fuzzy_candidates = []
        
        print(f"       🔍 Fuzzy matching for '{number}' (length: {len(number)})")
        
        if len(number) < 6:
            print(f"       ⏭️  Skipping fuzzy matching - number too short")
            return []
        
        # Enhanced OCR substitutions
        ocr_substitutions = {
            '0': ['O', 'o', 'Q', 'D'],
            'O': ['0', 'o', 'Q', 'D'],
            'o': ['0', 'O', 'Q'],
            'Q': ['0', 'O', 'o'],
            'D': ['0', 'O'],
            '1': ['l', 'I', '|', 'i', '!'],
            'l': ['1', 'I', '|', 'i'],
            'I': ['1', 'l', '|', 'i'],
            'i': ['1', 'l', 'I', '|'],
            '5': ['S', 's'],
            'S': ['5', 's'],
            's': ['5', 'S'],
            '8': ['B', 'b'],
            'B': ['8', 'b'],
            'b': ['8', 'B'],
            '6': ['G', 'g'],
            'G': ['6', 'g'],
            'g': ['6', 'G'],
            '2': ['Z', 'z'],
            'Z': ['2', 'z'],
            'z': ['2', 'Z'],
            '9': ['g', 'q'],
            'g': ['9', 'q'],
            'q': ['9', 'g']
        }
        
        # Check first 100 keys for performance
        checked_count = 0
        for key in response_map.keys():
            if checked_count >= 100:
                break
            
            if self._is_enhanced_fuzzy_match(number, key, ocr_substitutions):
                print(f"       ✅ Found fuzzy match: '{number}' ≈ '{key}'")
                fuzzy_candidates.append(key)
                break
            
            checked_count += 1
        
        if not fuzzy_candidates:
            print(f"       ❌ No fuzzy matches found (checked {checked_count} keys)")
        
        return fuzzy_candidates
    
    def _try_pattern_matching(self, number: str, response_map: Dict[str, Any]) -> List[str]:
        """Try pattern-based matching with logging"""
        pattern_candidates = []
        
        # Extract numeric parts only
        numeric_only = ''.join(filter(str.isdigit, number))
        print(f"       🔍 Pattern matching - numeric only: '{numeric_only}' (length: {len(numeric_only)})")
        
        if len(numeric_only) < 6:
            print(f"       ⏭️  Skipping pattern matching - numeric part too short")
            return []
        
        checked_count = 0
        for key in response_map.keys():
            if checked_count >= 200:  # Check more keys for pattern matching
                break
                
            key_numeric = ''.join(filter(str.isdigit, key))
            
            # Check if numeric parts match exactly
            if numeric_only == key_numeric and len(numeric_only) >= 8:
                print(f"       ✅ Found pattern match: '{number}' (numeric: '{numeric_only}') = '{key}' (numeric: '{key_numeric}')")
                pattern_candidates.append(key)
                break
            
            checked_count += 1
        
        if not pattern_candidates:
            print(f"       ❌ No pattern matches found (checked {checked_count} keys)")
        
        return pattern_candidates
    
    def _try_substring_matching(self, number: str, response_map: Dict[str, Any]) -> List[str]:
        """Try substring matching with detailed logging"""
        substring_candidates = []
        
        print(f"       🔍 Substring matching for '{number}' (length: {len(number)})")
        
        if len(number) < 8:
            print(f"       ⏭️  Skipping substring matching - number too short")
            return []
        
        checked_count = 0
        for key in response_map.keys():
            if checked_count >= 150:  # Reasonable limit for substring checking
                break
                
            # Check if number is contained in key or vice versa
            if len(key) >= 8:
                if number in key:
                    print(f"       ✅ Found substring match: '{number}' IN '{key}'")
                    substring_candidates.append(key)
                    break
                elif key in number:
                    print(f"       ✅ Found substring match: '{key}' IN '{number}'")
                    substring_candidates.append(key)
                    break
                else:
                    # Check overlap percentage
                    overlap_len = len(set(number) & set(key))
                    min_length = min(len(number), len(key))
                    overlap_ratio = overlap_len / min_length
                    
                    if overlap_ratio >= 0.7:
                        print(f"       ✅ Found overlap match: '{number}' ↔ '{key}' (overlap: {overlap_ratio:.1%})")
                        substring_candidates.append(key)
                        break
            
            checked_count += 1
        
        if not substring_candidates:
            print(f"       ❌ No substring matches found (checked {checked_count} keys)")
        
        return substring_candidates
    
    def _is_enhanced_fuzzy_match(self, number1: str, number2: str, substitutions: Dict[str, List[str]]) -> bool:
        """Enhanced fuzzy matching with better tolerance"""
        if abs(len(number1) - len(number2)) > 3:
            return False
        
        if len(number1) < 6:
            return False
        
        differences = 0
        max_differences = max(2, len(number1) // 6)  # Allow more differences for longer numbers
        
        min_len = min(len(number1), len(number2))
        for i in range(min_len):
            char1 = number1[i]
            char2 = number2[i] if i < len(number2) else ''
            
            if char1 != char2:
                # Check if it's a known substitution
                if char2 in substitutions.get(char1, []):
                    continue
                else:
                    differences += 1
                    if differences > max_differences:
                        return False
        
        return True


# app/workers/receipt_matcher.py - FAST VERSION
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# Add directories to Python path  
current_dir = Path(__file__).parent  # app/workers/
app_dir = current_dir.parent         # app/
project_root = app_dir.parent        # Project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

# Import services
from config.settings import settings
from services.receipt_matcher.file_service import FileService
from services.receipt_matcher.matching_service import MatchingService

class FastReceiptMatcher:
    """Fast receipt matcher - optimized for speed"""
    
    def __init__(self):
        self.file_service = FileService()
        self.matching_service = MatchingService()
        self.processing_service = ProcessingService()
        self.is_running = False
        
        # Fast configuration
        self.last_reload_time = time.time()
        self.reload_interval = 300  # Reload every 5 minutes (less frequent)
        self.response_map = {}
        
    def initialize(self):
        """Quick initialization"""
        print("🚀 Initializing FAST Receipt Matcher...")
        print(f"📂 Source: {self.file_service.get_source_directory()}")
        print(f"💾 Output: {self.file_service.get_output_directory()}")
        
        # Load response files
        self.response_map = self.file_service.load_response_files()
        self.processing_service.update_response_files_loaded(len(self.response_map))
        
        if len(self.response_map) == 0:
            print("❌ No response files loaded!")
        else:
            print(f"✅ Loaded {len(self.response_map)} response variants")
        
    def process_all_files_fast(self):
        """Fast processing of all files"""
        print("\n🚀 FAST PROCESSING MODE")
        
        if len(self.response_map) == 0:
            print("❌ No response files available!")
            return 0
        
        return self.processing_service.process_all_files_fast(
            self.response_map, self.file_service, self.matching_service
        )
    
    def run_fast_monitor(self, check_interval: int = 30):
        """Fast real-time monitoring"""
        print(f"\n🚀 Fast Real-time Monitoring (every {check_interval}s)")
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping...")
            self.is_running = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        self.is_running = True
        
        try:
            while self.is_running:
                # Reload response files less frequently
                current_time = time.time()
                if current_time - self.last_reload_time > self.reload_interval:
                    print("🔄 Reloading response files...")
                    self.response_map = self.file_service.load_response_files()
                    self.processing_service.update_response_files_loaded(len(self.response_map))
                    self.last_reload_time = current_time
                
                # Get new files only
                all_files = self.processing_service.fast_file_scan(self.file_service)
                new_files = [f for f in all_files if str(f) not in self.processing_service.get_processed_files()]
                
                if new_files:
                    print(f"Processing {len(new_files)} new files...")
                    
                    matched_count = 0
                    for file_path in new_files:
                        matched_data = self.processing_service.process_single_file(
                            file_path, self.response_map, self.file_service, self.matching_service
                        )
                        
                        if matched_data and matched_data.get('matched'):
                            matched_count += 1
                            self.processing_service.stats["total_matched"] += 1
                        else:
                            self.processing_service.stats["total_unmatched"] += 1
                        
                        self.processing_service.processed_files.add(str(file_path))
                        self.processing_service.stats["total_processed"] += 1
                    
                    if matched_count > 0:
                        print(f"✅ Matched {matched_count}/{len(new_files)} files")
                else:
                    print("No new files")
                
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            raise

def main():
    """Main execution - simplified"""
    import argparse
    
    parser = argparse.ArgumentParser(description="FAST Receipt Matcher")
    parser.add_argument("--interval", type=int, default=30,
                       help="Check interval in seconds (default: 30)")
    parser.add_argument("--process-all", action="store_true",
                       help="Process all files once and exit")
    parser.add_argument("--monitor", action="store_true",
                       help="Start real-time monitoring")
    
    args = parser.parse_args()
    
    # Initialize matcher
    matcher = FastReceiptMatcher()
    matcher.initialize()
    
    if args.process_all:
        print("🚀 Processing all files...")
        total_processed = matcher.process_all_files_fast()
        print(f"✅ Processed {total_processed} files")
    elif args.monitor:
        print("🚀 Starting monitoring...")
        matcher.run_fast_monitor(args.interval)
    else:
        # Default: process all then monitor
        print("🚀 Processing existing files...")
        matcher.process_all_files_fast()
        print("🚀 Starting monitoring...")
        matcher.run_fast_monitor(args.interval)

if __name__ == "__main__":
    main()