# realtime_receipt_matcher.py - Simple receipt matcher creating individual JSON files
import os
import json
import re
import time
import signal
import sys
from pathlib import Path
from datetime import datetime

class RealtimeReceiptMatcher:
    def __init__(self, receipt_files_dir, response_files_dir, output_dir=None):
        """
        Initialize the Real-time Receipt Matcher
        
        Args:
            receipt_files_dir: Directory containing receipt .bin files from non_delivery
            response_files_dir: Directory containing JSON response files from real_time_response
            output_dir: Directory to save matched results (optional)
        """
        self.receipt_files_dir = Path(receipt_files_dir)
        self.response_files_dir = Path(response_files_dir)
        
        # Set output directory under worker/data
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = Path(__file__).parent / "data" / "matched_non_delivery"
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Output directory confirmed: {self.output_dir}")
        
        # Create a processed directory for matched files
        self.processed_dir = self.output_dir / "processed_files"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        print(f"📦 Processed directory confirmed: {self.processed_dir}")
        
        # Track processed files (session only)
        self.processed_files = set()
        
        # Updated encoding/decoding to match OCR processor
        self.ENCODING_MAP = {
            "/": "__SLASH__",
            ":": "__COLON__",
            "*": "__STAR__",
            "?": "__QUESTION__",
            '"': "__QUOTE__",
            "<": "__LT__",
            ">": "__GT__",
            "|": "__PIPE__",
            "\\": "__BACKSLASH__",
            " ": "__SPACE__",
            "#": "__HASH__"  # Added hash encoding
        }
        
        # Stats tracking
        self.stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_matched": 0,
            "total_unmatched": 0,
            "total_processed": 0,
            "session_files": 0,
            "response_files_loaded": 0
        }
        
        print(f"\n{'='*60}")
        print("🚀 SIMPLE REAL-TIME RECEIPT MATCHER INITIALIZED")
        print(f"{'='*60}")
        print(f"📁 Non-delivery files dir: {self.receipt_files_dir}")
        print(f"📂 Response files dir: {self.response_files_dir}")
        print(f"💾 Output dir: {self.output_dir}")
        print(f"📦 Processed files dir: {self.processed_dir}")
        print(f"🔧 Enhanced encoding for: {', '.join(self.ENCODING_MAP.keys())}")
        print(f"{'='*60}\n")
    
    def decode_unicode_markers(self, text):
        """Decode Unicode markers back to original characters"""
        import re
        # Pattern to match __U<number>__
        pattern = r'__U(\d+)__'
        
        def replace_unicode(match):
            try:
                unicode_value = int(match.group(1))
                return chr(unicode_value)
            except:
                return match.group(0)  # Return original if can't decode
        
        return re.sub(pattern, replace_unicode, text)
    
    def encode_filename(self, number):
        """Replace ALL forbidden characters with markers for Windows compatibility"""
        if not number:
            return number
        
        encoded = str(number)
        
        # Apply all encodings
        for char, marker in self.ENCODING_MAP.items():
            encoded = encoded.replace(char, marker)
        
        # Handle non-ASCII characters (Chinese, etc.)
        sanitized = ""
        for char in encoded:
            if ord(char) > 127:  # Non-ASCII character
                sanitized += f"__U{ord(char)}__"
            else:
                sanitized += char
        
        return sanitized
    
    def decode_filename(self, filename):
        """Convert back from markers to original characters"""
        # Remove extension first
        name_without_ext = os.path.splitext(filename)[0]
        
        decoded = name_without_ext
        
        # First decode Unicode markers
        decoded = self.decode_unicode_markers(decoded)
        
        # Then reverse all other encodings
        for char, marker in self.ENCODING_MAP.items():
            decoded = decoded.replace(marker, char)
        
        return decoded
    
    def fix_timestamp_format(self, number):
        """
        Fix various timestamp format issues
        """
        if not number or not isinstance(number, str):
            return number
        
        # Pattern 1: YYYY-MM-DDHH_MM_SS → YYYY-MM-DD HH:MM:SS
        pattern1 = r'(\d{4}-\d{2}-\d{2})(\d{2})_(\d{2})_(\d{2})'
        match1 = re.search(pattern1, number)
        if match1:
            fixed = number.replace(match1.group(0), f"{match1.group(1)} {match1.group(2)}:{match1.group(3)}:{match1.group(4)}")
            return fixed
        
        # Pattern 2: YYYY-MM-DDHH:MM:SS → YYYY-MM-DD HH:MM:SS (add space)
        pattern2 = r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})'
        match2 = re.search(pattern2, number)
        if match2:
            fixed = number.replace(match2.group(0), f"{match2.group(1)} {match2.group(2)}")
            return fixed
        
        return number
    
    def create_filename_variants(self, number):
        """
        Create multiple filename variants for a given number
        """
        if not number:
            return []
        
        variants = [number]  # Original
        
        # Fix timestamp first
        fixed_timestamp = self.fix_timestamp_format(number)
        if fixed_timestamp != number:
            variants.append(fixed_timestamp)
        
        # Add encoded versions
        for variant in variants[:]:  # Copy to avoid modifying while iterating
            encoded = self.encode_filename(variant)
            if encoded not in variants:
                variants.append(encoded)
        
        # Add common patterns
        # Pattern: Remove spaces
        no_space = number.replace(" ", "")
        if no_space not in variants:
            variants.append(no_space)
        
        # Pattern: Underscore instead of space
        underscore_space = number.replace(" ", "_")
        if underscore_space not in variants:
            variants.append(underscore_space)
        
        return variants
    
    def extract_receipt_fields(self, response_data):
        """
        Extract key fields from response data
        """
        fields = {
            'number': 'unknown',
            'store_name': 'unknown',
            'store_id': 'unknown',
            'ticketAmount': 'unknown',
            'print_time': 'unknown'
        }
        
        # Handle different JSON structures
        data_to_check = response_data
        
        # If there's a 'record' field, prioritize that
        if isinstance(response_data, dict) and 'record' in response_data:
            data_to_check = response_data['record']
        
        if isinstance(data_to_check, dict):
            # Extract number
            fields['number'] = str(data_to_check.get('number', 
                data_to_check.get('receipt_number', 
                data_to_check.get('receiptNumber', 'unknown'))))
            
            # Extract store information
            fields['store_name'] = data_to_check.get('shopName', 
                data_to_check.get('shop_name', 
                data_to_check.get('storeName', 
                data_to_check.get('store_name', 'unknown'))))
            
            fields['store_id'] = data_to_check.get('shopCode', 
                data_to_check.get('shop_code', 
                data_to_check.get('storeId', 
                data_to_check.get('store_id', 'unknown'))))
            
            # Extract total amount
            fields['ticketAmount'] = data_to_check.get('totalAmount', 
                data_to_check.get('total_amount', 
                data_to_check.get('total', 
                data_to_check.get('amount', 
                data_to_check.get('grandTotal', 
                data_to_check.get('grand_total', 
                data_to_check.get('ticketAmount', 'unknown')))))))
            
            # Extract print time
            fields['print_time'] = data_to_check.get('printTime', 
                data_to_check.get('print_time', 
                data_to_check.get('timestamp', 
                data_to_check.get('dateTime', 
                data_to_check.get('date_time', 
                data_to_check.get('created_at', 'unknown'))))))
        
        return fields
    
    def save_individual_json(self, receipt_data):
        """
        Save individual JSON file named by receipt number
        """
        try:
            number = receipt_data['number']
            if number == 'unknown':
                print(f"    ⚠️  Cannot save: receipt number is unknown")
                return False
            
            # Ensure output directory exists
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create safe filename
            safe_filename = self.encode_filename(number)
            json_file = self.output_dir / f"{safe_filename}.json"
            
            # Check if file already exists
            if json_file.exists():
                print(f"    ⚠️  File {safe_filename}.json already exists")
                return False
            
            # Save the data
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(receipt_data, f, ensure_ascii=False, indent=2)
            
            print(f"    💾 Saved: {safe_filename}.json")
            print(f"    📁 Full path: {json_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving individual JSON: {e}")
            print(f"    📁 Tried to save to: {self.output_dir}")
            print(f"    📄 Filename: {safe_filename}.json")
            return False
    
    def load_response_files(self, force_reload=False):
        """
        Load all JSON files from real_time_response directory and create a mapping
        """
        print(f"\n📂 Checking response directory: {self.response_files_dir}")
        
        if not self.response_files_dir.exists():
            print(f"❌ Response directory {self.response_files_dir} does not exist!")
            return {}
        
        response_map = {}
        json_files = list(self.response_files_dir.glob("*.json"))
        
        print(f"📁 Found {len(json_files)} JSON files in {self.response_files_dir}")
        
        if len(json_files) == 0:
            print(f"⚠️  No JSON files found!")
            return {}
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Extract numbers from the JSON data based on structure
                    numbers_found = []
                    
                    # Case 1: Direct 'number' field at top level
                    if isinstance(data, dict) and 'number' in data:
                        numbers_found.append(str(data['number']))
                        
                    # Case 2: new_receipts array with receipt_number field
                    elif isinstance(data, dict) and 'new_receipts' in data:
                        new_receipts = data.get('new_receipts', [])
                        if isinstance(new_receipts, list):
                            for receipt in new_receipts:
                                if isinstance(receipt, dict):
                                    # Check for receipt_number field
                                    if 'receipt_number' in receipt:
                                        numbers_found.append(str(receipt['receipt_number']))
                                    # Also check for 'number' field in receipt
                                    elif 'number' in receipt:
                                        numbers_found.append(str(receipt['number']))
                                    # Check in nested 'record' if exists
                                    elif 'record' in receipt and isinstance(receipt['record'], dict):
                                        record = receipt['record']
                                        if 'number' in record:
                                            numbers_found.append(str(record['number']))
                    
                    # Store all found numbers in the response map
                    if numbers_found:
                        for number in numbers_found:
                            # Create all variants of this number
                            variants = self.create_filename_variants(number)
                            
                            # Store the response data for all variants
                            for variant in variants:
                                # Find the specific receipt data for this number
                                receipt_data = data
                                if 'new_receipts' in data:
                                    # Find the specific receipt in the array
                                    for receipt in data['new_receipts']:
                                        if receipt.get('receipt_number') == number:
                                            receipt_data = receipt
                                            break
                                
                                response_map[variant] = {
                                    'original_number': number,
                                    'json_file': json_file.name,
                                    'data': receipt_data
                                }
                        
                        self.stats["response_files_loaded"] += 1
                        
            except Exception as e:
                print(f"⚠️  Error loading {json_file.name}: {str(e)}")
        
        print(f"✅ Loaded {self.stats['response_files_loaded']} response files")
        print(f"📊 Total number variants in mapping: {len(response_map)}")
        
        return response_map
    
    def extract_number_from_filename(self, file_path):
        """
        Extract number from the filename itself
        """
        # Get filename without extension
        filename = file_path.stem
        
        # Decode the filename to get original number
        original_number = self.decode_filename(filename)
        
        return original_number
    
    def process_single_file(self, file_path, response_map):
        """
        Process a single receipt file and match with response data
        """
        try:
            # Extract number from filename
            number = self.extract_number_from_filename(file_path)
            
            if not number:
                print(f"⚠️  {file_path.name} - No number extracted from filename")
                return None
            
            # Show decoded number if it was encoded
            has_special_chars = any(marker in file_path.stem for marker in self.ENCODING_MAP.values())
            has_unicode = "__U" in file_path.stem
            if has_special_chars or has_unicode:
                print(f"📝 {file_path.name} → Decoded to: {number}")
            
            # Find matching response data
            match = None
            matched_variant = None
            
            # Try direct match first
            if number in response_map:
                match = response_map[number]
                matched_variant = number
            else:
                # Try other variants if direct match fails
                variants = self.create_filename_variants(number)
                for variant in variants:
                    if variant in response_map:
                        match = response_map[variant]
                        matched_variant = variant
                        break
            
            if match:
                response_data = match['data']
                
                # Extract all required fields
                receipt_fields = self.extract_receipt_fields(response_data)
                
                print(f"✅ {file_path.name} - Matched!")
                print(f"    📊 Receipt Number: {number}")
                print(f"    🔗 Response Number: {match['original_number']}")
                print(f"    🏪 Store: {receipt_fields['store_name']} ({receipt_fields['store_id']})")
                print(f"    💰 Ticket Amount: {receipt_fields['ticketAmount']}")
                print(f"    🕐 Print Time: {receipt_fields['print_time']}")
                print(f"    📄 Response file: {match['json_file']}")
                
                # Save individual JSON file
                self.save_individual_json(receipt_fields)
                
                # Move the matched file to processed directory
                self.move_to_processed(file_path)
                
                return {'matched': True}
            else:
                print(f"❌ {file_path.name} - No match found for number: {number}")
                return None
                
        except Exception as e:
            print(f"❌ {file_path.name} - Error: {str(e)}")
            return None
    
    def move_to_processed(self, file_path):
        """
        Move matched file to processed directory
        """
        try:
            # Ensure processed directory exists
            self.processed_dir.mkdir(parents=True, exist_ok=True)
            
            # Create destination path
            dest_path = self.processed_dir / file_path.name
            
            # If destination already exists, add timestamp to avoid overwriting
            if dest_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name_parts = dest_path.stem, dest_path.suffix
                dest_path = self.processed_dir / f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            
            # Move the file
            import shutil
            shutil.move(str(file_path), str(dest_path))
            print(f"    📦 Moved to: processed_files/{dest_path.name}")
            
        except Exception as e:
            print(f"    ⚠️  Could not move file: {e}")
            print(f"    📁 Source: {file_path}")
            print(f"    📁 Destination: {dest_path}")
            print(f"    📁 Processed dir: {self.processed_dir}")
            # Try to create the processed directory again
            try:
                self.processed_dir.mkdir(parents=True, exist_ok=True)
                print(f"    ✅ Created processed directory: {self.processed_dir}")
            except Exception as e2:
                print(f"    ❌ Failed to create processed directory: {e2}")
    
    def scan_for_new_files(self):
        """
        Scan for new receipt files that haven't been processed yet
        """
        if not self.receipt_files_dir.exists():
            print(f"❌ Directory {self.receipt_files_dir} does not exist!")
            return []
        
        # Get all receipt files (jpg, pdf, png, bin, json)
        all_files = []
        for ext in ['*.jpg', '*.pdf', '*.png', '*.bin', '*.json']:
            all_files.extend(self.receipt_files_dir.glob(ext))
        
        # Filter out already processed files
        new_files = [f for f in all_files if str(f) not in self.processed_files]
        
        return new_files
    
    def process_new_files(self, response_map):
        """
        Process any new files found
        """
        new_files = self.scan_for_new_files()
        
        if not new_files:
            return 0
        
        print(f"\n🎯 Found {len(new_files)} new receipt files to process...")
        
        matched_count = 0
        unmatched_count = 0
        
        for file_path in new_files:
            matched_data = self.process_single_file(file_path, response_map)
            
            if matched_data:
                matched_count += 1
                self.stats["total_matched"] += 1
            else:
                unmatched_count += 1
                self.stats["total_unmatched"] += 1
            
            # Mark as processed
            self.processed_files.add(str(file_path))
            self.stats["total_processed"] += 1
        
        if matched_count > 0 or unmatched_count > 0:
            print(f"\n📊 Batch Summary:")
            print(f"    ✅ Matched: {matched_count}")
            print(f"    ❌ Unmatched: {unmatched_count}")
        
        return len(new_files)
    
    def run_realtime_monitor(self, check_interval=15):
        """
        Main real-time monitoring loop
        """
        print(f"\n🚀 Starting Simple Real-time Receipt Matcher")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📁 Monitoring: {self.receipt_files_dir}")
        print(f"📂 Matching with: {self.response_files_dir}")
        print(f"💾 Saving individual JSON files to: {self.output_dir}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        def signal_handler(sig, frame):
            print(f"\n\n🛑 Stopping receipt matcher...")
            self.print_final_stats()
            print(f"🗑️ Session data cleared. Goodbye!")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        # Initial load of response files
        response_map = self.load_response_files()
        last_reload_time = time.time()
        reload_interval = 300  # Reload response files every 5 minutes
        
        try:
            while True:
                # Check if we need to reload response files
                current_time = time.time()
                if current_time - last_reload_time > reload_interval:
                    print(f"\n🔄 Reloading response files...")
                    self.stats["response_files_loaded"] = 0
                    response_map = self.load_response_files()
                    last_reload_time = current_time
                
                print(f"\n🔍 Scanning for new receipt files... {datetime.now().strftime('%H:%M:%S')}")
                
                # Process any new files
                processed_count = self.process_new_files(response_map)
                
                if processed_count == 0:
                    print("📭 No new receipt files to process")
                
                # Update session stats
                self.stats["session_files"] = len(self.processed_files)
                
                # Print current stats
                print(f"\n📊 Session Stats:")
                print(f"    🕐 Running since: {self.stats['start_time']}")
                print(f"    📂 Response files loaded: {self.stats['response_files_loaded']}")
                print(f"    🔄 Total processed: {self.stats['total_processed']}")
                print(f"    ✅ Total matched: {self.stats['total_matched']}")
                print(f"    ❌ Total unmatched: {self.stats['total_unmatched']}")
                print(f"    📋 Session files: {self.stats['session_files']}")
                
                match_rate = 0
                if self.stats['total_processed'] > 0:
                    match_rate = (self.stats['total_matched'] / self.stats['total_processed']) * 100
                    print(f"    📈 Match rate: {match_rate:.1f}%")
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def print_final_stats(self):
        """
        Print final statistics when stopping
        """
        print(f"\n{'='*60}")
        print("FINAL SESSION STATISTICS")
        print(f"{'='*60}")
        print(f"🕐 Session duration: {self.stats['start_time']} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📂 Response files loaded: {self.stats['response_files_loaded']}")
        print(f"✅ Total matched: {self.stats['total_matched']}")
        print(f"❌ Total unmatched: {self.stats['total_unmatched']}")
        print(f"📊 Total processed: {self.stats['total_processed']}")
        
        if self.stats['total_processed'] > 0:
            match_rate = (self.stats['total_matched'] / self.stats['total_processed']) * 100
            print(f"📈 Overall match rate: {match_rate:.1f}%")
        
        print(f"📁 Individual JSON files saved in: {self.output_dir}")
        print(f"{'='*60}")

def main():
    """
    Main function to run the simple real-time receipt matcher
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple Real-time Receipt Matcher")
    parser.add_argument("--interval", type=int, default=15,
                       help="Check interval in seconds (default: 15)")
    parser.add_argument("--receipt-dir", type=str,
                       default=r"C:\Point Detection\worker\data\non_delivery",
                       help="Directory containing receipt files")
    parser.add_argument("--response-dir", type=str,
                       default=r"C:\Point Detection\worker\data\real_time_response",
                       help="Directory containing response JSON files")
    parser.add_argument("--output-dir", type=str,
                       default=None,
                       help="Output directory for matched results")
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🚀 SIMPLE REAL-TIME RECEIPT MATCHER")
    print("="*60)
    
    # Create matcher instance
    matcher = RealtimeReceiptMatcher(
        receipt_files_dir=args.receipt_dir,
        response_files_dir=args.response_dir,
        output_dir=args.output_dir
    )
    
    # Start real-time monitoring
    matcher.run_realtime_monitor(check_interval=args.interval)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES - SIMPLE RECEIPT MATCHER
# ═══════════════════════════════════════════════════════════════

"""
SIMPLE REAL-TIME RECEIPT MATCHER

Creates individual JSON files named by receipt number with extracted information.

USAGE:
python realtime_receipt_matcher.py

FOLDER STRUCTURE:
C:\\Point Detection\\worker\\data\\
├── non_delivery/                       ← Monitors files here
│   ├── YY02-20250723-0027.bin
│   └── CN021059202507220002.json
├── real_time_response/                 ← Reads JSON files from here
│   └── response_files.json
└── matched_non_delivery/               ← Output directory
    ├── YY02-20250723-0027.json        ← Individual JSON files
    ├── CN021059202507220002.json       ← Named by receipt number
    └── processed_files/                ← Original files moved here
        ├── YY02-20250723-0027.bin
        └── CN021059202507220002.json

INDIVIDUAL JSON FILE FORMAT (YY02-20250723-0027.json):
{
  "number": "YY02-20250723-0027",
  "store_name": "费大厨",
  "store_id": "304553",
  "ticketAmount": "unknown",
  "print_time": "2025-07-23 14:17:46"
}

WORKFLOW:
1. Receipt file: non_delivery/YY02-20250723-0027.bin
2. Gets matched with response JSON
3. Creates: matched_non_delivery/YY02-20250723-0027.json
4. Moves original to: processed_files/YY02-20250723-0027.bin

FEATURES:
✅ Individual JSON files named by receipt number
✅ Simple extracted data: number, store_name, store_id, ticketAmount, print_time
✅ Handles encoding/decoding for special characters
✅ Real-time monitoring
✅ No complex folder structures
✅ Easy to process individual files

RUN COMMAND:
python realtime_receipt_matcher.py
"""