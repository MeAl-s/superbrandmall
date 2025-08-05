# app/services/receipt_matcher/file_service.py - Receipt matcher file operations
import sys
from pathlib import Path
import json
import shutil
import os
import re  # FIXED: Added missing import
from datetime import datetime
from typing import List, Optional, Set, Dict, Any

# Add project root to path for imports
current_file = Path(__file__)  # file_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings

# ... rest of the file stays the same

class FileService:
    """Handles receipt matcher file operations - extracted from your realtime_receipt_matcher.py"""
    
    def __init__(self):
        # Base directories - ALL inside worker/data/ with date organization
        self.receipt_files_dir = settings.WORKER_DIR / "data" / "non_delivery"      # worker/data/non_delivery/
        self.response_files_dir = settings.WORKER_DIR / "data" / "real_time_response"  # worker/data/real_time_response/
        self.output_dir = settings.WORKER_DIR / "data" / "matched_non_delivery"     # worker/data/matched_non_delivery/
        self.today = datetime.now().strftime("%Y-%m-%d")
        
        # Today's specific directories
        self.today_receipt_dir = self.receipt_files_dir / self.today    # worker/data/non_delivery/2025-07-23/
        self.today_output_dir = self.output_dir / self.today            # worker/data/matched_non_delivery/2025-07-23/
        self.today_processed_dir = self.today_output_dir / "processed_files"  # processed files for today
        
        # Encoding map - exactly like your original
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
            "#": "__HASH__"
        }
        
        self._setup_folders()
    
    def _setup_folders(self):
        """Create necessary folders with date organization"""
        # Create base directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create today's directories
        self.today_output_dir.mkdir(parents=True, exist_ok=True)
        self.today_processed_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Receipt matcher folders ready:")
        print(f"   📂 Receipt files: {self.today_receipt_dir}")
        print(f"   📂 Response files: {self.response_files_dir}")
        print(f"   💾 Output: {self.today_output_dir}")
        print(f"   📦 Processed: {self.today_processed_dir}")
    
    def decode_unicode_markers(self, text: str) -> str:
        """Decode Unicode markers back to original characters - EXACT logic from your original"""
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
    
    def encode_filename(self, number: str) -> str:
        """Replace ALL forbidden characters - EXACT logic from your original"""
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
    
    def decode_filename(self, filename: str) -> str:
        """Convert back from markers to original characters - EXACT logic from your original"""
        # Remove extension first
        name_without_ext = os.path.splitext(filename)[0]
        
        decoded = name_without_ext
        
        # First decode Unicode markers
        decoded = self.decode_unicode_markers(decoded)
        
        # Then reverse all other encodings
        for char, marker in self.ENCODING_MAP.items():
            decoded = decoded.replace(marker, char)
        
        return decoded
    
    def fix_timestamp_format(self, number: str) -> str:
        """Fix various timestamp format issues - EXACT logic from your original"""
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
    
    def create_filename_variants(self, number: str) -> List[str]:
        """Create multiple filename variants - EXACT logic from your original"""
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
    
    def scan_for_new_files(self, processed_files: Set[str]) -> List[Path]:
        """Scan for new receipt files that haven't been processed yet"""
        if not self.today_receipt_dir.exists():
            return []
        
        # Get all receipt files (jpg, pdf, png, bin, json) - exactly like your original
        all_files = []
        for ext in ['*.jpg', '*.pdf', '*.png', '*.bin', '*.json']:
            all_files.extend(self.today_receipt_dir.glob(ext))
        
        # Filter out already processed files
        new_files = [f for f in all_files if str(f) not in processed_files]
        
        return new_files
    
    def save_individual_json(self, receipt_data: Dict[str, Any]) -> bool:
        """Save individual JSON file named by receipt number - EXACT logic from your original"""
        try:
            number = receipt_data['number']
            if number == 'unknown':
                print(f"    ⚠️  Cannot save: receipt number is unknown")
                return False
            
            # Ensure output directory exists
            self.today_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create safe filename
            safe_filename = self.encode_filename(number)
            json_file = self.today_output_dir / f"{safe_filename}.json"
            
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
            print(f"    📁 Tried to save to: {self.today_output_dir}")
            return False
    
    def move_to_processed(self, file_path: Path) -> bool:
        """Move matched file to processed directory - EXACT logic from your original"""
        try:
            # Ensure processed directory exists
            self.today_processed_dir.mkdir(parents=True, exist_ok=True)
            
            # Create destination path
            dest_path = self.today_processed_dir / file_path.name
            
            # If destination already exists, add timestamp to avoid overwriting
            if dest_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name_parts = dest_path.stem, dest_path.suffix
                dest_path = self.today_processed_dir / f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            
            # Move the file
            shutil.move(str(file_path), str(dest_path))
            print(f"    📦 Moved to: processed_files/{dest_path.name}")
            return True
            
        except Exception as e:
            print(f"    ⚠️  Could not move file: {e}")
            return False
    
    def extract_number_from_filename(self, file_path: Path) -> str:
        """Extract number from the filename itself - from your original"""
        # Get filename without extension
        filename = file_path.stem
        
        # Decode the filename to get original number
        original_number = self.decode_filename(filename)
        
        return original_number
    
    def load_response_files(self) -> Dict[str, Any]:
        """Load all JSON files from real_time_response directory - EXACT logic from your original"""
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
        
        response_files_loaded = 0
        
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
                    
                    # Case 3: Enhanced format - receipts array (from new realtime detector)
                    elif isinstance(data, dict) and 'receipts' in data:
                        receipts = data.get('receipts', [])
                        if isinstance(receipts, list):
                            for receipt in receipts:
                                if isinstance(receipt, dict):
                                    if 'receipt_number' in receipt:
                                        numbers_found.append(str(receipt['receipt_number']))
                                    elif 'number' in receipt:
                                        numbers_found.append(str(receipt['number']))
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
                                elif 'receipts' in data:
                                    # Find the specific receipt in the array
                                    for receipt in data['receipts']:
                                        if receipt.get('receipt_number') == number:
                                            receipt_data = receipt
                                            break
                                
                                response_map[variant] = {
                                    'original_number': number,
                                    'json_file': json_file.name,
                                    'data': receipt_data
                                }
                        
                        response_files_loaded += 1
                        
            except Exception as e:
                print(f"⚠️  Error loading {json_file.name}: {str(e)}")
        
        print(f"✅ Loaded {response_files_loaded} response files")
        print(f"📊 Total number variants in mapping: {len(response_map)}")
        
        return response_map
    
    def get_matcher_summary(self) -> dict:
        """Get summary of matching results"""
        output_count = len(list(self.today_output_dir.glob("*.json"))) if self.today_output_dir.exists() else 0
        processed_count = len(list(self.today_processed_dir.glob("*"))) if self.today_processed_dir.exists() else 0
        source_count = len(self.scan_for_new_files(set()))
        
        return {
            "date": self.today,
            "source_files": source_count,
            "matched_files": output_count,
            "processed_files": processed_count,
            "output_dir": str(self.today_output_dir),
            "processed_dir": str(self.today_processed_dir)
        }
    
    def get_source_directory(self) -> Path:
        """Get today's source directory"""
        return self.today_receipt_dir
    
    def get_output_directory(self) -> Path:
        """Get today's output directory"""
        return self.today_output_dir