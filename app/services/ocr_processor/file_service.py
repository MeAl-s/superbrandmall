# app/services/ocr_processor/file_service.py - OCR file operations (extracted from your ocr_processor.py)
import sys
from pathlib import Path
import json
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

# Add project root to path for imports
current_file = Path(__file__)  # file_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings

class FileService:
    """Handles OCR file operations - extracted from your ocr_processor.py"""
    
    def __init__(self):
        self.base_ocr_dir = settings.OCR_FILES_DIR
        self.monitor_dir = settings.OCR_MONITOR_DIR
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.today_ocr_dir = self.base_ocr_dir / self.today  # Today's date folder
        self._setup_folders()
    
    def _setup_folders(self):
        """Create necessary folders - with today's date organization"""
        self.base_ocr_dir.mkdir(parents=True, exist_ok=True)
        self.today_ocr_dir.mkdir(parents=True, exist_ok=True)  # Create today's folder
        print(f"📁 OCR base folder ready: {self.base_ocr_dir}")
        print(f"📅 Today's OCR folder ready: {self.today_ocr_dir}")
        print(f"📂 Monitoring JSON files in: {self.monitor_dir}")
    
    def get_today_ocr_directory(self) -> Path:
        """Get today's OCR directory - for easy access"""
        return self.today_ocr_dir
    
    def encode_filename(self, number: str) -> str:
        """Replace ALL forbidden characters - EXACT copy from your original code"""
        if not number:
            return number
        
        # Your original encoding map
        ENCODING_MAP = {
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
        
        encoded = str(number)
        changes_made = []
        
        # Apply all encodings - exactly like your original
        for char, marker in ENCODING_MAP.items():
            if char in encoded:
                encoded = encoded.replace(char, marker)
                changes_made.append(f"{char} → {marker}")
        
        # Handle non-ASCII characters (Chinese, etc.) - from your original
        encoded = self._sanitize_filename(encoded)
        
        # Log if encoding happened - exactly like your original
        if changes_made or encoded != str(number):
            print(f"    📝 Encoded: {number}")
            print(f"       → {encoded}")
            if changes_made:
                print(f"       Changes: {', '.join(changes_made)}")
        
        return encoded
    
    def _sanitize_filename(self, filename: str) -> str:
        """Remove or replace characters - EXACT copy from your original"""
        sanitized = ""
        for char in filename:
            if ord(char) > 127:  # Non-ASCII character
                sanitized += f"__U{ord(char)}__"
            else:
                sanitized += char
        return sanitized
    
    def fix_timestamp_format(self, number: str) -> str:
        """Fix various timestamp format issues - EXACT copy from your original"""
        if not number or not isinstance(number, str):
            return number
        
        # Pattern 1: YYYY-MM-DDHH_MM_SS → YYYY-MM-DD HH:MM:SS
        pattern1 = r'(\d{4}-\d{2}-\d{2})(\d{2})_(\d{2})_(\d{2})'
        match1 = re.search(pattern1, number)
        if match1:
            fixed = number.replace(match1.group(0), f"{match1.group(1)} {match1.group(2)}:{match1.group(3)}:{match1.group(4)}")
            print(f"    🕐 Fixed timestamp: {number} → {fixed}")
            return fixed
        
        # Pattern 2: YYYY-MM-DDHH:MM:SS → YYYY-MM-DD HH:MM:SS (add space)
        pattern2 = r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})'
        match2 = re.search(pattern2, number)
        if match2:
            fixed = number.replace(match2.group(0), f"{match2.group(1)} {match2.group(2)}")
            print(f"    🕐 Fixed timestamp: {number} → {fixed}")
            return fixed
        
        return number
    
    def prepare_filename(self, receipt_number: str, content_type: str) -> Tuple[str, Path]:
        """Prepare filename and path for saving - combines your original logic"""
        # Fix timestamp format first, then encode - exactly like your original
        fixed_number = self.fix_timestamp_format(str(receipt_number))
        filename = self.encode_filename(fixed_number)
        
        # Get file extension from content type - exactly like your original
        ext = settings.ocr_processor.content_types.get(content_type, ".bin")
        
        filepath = f"{filename}{ext}"
        full_path = self.ocr_files_dir / filepath
        
        return filepath, full_path
    
    def file_already_exists(self, full_path: Path) -> bool:
        """Check if file already exists - from your original logic"""
        return full_path.exists()
    
    def save_file_content(self, full_path: Path, file_content: bytes) -> bool:
        """Save file content - extracted from your original download function"""
        try:
            with open(full_path, "wb") as fw:
                fw.write(file_content)
            return True
        except Exception as e:
            print(f"    ❌ Error saving file: {e}")
            return False
    
    def find_todays_json_file(self) -> Optional[Path]:
        """Find today's receipts JSON file - handles both old and new formats"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Try new format first (from enhanced realtime detector)
        new_format_file = self.monitor_dir / f"receipts_{today}.json"
        if new_format_file.exists():
            return new_format_file
        
        # Fallback to old format - EXACT path from your original
        old_format_file = self.monitor_dir / f"new_receipts_today_{today}.json"
        if old_format_file.exists():
            return old_format_file
        
        print(f"📭 No JSON file found for today: {today}")
        return None
    
    def load_receipts_from_json(self) -> List[Dict[str, Any]]:
        """Load all receipts from today's JSON file - handles both formats"""
        json_file = self.find_todays_json_file()
        
        if not json_file:
            return []
        
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Handle both old and new format
            if "receipts" in data:  # New format (from enhanced realtime detector)
                receipts = data["receipts"]
            else:  # Old format - exactly like your original
                receipts = data.get("new_receipts", [])
            
            print(f"    📊 Found {len(receipts)} receipts in {json_file.name}")
            return receipts
            
        except Exception as e:
            print(f"❌ Error reading JSON file: {e}")
            return []
    
    def get_example_files(self, count: int = 5) -> List[str]:
        """Get example filenames for debugging - from today's folder"""
        example_files = list(self.today_ocr_dir.glob("*"))[-count:]
        return [file.name for file in example_files]
    
    def get_today_files_count(self) -> int:
        """Get count of files in today's folder"""
        return len(list(self.today_ocr_dir.glob("*")))
    
    def get_all_date_folders(self) -> List[str]:
        """Get all date folders for reporting"""
        date_folders = []
        for folder in self.base_ocr_dir.iterdir():
            if folder.is_dir() and folder.name.match(r'\d{4}-\d{2}-\d{2}'):
                date_folders.append(folder.name)
        return sorted(date_folders)
    
    def get_data_directory(self) -> Path:
        """Get today's OCR directory - updated to return today's folder"""
        return self.today_ocr_dir