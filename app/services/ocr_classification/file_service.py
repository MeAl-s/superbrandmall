# app/services/ocr_classification/file_service.py - OCR classification file operations
import sys
from pathlib import Path
import json
import shutil
from datetime import datetime
from typing import List, Optional, Tuple

# Add project root to path for imports
current_file = Path(__file__)  # file_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings

class FileService:
    """Handles OCR classification file operations - extracted from your realtime_classifier.py"""
    
    def __init__(self):
        # Base directories - ALL inside worker/data/
        self.source_dir = settings.OCR_FILES_DIR  # worker/data/receipt_files/
        self.url_dir = settings.WORKER_DIR / "data" / "receipt_ocring"      # worker/data/receipt_ocring/
        self.no_url_dir = settings.WORKER_DIR / "data" / "receipt_checked"  # worker/data/receipt_checked/
        self.today = datetime.now().strftime("%Y-%m-%d")
        
        # Today's specific directories
        self.today_source_dir = self.source_dir / self.today  # worker/data/receipt_files/2025-07-23/
        self.today_url_dir = self.url_dir / self.today        # worker/data/receipt_ocring/2025-07-23/
        self.today_no_url_dir = self.no_url_dir / self.today  # worker/data/receipt_checked/2025-07-23/
        
        self._setup_folders()
    
    def _setup_folders(self):
        """Create classification folders - exact logic from your setup_classification_folders"""
        # Create base directories
        self.url_dir.mkdir(parents=True, exist_ok=True)
        self.no_url_dir.mkdir(parents=True, exist_ok=True)
        
        # Create today's directories
        self.today_url_dir.mkdir(parents=True, exist_ok=True)
        self.today_no_url_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Classification folders ready:")
        print(f"   📂 Source: {self.today_source_dir}")
        print(f"   ✅ With URLs: {self.today_url_dir}")
        print(f"   📋 No URLs: {self.today_no_url_dir}")
    
    def scan_for_new_files(self, processed_files: set) -> List[Path]:
        """Scan source directory for new files - exact logic from your scan_for_new_files"""
        if not self.today_source_dir.exists():
            return []
        
        new_files = []
        
        # Get all files in today's source directory
        for file_path in self.today_source_dir.iterdir():
            if file_path.is_file():
                file_key = f"{file_path.name}_{file_path.stat().st_mtime}"
                
                # Check if we've already processed this file
                if file_key not in processed_files:
                    new_files.append(file_path)
                    processed_files.add(file_key)
        
        return new_files
    
    def classify_single_file(self, file_path: Path) -> Tuple[bool, Optional[str], Optional[str]]:
        """Classify a single file - EXACT logic from your classify_single_file function"""
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            has_url = False
            file_type = "unknown"
            
            # Try to parse as JSON first
            try:
                data = json.loads(content)
                file_type = "json"
                
                # Extract the data field if it exists
                if 'data' in data and data['data']:
                    data_content = str(data['data'])
                    has_url = self._contains_hddc01_url(data_content)
                else:
                    # If no data field, check entire JSON content
                    has_url = self._contains_hddc01_url(content)
                    
            except json.JSONDecodeError:
                # If not valid JSON, check raw content
                file_type = "non-json"
                has_url = self._contains_hddc01_url(content)
            
            # Determine destination and move file
            if has_url:
                destination = self.today_url_dir / file_path.name
                destination_type = "receipt_ocring"
            else:
                destination = self.today_no_url_dir / file_path.name
                destination_type = "receipt_checked"
            
            # Move the file
            shutil.move(str(file_path), str(destination))
            
            return True, destination_type, None
            
        except Exception as e:
            return False, None, str(e)
    
    def _contains_hddc01_url(self, text: str) -> bool:
        """Check if text contains hddc01 URL - exact logic from your contains_hddc01_url"""
        import re
        hddc01_pattern = r'https://hddc01\.superbrandmall\.com:443/[^\s<>"{}|\\^`\[\]]+'
        return bool(re.search(hddc01_pattern, text))
    
    def get_all_files_in_source(self) -> List[Path]:
        """Get all files in today's source directory for manual classification"""
        if not self.today_source_dir.exists():
            return []
        
        return [f for f in self.today_source_dir.iterdir() if f.is_file()]
    
    def get_classification_summary(self) -> dict:
        """Get summary of classified files"""
        url_count = len(list(self.today_url_dir.glob("*"))) if self.today_url_dir.exists() else 0
        no_url_count = len(list(self.today_no_url_dir.glob("*"))) if self.today_no_url_dir.exists() else 0
        
        return {
            "date": self.today,
            "files_with_urls": url_count,
            "files_without_urls": no_url_count,
            "total_classified": url_count + no_url_count,
            "url_dir": str(self.today_url_dir),
            "no_url_dir": str(self.today_no_url_dir)
        }
    
    def get_source_directory(self) -> Path:
        """Get today's source directory"""
        return self.today_source_dir