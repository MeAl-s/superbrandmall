# app/services/ocr_downloader/file_service.py - OCR downloader file operations
import sys
from pathlib import Path
import re
from datetime import datetime
from typing import List, Optional, Tuple
from urllib.parse import urlparse, unquote

# Add project root to path for imports
current_file = Path(__file__)  # file_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings

class FileService:
    """Handles OCR downloader file operations - extracted from your realtime_downloader.py"""
    
    def __init__(self):
        # Base directories - ALL inside worker/data/
        self.source_dir = settings.WORKER_DIR / "data" / "receipt_ocring"     # worker/data/receipt_ocring/
        self.download_dir = settings.WORKER_DIR / "data" / "downloaded_receipts"  # worker/data/downloaded_receipts/
        self.today = datetime.now().strftime("%Y-%m-%d")
        
        # Today's specific directories
        self.today_source_dir = self.source_dir / self.today     # worker/data/receipt_ocring/2025-07-23/
        self.today_download_dir = self.download_dir / self.today # worker/data/downloaded_receipts/2025-07-23/
        
        self._setup_folders()
    
    def _setup_folders(self):
        """Create download folders - exact logic from your setup_download_folders"""
        # Create base directories
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Create today's directories
        self.today_download_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Download folders ready:")
        print(f"   📂 Source: {self.today_source_dir}")
        print(f"   💾 Downloads: {self.today_download_dir}")
    
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
    
    def extract_url_from_file(self, file_path: Path) -> Optional[str]:
        """Extract URL from file - EXACT logic from your extract_url_from_file function"""
        try:
            # Read file content
            txt = file_path.read_bytes().decode("utf-8", errors="ignore")
            
            # Look for URL pattern in data field
            m = re.search(r'"data"\s*:\s*"(?P<url>https?://[^"]+)"', txt)
            if m:
                return m.group("url")
            
            # Alternative pattern - direct URL search
            m = re.search(r'https://hddc01\.superbrandmall\.com:443/[^\s<>"{}|\\^`\[\]]+', txt)
            if m:
                return m.group(0)
            
            return None
            
        except Exception as e:
            print(f"    ❌ Error reading file {file_path.name}: {e}")
            return None
    
    def prepare_download_filename(self, source_filename: str, content_type: str, url: str) -> Tuple[str, Path]:
        """Prepare download filename - EXACT logic from your download_file_with_session function"""
        # Use the source filename as base filename
        base_name = Path(source_filename).stem  # Remove extension to get the base name
        
        # Map content types to extensions - exactly like your original
        extension_map = {
            'image/jpeg': '.jpg',
            'image/jpg': '.jpg', 
            'image/png': '.png',
            'image/gif': '.gif',
            'image/bmp': '.bmp',
            'image/tiff': '.tiff',
            'image/webp': '.webp',
            'application/pdf': '.pdf'
        }
        
        # Get extension from content type
        extension = extension_map.get(content_type.lower(), '')
        
        # If no extension from content type, try to get from URL
        if not extension:
            url_path = urlparse(unquote(url)).path
            url_extension = Path(url_path).suffix.lower()
            if url_extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.pdf']:
                extension = url_extension
            else:
                # Default to .jpg for images - exactly like your original
                extension = '.jpg'
        
        # Create final filename
        filename = base_name + extension
        dest_path = self.today_download_dir / filename
        
        return filename, dest_path
    
    def file_already_exists(self, dest_path: Path) -> bool:
        """Check if file already exists"""
        return dest_path.exists()
    
    def save_downloaded_content(self, dest_path: Path, response) -> bool:
        """Save downloaded content to file"""
        try:
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(8192):
                    f.write(chunk)
            return True
        except Exception as e:
            print(f"    ❌ Error saving file: {e}")
            return False
    
    def get_all_files_in_source(self) -> List[Path]:
        """Get all files in today's source directory for manual download"""
        if not self.today_source_dir.exists():
            return []
        
        return [f for f in self.today_source_dir.iterdir() if f.is_file()]
    
    def get_download_summary(self) -> dict:
        """Get summary of downloaded files"""
        download_count = len(list(self.today_download_dir.glob("*"))) if self.today_download_dir.exists() else 0
        
        return {
            "date": self.today,
            "downloaded_files": download_count,
            "download_dir": str(self.today_download_dir),
            "source_dir": str(self.today_source_dir)
        }
    
    def get_source_directory(self) -> Path:
        """Get today's source directory"""
        return self.today_source_dir
    
    def get_download_directory(self) -> Path:
        """Get today's download directory"""
        return self.today_download_dir