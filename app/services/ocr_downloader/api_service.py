# app/services/ocr_downloader/api_service.py - OCR downloader API service
import sys
from pathlib import Path
from typing import Tuple, Optional
import requests

# Add project root to path for imports
current_file = Path(__file__)  # api_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings
from services.auth_service import AuthenticationService

class APIService:
    """Handles file download API calls - extracted from your download_file_with_session function"""
    
    def __init__(self):
        self.auth_service = AuthenticationService()
    
    def detect_content_type(self, url: str) -> str:
        """Detect content type using HEAD request - EXACT logic from your original"""
        session = self.auth_service.get_session()
        
        try:
            # Make a HEAD request to get content type
            head_resp = session.head(url, timeout=10)
            content_type = head_resp.headers.get('Content-Type', '').lower()
            return content_type
            
        except Exception as e:
            print(f"    ⚠️ Could not detect file type: {e}, defaulting to image/jpeg")
            return 'image/jpeg'  # Default fallback
    
    def download_file_content(self, url: str) -> Tuple[Optional[requests.Response], Optional[str]]:
        """Download file content from URL - extracted from your download_file_with_session"""
        session = self.auth_service.get_session()
        
        try:
            # Download the actual file
            response = session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Get content type from response
            content_type = response.headers.get('Content-Type', 'image/jpeg').lower()
            
            return response, content_type
            
        except Exception as e:
            print(f"    ❌ Download failed: {e}")
            return None, None
    
    def test_url_accessibility(self, url: str) -> bool:
        """Test if URL is accessible"""
        session = self.auth_service.get_session()
        
        try:
            response = session.head(url, timeout=10)
            return response.status_code == 200
        except Exception:
            return False