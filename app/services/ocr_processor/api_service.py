# app/services/ocr_processor/api_service.py - OCR API service (MATCHES YOUR WORKING DEMO)
import sys
from pathlib import Path
from datetime import datetime
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
from app.services.auth_service import auth_service  # Use the singleton instance

class APIService:
    """Handles receipt file download API calls - EXACT logic from your working demo"""
    
    def __init__(self):
        self.auth_service = auth_service  # Use singleton
    
    def download_receipt_file(self, feature_code: str) -> Tuple[Optional[bytes], Optional[str]]:
        """Download receipt file - EXACT logic from your working demo"""
        session = self.auth_service.get_session()
        
        # Same parameters as your demo
        ts = int(datetime.utcnow().timestamp() * 1000)
        params = {
            "_dc": ts,
            "featureCode": feature_code,
            "deleteSpecialCharacter": "true"
        }
        
        try:
            # Same request as your demo
            resp = session.get(
                settings.FILE_API,  # Now points to correct endpoint!
                params=params,
                stream=True,
                timeout=15  # Same timeout as demo
            )
            resp.raise_for_status()
            
            # Same content type handling as demo
            content_type = resp.headers.get("Content-Type", "").split(";")[0]
            
            # Same file reading logic as demo
            file_content = b""
            for chunk in resp.iter_content(8192):
                file_content += chunk
            
            return file_content, content_type
            
        except Exception as e:
            print(f"    ❌ Error downloading {feature_code}: {e}")
            return None, None
    
    def test_connection(self) -> bool:
        """Test API connection"""
        try:
            session = self.auth_service.get_session()
            
            # Simple test request with correct endpoint
            ts = int(datetime.utcnow().timestamp() * 1000)
            params = {"_dc": ts, "featureCode": "test", "deleteSpecialCharacter": "true"}
            
            response = session.get(
                settings.FILE_API,
                params=params,
                timeout=5
            )
            
            # Even a 400/404 response means the API is reachable
            return response.status_code in [200, 400, 404]
            
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False