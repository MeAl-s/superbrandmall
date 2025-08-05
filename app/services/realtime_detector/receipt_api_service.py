# app/services/realtime_detector/receipt_api_service.py
from datetime import datetime
from typing import List, Dict, Any
import requests

from config.settings import settings
from app.services.auth_service import AuthenticationService

class ReceiptAPIService:
    """Handles API communication - refactored from your fetch_recent_receipts function"""
    
    def __init__(self):
        self.auth_service = AuthenticationService()
    
    def fetch_recent_receipts(self) -> List[Dict[str, Any]]:
        """Fetch TODAY's receipts - exact logic from your original function"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        now = datetime.utcnow()
        
        # ALWAYS start from today 00:00:00 - exactly like your original
        start_ts = f"{today} 00:00:00"
        end_ts = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # Your original print statements
        print(f"🗓️ Fetching TODAY's receipts ONLY: {today}")
        print(f"🔍 From {start_ts} to {end_ts}")
        print(f"⚠️  NEVER fetching historical data - TODAY ONLY!")
        
        all_records = []
        start = 0
        page_size = settings.realtime_detector.page_size
        
        session = self.auth_service.get_session()
        
        while True:
            batch_records = self._fetch_receipt_batch(
                session, start_ts, end_ts, start, page_size
            )
            
            if not batch_records:
                break
                
            all_records.extend(batch_records)
            
            if len(batch_records) < page_size:
                break
                
            start += page_size
        
        return all_records
    
    def _fetch_receipt_batch(
        self, 
        session: requests.Session,
        start_ts: str, 
        end_ts: str, 
        start: int, 
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch a single batch - exact logic from your original while loop"""
        try:
            # Build URL with timestamp - exactly like your original
            ts = int(datetime.utcnow().timestamp() * 1000)
            url = f"{settings.RECEIPT_API}?_dc={ts}"
            
            # Build payload - exactly like your original
            payload = {
                "filters": [
                    {"property": "printTimeFrom:>=", "value": start_ts},
                    {"property": "printTimeTo:<=", "value": end_ts},
                    {"property": "shopUuid:=", "value": ""}
                ],
                "sorters": [{"property": "printTime", "direction": "DESC"}],
                "start": start,
                "limit": limit
            }
            
            # Make request - exactly like your original
            r = session.post(
                url, 
                json=payload, 
                headers={"Accept": "application/json"}, 
                timeout=settings.realtime_detector.request_timeout
            )
            r.raise_for_status()
            
            data = r.json().get("data", [])
            return data
            
        except Exception as e:
            # Your original error message format
            print(f"❌ Error fetching batch: {e}")
            return []
    
    def test_connection(self) -> bool:
        """Test API connection"""
        try:
            session = self.auth_service.get_session()
            
            ts = int(datetime.utcnow().timestamp() * 1000)
            url = f"{settings.RECEIPT_API}?_dc={ts}"
            
            payload = {
                "filters": [],
                "sorters": [{"property": "printTime", "direction": "DESC"}],
                "start": 0,
                "limit": 1
            }
            
            response = session.post(
                url, 
                json=payload, 
                headers={"Accept": "application/json"}, 
                timeout=5
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
