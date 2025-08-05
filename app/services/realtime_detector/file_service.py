# app/services/file_service.py - Enhanced for continuous daily operation
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

from config.settings import settings

class FileService:
    """Handles file operations - enhanced for continuous daily operation"""
    
    def __init__(self):
        self.data_dir = settings.DATA_DIR  # Points to worker/data/real_time_response
        self._setup_data_folder()
    
    def _setup_data_folder(self):
        """Create data folder"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Data folder ready: {self.data_dir}")
    
    def save_realtime_batch(self, analysis: Dict[str, Any], batch_number: int) -> bool:
        """Save new receipts - enhanced for continuous daily operation"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Only save if there are NEW receipts
        if not analysis["new"]:
            print("📭 No new receipts to save")
            return False
        
        print(f"💾 Saving {len(analysis['new'])} new receipts...")
        
        # ENHANCED: Clean date-based file naming for continuous operation
        daily_filename = self.data_dir / f"receipts_{today}.json"
        
        # Load existing daily file or create new structure
        if daily_filename.exists():
            with open(daily_filename, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            print(f"📂 Loaded existing file for {today} with {len(existing_data.get('receipts', []))} receipts")
        else:
            existing_data = {
                "date": today,
                "created_at": datetime.now().isoformat(),
                "receipts": [],
                "daily_stats": {
                    "total_receipts": 0,
                    "first_receipt_time": None,
                    "last_receipt_time": None,
                    "unique_shops": []
                }
            }
            print(f"📄 Created new daily file for {today}")
        
        # Add new receipts to existing data
        existing_shops = set(existing_data["daily_stats"].get("unique_shops", []))
        
        for item in analysis["new"]:
            receipt_entry = {
                "detected_at": datetime.now().isoformat(),
                "receipt_number": item["number"],
                "shop_name": item["record"].get("shopName", "Unknown"),
                "print_time": item["record"].get("printTime", "Unknown"),
                "total_price": item["record"].get("totalPrice", "Unknown"),
                "record": item["record"]
            }
            existing_data["receipts"].append(receipt_entry)
            
            # Track unique shops
            if receipt_entry["shop_name"] != "Unknown":
                existing_shops.add(receipt_entry["shop_name"])
            
            print(f"   ✅ Saved: {item['number']} from {receipt_entry['shop_name']}")
        
        # Update daily statistics
        existing_data["last_updated"] = datetime.now().isoformat()
        existing_data["total_receipts"] = len(existing_data["receipts"])
        existing_data["daily_stats"]["total_receipts"] = len(existing_data["receipts"])
        existing_data["daily_stats"]["unique_shops"] = list(existing_shops)
        existing_data["daily_stats"]["unique_shop_count"] = len(existing_shops)
        
        # Set first and last receipt times
        if existing_data["receipts"]:
            existing_data["daily_stats"]["first_receipt_time"] = existing_data["receipts"][0]["detected_at"]
            existing_data["daily_stats"]["last_receipt_time"] = existing_data["receipts"][-1]["detected_at"]
        
        # Save updated file
        try:
            with open(daily_filename, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
            
            print(f"📝 Successfully saved to {daily_filename.name}")
            print(f"📊 Daily total for {today}: {existing_data['total_receipts']} receipts from {len(existing_shops)} shops")
            return True
            
        except Exception as e:
            print(f"❌ Error saving file: {e}")
            return False
    
    def get_today_summary(self) -> Optional[Dict[str, Any]]:
        """Get summary for today - enhanced for continuous operation"""
        today = datetime.now().strftime("%Y-%m-%d")
        daily_filename = self.data_dir / f"receipts_{today}.json"
        
        if daily_filename.exists():
            with open(daily_filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                total_receipts = data.get("total_receipts", 0)
                daily_stats = data.get("daily_stats", {})
                
                print(f"📊 DAILY SUMMARY ({today}):")
                print(f"  🆕 Total receipts: {total_receipts}")
                print(f"  🏪 Unique shops: {daily_stats.get('unique_shop_count', 0)}")
                
                if total_receipts > 0:
                    print(f"  🕐 First receipt: {daily_stats.get('first_receipt_time', 'Unknown')}")
                    print(f"  🕐 Last receipt: {daily_stats.get('last_receipt_time', 'Unknown')}")
                    print(f"  🕐 File created: {data.get('created_at', 'Unknown')}")
                    print(f"  🕐 Last updated: {data.get('last_updated', 'Unknown')}")
                
                return {
                    "date": today,
                    "total_receipts": total_receipts,
                    "daily_stats": daily_stats,
                    "created_at": data.get("created_at"),
                    "last_updated": data.get("last_updated")
                }
        
        print(f"📊 No receipts found for {today}")
        return None
    
    def get_date_summary(self, date_str: str) -> Optional[Dict[str, Any]]:
        """Get summary for any specific date"""
        daily_filename = self.data_dir / f"receipts_{date_str}.json"
        
        if daily_filename.exists():
            with open(daily_filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {
                    "date": date_str,
                    "total_receipts": data.get("total_receipts", 0),
                    "daily_stats": data.get("daily_stats", {}),
                    "created_at": data.get("created_at"),
                    "last_updated": data.get("last_updated")
                }
        
        return None
    
    def get_week_summary(self, days_back: int = 7) -> Dict[str, Any]:
        """Get summary for the last N days"""
        week_data = []
        total_week_receipts = 0
        unique_shops_week = set()
        
        for i in range(days_back):
            date = datetime.now() - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")
            
            day_summary = self.get_date_summary(date_str)
            if day_summary:
                week_data.append(day_summary)
                total_week_receipts += day_summary["total_receipts"]
                unique_shops_week.update(day_summary["daily_stats"].get("unique_shops", []))
        
        return {
            "period": f"Last {days_back} days",
            "total_receipts": total_week_receipts,
            "unique_shops": len(unique_shops_week),
            "daily_breakdown": week_data,
            "average_per_day": round(total_week_receipts / days_back, 2) if days_back > 0 else 0
        }
    
    def cleanup_old_files(self, days_to_keep: int = 30) -> int:
        """Clean up old receipt files - enhanced for continuous operation"""
        cleanup_date = datetime.now() - timedelta(days=days_to_keep)
        cleaned_count = 0
        
        print(f"🧹 Cleaning files older than {days_to_keep} days")
        
        # Clean both old and new format files
        patterns = ["receipts_*.json", "new_receipts_today_*.json"]
        
        for pattern in patterns:
            for file_path in self.data_dir.glob(pattern):
                try:
                    # Extract date from filename
                    if "receipts_" in file_path.name:
                        date_match = re.search(r'receipts_(\d{4}-\d{2}-\d{2})\.json', file_path.name)
                    else:
                        date_match = re.search(r'new_receipts_today_(\d{4}-\d{2}-\d{2})\.json', file_path.name)
                    
                    if date_match:
                        file_date_str = date_match.group(1)
                        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                        
                        if file_date < cleanup_date:
                            # Archive before deleting
                            archive_dir = self.data_dir / "archive"
                            archive_dir.mkdir(exist_ok=True)
                            
                            archive_file = archive_dir / file_path.name
                            file_path.rename(archive_file)
                            cleaned_count += 1
                            print(f"📦 Archived: {file_path.name}")
                            
                except Exception as e:
                    print(f"⚠️ Error processing {file_path.name}: {e}")
        
        return cleaned_count
    
    def get_data_directory(self) -> Path:
        """Get the data directory path"""
        return self.data_dir