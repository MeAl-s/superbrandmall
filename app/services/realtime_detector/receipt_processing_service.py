# app/services/receipt_processing_service.py - Processing logic (extracted from your analyze and session functions)
from datetime import datetime
from typing import Set, List, Dict, Any

from app.config.settings import settings

class ReceiptProcessingService:
    """Handles receipt processing - refactored from your session tracking and analyze functions"""
    
    def __init__(self):
        # Global state tracking - exactly like your original
        self.session_numbers: Set[str] = set()
        self.session_stats = {
            "start_time": None,
            "total_fetches": 0,
            "total_new_receipts": 0,
            "last_fetch_time": None,
            "last_fetch_count": 0
        }
        self.initialize_session()
    
    def initialize_session(self):
        """Initialize session - exact logic from your initialize_session function"""
        self.session_numbers = set()  # Start fresh each session    
        print("🆕 Starting fresh session - no historical tracking")
        print("🔍 Will only detect duplicates within current session")
    
    def analyze_new_vs_duplicate(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze receipts - exact logic from your analyze_new_vs_duplicate function"""
        new_receipts = []
        duplicate_receipts = []
        no_number_receipts = []
        
        print(f"🔍 Analyzing {len(records)} fetched receipts...")
        print(f"📊 Previously seen: {len(self.session_numbers)} unique receipts")
        
        for record in records:
            number = record.get("number")
            
            if not number:
                no_number_receipts.append(record)
                continue
            
            number_str = str(number).strip()
            
            if number_str in self.session_numbers:
                # This is a duplicate within current session
                duplicate_receipts.append({
                    "record": record,
                    "number": number_str,
                    "status": "duplicate"
                })
            else:
                # This is NEW! Show details immediately - exactly like your original
                print(f"🎉 NEW RECEIPT DETECTED!")
                print(f"   📄 Number: {number_str}")
                print(f"   🏪 Shop: {record.get('shopName', 'Unknown')}")
                print(f"   🕐 Print Time: {record.get('printTime', 'Unknown')}")
                print(f"   💰 Total: {record.get('totalPrice', 'Unknown')}")
                
                new_receipts.append({
                    "record": record,
                    "number": number_str,
                    "status": "new"
                })
                # Add to session tracking
                self.session_numbers.add(number_str)
                print(f"   ✅ Added to session tracking (now tracking {len(self.session_numbers)} receipts)")
        
        if new_receipts:
            print(f"\n🎊 SUMMARY: Found {len(new_receipts)} NEW receipts!")
        else:
            print(f"🔄 No new receipts found - all {len(records)} were already seen")
        
        return {
            "new": new_receipts,
            "duplicates": duplicate_receipts,
            "no_number": no_number_receipts,
            "summary": {
                "total_fetched": len(records),
                "new_count": len(new_receipts),
                "duplicate_count": len(duplicate_receipts),
                "no_number_count": len(no_number_receipts)
            }
        }
    
    def print_realtime_status(self, analysis: Dict[str, Any], batch_number: int):
        """Print status - exact logic from your print_realtime_status function"""
        summary = analysis["summary"]
        
        print("\n" + "="*60)
        print(f"🔄 BATCH #{batch_number} - {datetime.now().strftime('%H:%M:%S')}")
        print("="*60)
        print(f"📥 Total fetched: {summary['total_fetched']}")
        print(f"🆕 New this batch: {summary['new_count']}")
        
        # Show new receipt numbers
        if analysis["new"]:
            print(f"\n🎉 NEW RECEIPTS DETECTED:")
            for item in analysis["new"][:5]:  # Show first 5
                record = item["record"]
                print(f"  📄 {item['number']} (Shop: {record.get('shopName', 'Unknown')})")
            
            if len(analysis["new"]) > 5:
                print(f"  ... and {len(analysis['new']) - 5} more")
        else:
            print(f"🔄 No new receipts this batch (all {summary['total_fetched']} already seen)")
        
        # Update session stats - exactly like your original
        self.session_stats["total_fetches"] += 1
        self.session_stats["total_new_receipts"] += summary["new_count"]
        self.session_stats["last_fetch_time"] = datetime.now().isoformat()
        self.session_stats["last_fetch_count"] = summary["total_fetched"]
        
        print(f"\n📊 SESSION STATS:")
        print(f"  🕐 Running since: {self.session_stats['start_time']}")
        print(f"  🔄 Total fetches: {self.session_stats['total_fetches']}")
        print(f"  🆕 Total new found: {self.session_stats['total_new_receipts']}")
        print(f"  📚 Unique receipts seen: {len(self.session_numbers)}")
        print(f"  📊 Last fetch: {summary['total_fetched']} receipts")
        
        print("="*60)
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get current session statistics"""
        return self.session_stats.copy()
    
    def reset_session(self):
        """Reset session data"""
        self.initialize_session()
        print("🔄 Session reset - starting fresh")
    
    def get_unique_receipt_count(self) -> int:
        """Get count of unique receipts seen this session"""
        return len(self.session_numbers)
    
    def set_session_start_time(self, start_time: str):
        """Set session start time - matches your session_stats setup"""
        self.session_stats["start_time"] = start_time
