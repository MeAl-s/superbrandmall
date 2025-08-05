# services/database.py
# Phase 4: Database Services for Receipt Management

import sys
import os
import json
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional, Dict
from datetime import datetime
import logging

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.receipt import Receipt
from schemas.receipt import ReceiptCreate, ReceiptResponse, ReceiptJsonFormat
from config.database import SessionLocal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReceiptDatabase:
    """Database service for receipt operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_receipt(self, receipt_data: ReceiptCreate) -> Receipt:
        """Insert receipt data into database"""
        try:
            db_receipt = Receipt(
                number=receipt_data.number,
                store_name=receipt_data.store_name,
                store_id=receipt_data.store_id,
                ticketAmount=receipt_data.ticketAmount,
                print_time=receipt_data.print_time,
                source_file=receipt_data.source_file,
                response_file=receipt_data.response_file,
                matched_at=datetime.now(),
                status="matched"
            )
            
            self.db.add(db_receipt)
            self.db.commit()
            self.db.refresh(db_receipt)
            
            logger.info(f"✅ Receipt {receipt_data.number} saved to database")
            return db_receipt
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Error saving receipt {receipt_data.number}: {e}")
            raise
    
    def get_receipt_by_number(self, receipt_number: str) -> Optional[Receipt]:
        """Get receipt by number"""
        return self.db.query(Receipt).filter(Receipt.number == receipt_number).first()
    
    def get_receipts_by_store(self, store_id: str, limit: int = 100) -> List[Receipt]:
        """Get receipts by store ID"""
        return (self.db.query(Receipt)
                .filter(Receipt.store_id == store_id)
                .order_by(Receipt.matched_at.desc())
                .limit(limit)
                .all())
    
    def get_all_receipts(self, skip: int = 0, limit: int = 100) -> List[Receipt]:
        """Get all receipts with pagination"""
        return (self.db.query(Receipt)
                .order_by(Receipt.matched_at.desc())
                .offset(skip)
                .limit(limit)
                .all())
    
    def get_receipts_count(self) -> int:
        """Get total count of receipts"""
        return self.db.query(Receipt).count()
    
    def get_store_receipt_numbers(self, store_id: str) -> List[str]:
        """Get just receipt numbers for a store (simple format)"""
        receipts = self.db.query(Receipt.number).filter(Receipt.store_id == store_id).all()
        return [receipt.number for receipt in receipts]
    
    def get_stores_summary(self) -> List[Dict]:
        """Get summary of all stores with receipt counts"""
        result = (self.db.query(
                    Receipt.store_id,
                    Receipt.store_name,
                    func.count(Receipt.id).label('receipt_count')
                )
                .group_by(Receipt.store_id, Receipt.store_name)
                .all())
        
        return [
            {
                "store_id": row.store_id,
                "store_name": row.store_name,
                "receipt_count": row.receipt_count
            }
            for row in result
        ]
    
    def update_receipt_status(self, receipt_number: str, status: str) -> Optional[Receipt]:
        """Update receipt status"""
        receipt = self.get_receipt_by_number(receipt_number)
        if receipt:
            receipt.status = status
            self.db.commit()
            self.db.refresh(receipt)
        return receipt
    
    def update_receipt(self, receipt_number: str, **update_data) -> Optional[Receipt]:
        """Update receipt with arbitrary fields"""
        receipt = self.get_receipt_by_number(receipt_number)
        if receipt:
            for field, value in update_data.items():
                if hasattr(receipt, field) and value is not None:
                    setattr(receipt, field, value)
            self.db.commit()
            self.db.refresh(receipt)
        return receipt
    
    def delete_receipt(self, receipt_number: str) -> bool:
        """Delete receipt by number"""
        receipt = self.get_receipt_by_number(receipt_number)
        if receipt:
            self.db.delete(receipt)
            self.db.commit()
            return True
        return False
    
    def process_json_file_to_db(self, json_file_path: str) -> bool:
        """Process individual JSON file and insert to database"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if receipt already exists
            existing = self.get_receipt_by_number(data.get('number'))
            if existing:
                logger.warning(f"⚠️  Receipt {data.get('number')} already exists in database")
                return False
            
            # Create receipt data - matching your exact JSON format
            receipt_data = ReceiptCreate(
                number=data.get('number', 'unknown'),
                store_name=data.get('store_name', 'unknown'),
                store_id=data.get('store_id', 'unknown'),
                ticketAmount=data.get('ticketAmount'),  # Keep as number, can be None
                print_time=data.get('print_time', 'unknown'),
                source_file=json_file_path,
                response_file='processed_from_json'
            )
            
            # Insert to database
            self.create_receipt(receipt_data)
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing JSON file {json_file_path}: {e}")
            return False
    
    def get_receipts_json_format(self, store_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get receipts in your exact JSON format"""
        if store_id:
            receipts = self.get_receipts_by_store(store_id, limit)
        else:
            receipts = self.get_all_receipts(0, limit)
        
        json_receipts = []
        for receipt in receipts:
            json_receipts.append({
                "number": receipt.number,
                "store_name": receipt.store_name,
                "store_id": receipt.store_id,
                "ticketAmount": receipt.ticketAmount,
                "print_time": receipt.print_time
            })
        
        return json_receipts


def test_database_service():
    """Test the database service with all operations"""
    print("🧪 Testing Database Service...")
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Initialize service
        receipt_service = ReceiptDatabase(db)
        
        # Test 1: Create new receipt
        print("\n1️⃣ Testing create_receipt...")
        test_receipt_data = ReceiptCreate(
            number="TEST-" + str(int(datetime.now().timestamp())),
            store_name="测试店铺",  # Test Chinese characters
            store_id="TEST001",
            ticketAmount=99.99,
            print_time="2025-07-23 15:30:00",
            source_file="test_create.json",
            response_file="test_response.json"
        )
        
        try:
            created_receipt = receipt_service.create_receipt(test_receipt_data)
            print(f"✅ Receipt created: {created_receipt.number}")
        except Exception as e:
            print(f"❌ Create failed: {e}")
            return False
        
        # Test 2: Get receipt by number
        print("\n2️⃣ Testing get_receipt_by_number...")
        found_receipt = receipt_service.get_receipt_by_number(created_receipt.number)
        if found_receipt:
            print(f"✅ Receipt found: {found_receipt.number} - {found_receipt.store_name}")
        else:
            print("❌ Receipt not found")
            return False
        
        # Test 3: Update receipt
        print("\n3️⃣ Testing update_receipt...")
        updated_receipt = receipt_service.update_receipt(
            created_receipt.number,
            ticketAmount=199.99,
            notes="Updated via test"
        )
        if updated_receipt and updated_receipt.ticketAmount == 199.99:
            print(f"✅ Receipt updated: ticketAmount = {updated_receipt.ticketAmount}")
        else:
            print("❌ Receipt update failed")
            return False
        
        # Test 4: Get receipts by store
        print("\n4️⃣ Testing get_receipts_by_store...")
        store_receipts = receipt_service.get_receipts_by_store("TEST001")
        print(f"✅ Found {len(store_receipts)} receipts for store TEST001")
        
        # Test 5: Get all receipts
        print("\n5️⃣ Testing get_all_receipts...")
        all_receipts = receipt_service.get_all_receipts(skip=0, limit=5)
        print(f"✅ Found {len(all_receipts)} receipts (limit 5)")
        
        # Test 6: Get receipts count
        print("\n6️⃣ Testing get_receipts_count...")
        total_count = receipt_service.get_receipts_count()
        print(f"✅ Total receipts in database: {total_count}")
        
        # Test 7: Get stores summary
        print("\n7️⃣ Testing get_stores_summary...")
        stores_summary = receipt_service.get_stores_summary()
        print(f"✅ Found {len(stores_summary)} stores:")
        for store in stores_summary[:3]:  # Show first 3
            print(f"   - {store['store_name']} ({store['store_id']}): {store['receipt_count']} receipts")
        
        # Test 8: Get receipts in JSON format
        print("\n8️⃣ Testing get_receipts_json_format...")
        json_receipts = receipt_service.get_receipts_json_format(limit=2)
        print(f"✅ Got {len(json_receipts)} receipts in JSON format:")
        if json_receipts:
            print(f"   Sample: {json_receipts[0]}")
        
        # Test 9: Get store receipt numbers (simple format)
        print("\n9️⃣ Testing get_store_receipt_numbers...")
        receipt_numbers = receipt_service.get_store_receipt_numbers("306862")  # Use existing store
        print(f"✅ Found {len(receipt_numbers)} receipt numbers for store 306862")
        if receipt_numbers:
            print(f"   Sample numbers: {receipt_numbers[:3]}")
        
        # Test 10: Process JSON file (simulate)
        print("\n🔟 Testing process_json_file_to_db...")
        # Create a temporary JSON file
        import tempfile
        test_json_data = {
            "number": "JSON-TEST-" + str(int(datetime.now().timestamp())),
            "store_name": "JSON测试店",
            "store_id": "JSON001",
            "ticketAmount": 333.33,
            "print_time": "2025-07-23 16:00:00"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(test_json_data, f, ensure_ascii=False)
            temp_file_path = f.name
        
        try:
            json_success = receipt_service.process_json_file_to_db(temp_file_path)
            if json_success:
                print(f"✅ JSON file processed successfully: {test_json_data['number']}")
            else:
                print("❌ JSON file processing failed")
                return False
        finally:
            # Clean up temp file
            os.unlink(temp_file_path)
        
        # Test 11: Delete receipt (clean up test data)
        print("\n🗑️ Testing delete_receipt (cleanup)...")
        delete_success = receipt_service.delete_receipt(created_receipt.number)
        if delete_success:
            print(f"✅ Test receipt deleted: {created_receipt.number}")
        else:
            print("❌ Delete failed")
        
        return True
        
    except Exception as e:
        print(f"❌ Database service test failed: {e}")
        return False
    finally:
        db.close()


def show_service_examples():
    """Show example usage of the database service"""
    print("\n📋 Database Service Examples:")
    print("=" * 50)
    
    print("\n🔸 Creating a receipt:")
    print("""
receipt_data = ReceiptCreate(
    number="11034250718000135",
    store_name="阳坊 涮肉",
    store_id="306862", 
    ticketAmount=547.0,
    print_time="2025-07-23 14:36:16"
)
receipt = receipt_service.create_receipt(receipt_data)
""")
    
    print("\n🔸 Getting receipts in JSON format:")
    print("""
json_receipts = receipt_service.get_receipts_json_format(store_id="306862")
# Returns: [{"number": "...", "store_name": "...", "ticketAmount": 547.0, ...}]
""")
    
    print("\n🔸 Getting store summary:")
    print("""
stores = receipt_service.get_stores_summary()
# Returns: [{"store_id": "306862", "store_name": "阳坊 涮肉", "receipt_count": 5}]
""")


def main():
    """Main function to test database services"""
    print("🚀 Phase 4: Database Services Test")
    print("=" * 50)
    
    # Show service examples
    show_service_examples()
    
    # Test the service
    service_works = test_database_service()
    
    if service_works:
        print("\n✅ Phase 4 completed successfully!")
        print("🎯 Ready for Phase 5: API Routes")
        return True
    else:
        print("\n❌ Database service testing failed!")
        return False


if __name__ == "__main__":
    main()