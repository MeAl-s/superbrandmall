# schemas/receipt.py
# Phase 3: Pydantic Schemas for Receipt API

import sys
import os
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Union
from datetime import datetime

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class ReceiptBase(BaseModel):
    """
    Base receipt schema matching your exact JSON format:
    {
      "number": "11034250718000135",
      "store_name": "阳坊 涮肉",
      "store_id": "306862",
      "ticketAmount": 547.0,
      "print_time": "2025-07-23 14:36:16"
    }
    """
    number: str = Field(..., description="Receipt number", example="11034250718000135")
    store_name: str = Field(..., description="Store name (supports Chinese)", example="阳坊 涮肉")
    store_id: str = Field(..., description="Store ID", example="306862")
    ticketAmount: Optional[Union[float, int]] = Field(None, description="Ticket amount as number", example=547.0)
    print_time: str = Field(..., description="Print time", example="2025-07-23 14:36:16")
    
    @validator('ticketAmount')
    def validate_ticket_amount(cls, v):
        """Ensure ticketAmount is a valid number or None"""
        if v is not None:
            if isinstance(v, (int, float)):
                return float(v)  # Convert to float for consistency
            else:
                raise ValueError('ticketAmount must be a number')
        return v
    
    @validator('number')
    def validate_number(cls, v):
        """Ensure receipt number is not empty"""
        if not v or not v.strip():
            raise ValueError('Receipt number cannot be empty')
        return v.strip()

class ReceiptCreate(ReceiptBase):
    """Schema for creating a new receipt"""
    source_file: Optional[str] = Field(None, description="Source file path")
    response_file: Optional[str] = Field(None, description="Response file path")
    
    class Config:
        schema_extra = {
            "example": {
                "number": "11034250718000135",
                "store_name": "阳坊 涮肉",
                "store_id": "306862",
                "ticketAmount": 547.0,
                "print_time": "2025-07-23 14:36:16",
                "source_file": "receipt_11034250718000135.json",
                "response_file": "response_11034250718000135.json"
            }
        }

class ReceiptUpdate(BaseModel):
    """Schema for updating an existing receipt"""
    store_name: Optional[str] = Field(None, description="Update store name")
    store_id: Optional[str] = Field(None, description="Update store ID")
    ticketAmount: Optional[Union[float, int]] = Field(None, description="Update ticket amount")
    print_time: Optional[str] = Field(None, description="Update print time")
    status: Optional[str] = Field(None, description="Update status")
    notes: Optional[str] = Field(None, description="Update notes")
    
    @validator('ticketAmount')
    def validate_ticket_amount(cls, v):
        """Ensure ticketAmount is a valid number or None"""
        if v is not None:
            if isinstance(v, (int, float)):
                return float(v)
            else:
                raise ValueError('ticketAmount must be a number')
        return v

class ReceiptResponse(ReceiptBase):
    """Schema for receipt response (includes database fields)"""
    id: int = Field(..., description="Database ID")
    matched_at: datetime = Field(..., description="When record was created")
    status: str = Field(..., description="Processing status")
    source_file: Optional[str] = Field(None, description="Source file path")
    response_file: Optional[str] = Field(None, description="Response file path")
    notes: Optional[str] = Field(None, description="Additional notes")
    
    class Config:
        from_attributes = True  # For SQLAlchemy model conversion
        schema_extra = {
            "example": {
                "id": 1,
                "number": "11034250718000135",
                "store_name": "阳坊 涮肉",
                "store_id": "306862",
                "ticketAmount": 547.0,
                "print_time": "2025-07-23 14:36:16",
                "matched_at": "2025-07-23T14:36:16.123456+00:00",
                "status": "matched",
                "source_file": "receipt_11034250718000135.json",
                "response_file": "response_11034250718000135.json",
                "notes": None
            }
        }

class ReceiptJsonFormat(BaseModel):
    """Exact format matching your original JSON structure (for API responses)"""
    number: str
    store_name: str
    store_id: str
    ticketAmount: Union[float, int]
    print_time: str
    
    class Config:
        schema_extra = {
            "example": {
                "number": "11034250718000135",
                "store_name": "阳坊 涮肉",
                "store_id": "306862",
                "ticketAmount": 547.0,
                "print_time": "2025-07-23 14:36:16"
            }
        }

class ReceiptListResponse(BaseModel):
    """Schema for paginated receipt list response"""
    receipts: List[ReceiptResponse]
    total: int = Field(..., description="Total number of receipts")
    page: int = Field(..., description="Current page number")
    per_page: int = Field(..., description="Records per page")
    
    class Config:
        schema_extra = {
            "example": {
                "receipts": [
                    {
                        "id": 1,
                        "number": "11034250718000135",
                        "store_name": "阳坊 涮肉",
                        "store_id": "306862",
                        "ticketAmount": 547.0,
                        "print_time": "2025-07-23 14:36:16",
                        "matched_at": "2025-07-23T14:36:16.123456+00:00",
                        "status": "matched",
                        "source_file": None,
                        "response_file": None,
                        "notes": None
                    }
                ],
                "total": 1,
                "page": 1,
                "per_page": 100
            }
        }

class StoreReceiptsResponse(BaseModel):
    """Schema for store-specific receipts response"""
    store_id: str
    store_name: str
    receipts: List[str] = Field(..., description="List of receipt numbers")
    total_receipts: int = Field(..., description="Total number of receipts for this store")
    
    class Config:
        schema_extra = {
            "example": {
                "store_id": "306862",
                "store_name": "阳坊 涮肉",
                "receipts": ["11034250718000135", "11034250718000136", "11034250718000137"],
                "total_receipts": 3
            }
        }

class BulkProcessResponse(BaseModel):
    """Schema for bulk processing response"""
    processed: int = Field(..., description="Number of successfully processed files")
    failed: int = Field(..., description="Number of failed files")
    total: int = Field(..., description="Total number of files")
    message: str = Field(..., description="Processing summary message")
    
    class Config:
        schema_extra = {
            "example": {
                "processed": 45,
                "failed": 2,
                "total": 47,
                "message": "Bulk processing completed: 45 successful, 2 failed"
            }
        }

def test_schemas():
    """Test all Pydantic schemas"""
    print("🧪 Testing Pydantic schemas...")
    
    # Test ReceiptBase
    print("\n1️⃣ Testing ReceiptBase...")
    try:
        receipt_base = ReceiptBase(
            number="11034250718000135",
            store_name="阳坊 涮肉",
            store_id="306862",
            ticketAmount=547.0,
            print_time="2025-07-23 14:36:16"
        )
        print(f"✅ ReceiptBase created: {receipt_base.dict()}")
    except Exception as e:
        print(f"❌ ReceiptBase failed: {e}")
        return False
    
    # Test ReceiptCreate
    print("\n2️⃣ Testing ReceiptCreate...")
    try:
        receipt_create = ReceiptCreate(
            number="11034250718000135",
            store_name="阳坊 涮肉",
            store_id="306862",
            ticketAmount=547.0,
            print_time="2025-07-23 14:36:16",
            source_file="test.json"
        )
        print(f"✅ ReceiptCreate created: {receipt_create.dict()}")
    except Exception as e:
        print(f"❌ ReceiptCreate failed: {e}")
        return False
    
    # Test validation
    print("\n3️⃣ Testing validation...")
    try:
        # Test empty number (should fail)
        try:
            ReceiptBase(
                number="",
                store_name="Test Store",
                store_id="123",
                ticketAmount=100.0,
                print_time="2025-07-23 14:36:16"
            )
            print("❌ Empty number validation failed - should have raised error")
            return False
        except ValueError:
            print("✅ Empty number validation works")
        
        # Test invalid ticketAmount (should fail)
        try:
            ReceiptBase(
                number="TEST123",
                store_name="Test Store",
                store_id="123",
                ticketAmount="invalid",  # String instead of number
                print_time="2025-07-23 14:36:16"
            )
            print("❌ Invalid ticketAmount validation failed - should have raised error")
            return False
        except ValueError:
            print("✅ Invalid ticketAmount validation works")
        
    except Exception as e:
        print(f"❌ Validation testing failed: {e}")
        return False
    
    # Test ReceiptJsonFormat
    print("\n4️⃣ Testing ReceiptJsonFormat...")
    try:
        json_format = ReceiptJsonFormat(
            number="11034250718000135",
            store_name="阳坊 涮肉",
            store_id="306862",
            ticketAmount=547.0,
            print_time="2025-07-23 14:36:16"
        )
        print(f"✅ ReceiptJsonFormat created: {json_format.dict()}")
    except Exception as e:
        print(f"❌ ReceiptJsonFormat failed: {e}")
        return False
    
    # Test with database model
    print("\n5️⃣ Testing database model integration...")
    try:
        from models.receipt import Receipt
        from config.database import SessionLocal
        
        db = SessionLocal()
        try:
            # Get existing receipt from database
            db_receipt = db.query(Receipt).filter(Receipt.number == "11034250718000135").first()
            
            if db_receipt:
                # Convert to Pydantic model
                receipt_response = ReceiptResponse.from_orm(db_receipt)
                print(f"✅ Database to Pydantic conversion works: {receipt_response.dict()}")
                
                # Convert to JSON format
                json_receipt = ReceiptJsonFormat(
                    number=db_receipt.number,
                    store_name=db_receipt.store_name,
                    store_id=db_receipt.store_id,
                    ticketAmount=db_receipt.ticketAmount,
                    print_time=db_receipt.print_time
                )
                print(f"✅ Database to JSON format works: {json_receipt.dict()}")
            else:
                print("⚠️  No database record found (run Phase 2 first)")
        finally:
            db.close()
            
    except Exception as e:
        print(f"❌ Database integration test failed: {e}")
        return False
    
    return True

def show_schema_examples():
    """Show example usage of all schemas"""
    print("\n📋 Schema Examples:")
    print("=" * 50)
    
    print("\n🔸 ReceiptCreate (for API input):")
    create_example = {
        "number": "11034250718000135",
        "store_name": "阳坊 涮肉",
        "store_id": "306862",
        "ticketAmount": 547.0,
        "print_time": "2025-07-23 14:36:16"
    }
    print(create_example)
    
    print("\n🔸 ReceiptJsonFormat (for API output matching your format):")
    json_example = {
        "number": "11034250718000135",
        "store_name": "阳坊 涮肉",
        "store_id": "306862",
        "ticketAmount": 547.0,
        "print_time": "2025-07-23 14:36:16"
    }
    print(json_example)
    
    print("\n🔸 ReceiptResponse (full database response):")
    response_example = {
        "id": 1,
        "number": "11034250718000135",
        "store_name": "阳坊 涮肉",
        "store_id": "306862",
        "ticketAmount": 547.0,
        "print_time": "2025-07-23 14:36:16",
        "matched_at": "2025-07-23T14:36:16.123456+00:00",
        "status": "matched",
        "source_file": None,
        "response_file": None,
        "notes": None
    }
    print(response_example)

def main():
    """Main function to test Pydantic schemas"""
    print("🚀 Phase 3: Pydantic Schemas Test")
    print("=" * 50)
    
    # Show schema examples
    show_schema_examples()
    
    # Test all schemas
    schemas_work = test_schemas()
    
    if schemas_work:
        print("\n✅ Phase 3 completed successfully!")
        print("🎯 Ready for Phase 4: Database services")
        return True
    else:
        print("\n❌ Schema testing failed!")
        return False

if __name__ == "__main__":
    main()