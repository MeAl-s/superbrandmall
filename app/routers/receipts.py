from fastapi import APIRouter, HTTPException
import sys
from pathlib import Path

# Add paths for existing services
current_dir = Path(__file__).parent
app_dir = current_dir.parent
project_root = app_dir.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

router = APIRouter()

@router.get("/receipts")
async def get_receipts(limit: int = 10):
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        if not receipt_service.connect():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        with receipt_service.get_cursor() as cursor:
            cursor.execute("SELECT * FROM receipts ORDER BY id DESC LIMIT %s", (limit,))
            receipts = cursor.fetchall()
        
        receipt_service.disconnect()
        
        # Convert to dict format
        receipt_list = []
        for receipt in receipts:
            receipt_dict = dict(receipt)
            # Handle datetime serialization
            for key, value in receipt_dict.items():
                if hasattr(value, 'isoformat'):
                    receipt_dict[key] = value.isoformat()
                elif key == 'ticket_amount' and value:
                    receipt_dict[key] = float(value)
            receipt_list.append(receipt_dict)
        
        return {
            "receipts": receipt_list,
            "total_returned": len(receipt_list)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/receipts/{receipt_id}")
async def get_receipt(receipt_id: int):
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        if not receipt_service.connect():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        with receipt_service.get_cursor() as cursor:
            cursor.execute("SELECT * FROM receipts WHERE id = %s", (receipt_id,))
            receipt = cursor.fetchone()
        
        receipt_service.disconnect()
        
        if not receipt:
            raise HTTPException(status_code=404, detail="Receipt not found")
        
        # Convert to dict and handle serialization
        receipt_dict = dict(receipt)
        for key, value in receipt_dict.items():
            if hasattr(value, 'isoformat'):
                receipt_dict[key] = value.isoformat()
            elif key == 'ticket_amount' and value:
                receipt_dict[key] = float(value)
        
        return receipt_dict
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/receipts/search/{search_term}")
async def search_receipts(search_term: str, limit: int = 20):
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        if not receipt_service.connect():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        with receipt_service.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM receipts 
                WHERE receipt_number ILIKE %s 
                   OR store_name ILIKE %s 
                   OR CAST(ticket_amount AS TEXT) ILIKE %s
                ORDER BY id DESC LIMIT %s
            """, (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', limit))
            receipts = cursor.fetchall()
        
        receipt_service.disconnect()
        
        # Convert results
        receipt_list = []
        for receipt in receipts:
            receipt_dict = dict(receipt)
            for key, value in receipt_dict.items():
                if hasattr(value, 'isoformat'):
                    receipt_dict[key] = value.isoformat()
                elif key == 'ticket_amount' and value:
                    receipt_dict[key] = float(value)
            receipt_list.append(receipt_dict)
        
        return {
            "search_term": search_term,
            "results": receipt_list,
            "total_found": len(receipt_list)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))