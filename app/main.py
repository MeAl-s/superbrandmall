import sys
import os
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime
import logging
from typing import Optional, List, Dict, Any

# Add paths
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(current_dir))

from fastapi import FastAPI, HTTPException, Query, Path as FastAPIPath
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FastAPI Receipt System...")
    yield
    logger.info("Shutting down...")

# Create app
app = FastAPI(
    title="Receipt Management API",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "Receipt Management API",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "receipts": "/receipts",
            "single_receipt": "/receipts/{id}",
            "search": "/receipts/search/{term}",
            "stats": "/receipts/stats",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        connected = receipt_service.connect()
        
        if connected:
            with receipt_service.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM receipts")
                result = cursor.fetchone()
                count = result['count'] if result else 0
            
            receipt_service.disconnect()
            
            return {
                "status": "healthy",
                "database": "connected",
                "total_receipts": count,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "unhealthy",
                "database": "disconnected",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/receipts")
async def get_receipts(
    limit: int = Query(default=10, ge=1, le=100, description="Number of receipts to return"),
    offset: int = Query(default=0, ge=0, description="Number of receipts to skip"),
    store_name: Optional[str] = Query(default=None, description="Filter by store name")
):
    """Get receipts with optional filtering and pagination"""
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        if not receipt_service.connect():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Build query based on filters
        base_query = "SELECT * FROM receipts"
        count_query = "SELECT COUNT(*) as total FROM receipts"
        params = []
        
        if store_name:
            where_clause = " WHERE store_name ILIKE %s"
            base_query += where_clause
            count_query += where_clause
            params.append(f'%{store_name}%')
        
        # Get total count
        with receipt_service.get_cursor() as cursor:
            cursor.execute(count_query, params)
            total_result = cursor.fetchone()
            total_count = total_result['total'] if total_result else 0
        
        # Get receipts with pagination
        base_query += " ORDER BY id DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        with receipt_service.get_cursor() as cursor:
            cursor.execute(base_query, params)
            receipts = cursor.fetchall()
        
        receipt_service.disconnect()
        
        # Convert to dict format
        receipt_list = []
        for receipt in receipts:
            receipt_dict = dict(receipt)
            # Handle datetime and decimal serialization
            for key, value in receipt_dict.items():
                if hasattr(value, 'isoformat'):
                    receipt_dict[key] = value.isoformat()
                elif key == 'ticket_amount' and value:
                    receipt_dict[key] = float(value)
            receipt_list.append(receipt_dict)
        
        return {
            "receipts": receipt_list,
            "total_count": total_count,
            "returned_count": len(receipt_list),
            "limit": limit,
            "offset": offset,
            "has_more": (offset + len(receipt_list)) < total_count
        }
        
    except Exception as e:
        logger.error(f"Error getting receipts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/receipts/{receipt_id}")
async def get_receipt(receipt_id: int = FastAPIPath(..., description="Receipt ID")):
    """Get a specific receipt by ID"""
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
            raise HTTPException(status_code=404, detail=f"Receipt with ID {receipt_id} not found")
        
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
        logger.error(f"Error getting receipt {receipt_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/receipts/search/{search_term}")
async def search_receipts(
    search_term: str = FastAPIPath(..., description="Search term"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum results to return")
):
    """Search receipts by receipt number, store name, or amount"""
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
            "total_found": len(receipt_list),
            "max_results": limit
        }
        
    except Exception as e:
        logger.error(f"Error searching receipts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/receipts/stats/summary")
async def get_receipt_stats():
    """Get summary statistics for all receipts"""
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        if not receipt_service.connect():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        stats = {}
        
        with receipt_service.get_cursor() as cursor:
            # Get basic stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_receipts,
                    SUM(ticket_amount) as total_amount,
                    AVG(ticket_amount) as avg_amount,
                    MIN(ticket_amount) as min_amount,
                    MAX(ticket_amount) as max_amount,
                    COUNT(DISTINCT store_name) as unique_stores
                FROM receipts
                WHERE ticket_amount IS NOT NULL
            """)
            result = cursor.fetchone()
            
            if result:
                stats.update({
                    'total_receipts': result['total_receipts'],
                    'total_amount': float(result['total_amount']) if result['total_amount'] else 0.0,
                    'avg_amount': float(result['avg_amount']) if result['avg_amount'] else 0.0,
                    'min_amount': float(result['min_amount']) if result['min_amount'] else 0.0,
                    'max_amount': float(result['max_amount']) if result['max_amount'] else 0.0,
                    'unique_stores': result['unique_stores']
                })
            
            # Get date range
            cursor.execute("""
                SELECT 
                    MIN(processing_date) as earliest_date,
                    MAX(processing_date) as latest_date
                FROM receipts 
                WHERE processing_date IS NOT NULL
            """)
            date_result = cursor.fetchone()
            
            if date_result:
                stats['date_range'] = {
                    'earliest': date_result['earliest_date'].isoformat() if date_result['earliest_date'] else None,
                    'latest': date_result['latest_date'].isoformat() if date_result['latest_date'] else None
                }
        
        receipt_service.disconnect()
        
        return {
            'statistics': stats,
            'generated_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting receipt stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/receipts/stores")
async def get_stores():
    """Get list of all unique stores"""
    try:
        from services.database.receipt_service import ReceiptService
        
        receipt_service = ReceiptService()
        if not receipt_service.connect():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        with receipt_service.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    store_name,
                    COUNT(*) as receipt_count,
                    SUM(ticket_amount) as total_amount,
                    AVG(ticket_amount) as avg_amount
                FROM receipts 
                WHERE store_name IS NOT NULL
                GROUP BY store_name
                ORDER BY receipt_count DESC
            """)
            stores = cursor.fetchall()
        
        receipt_service.disconnect()
        
        # Convert results
        store_list = []
        for store in stores:
            store_dict = {
                'store_name': store['store_name'],
                'receipt_count': store['receipt_count'],
                'total_amount': float(store['total_amount']) if store['total_amount'] else 0.0,
                'avg_amount': float(store['avg_amount']) if store['avg_amount'] else 0.0
            }
            store_list.append(store_dict)
        
        return {
            'stores': store_list,
            'total_stores': len(store_list)
        }
        
    except Exception as e:
        logger.error(f"Error getting stores: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test-db")
async def test_database():
    """Test database connection (your existing endpoint)"""
    try:
        # Test your existing database connection
        from services.database.receipt_service import ReceiptService
        from config.settings import settings
        
        receipt_service = ReceiptService()
        connected = receipt_service.connect()
        
        if connected:
            # Get count
            with receipt_service.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM receipts")
                result = cursor.fetchone()
                count = result['count'] if result else 0
            
            receipt_service.disconnect()
            
            return {
                "database": "connected",
                "total_receipts": count,
                "settings_loaded": bool(settings)
            }
        else:
            return {"database": "failed", "error": "Could not connect"}
            
    except Exception as e:
        return {"database": "error", "details": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)