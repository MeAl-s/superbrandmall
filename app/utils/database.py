# utils/database.py
"""
Database manager that integrates with your existing ReceiptService
"""
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from contextlib import contextmanager
from datetime import datetime, date

# Add paths for your existing services (same pattern as your database inserter)
current_dir = Path(__file__).parent
app_dir = current_dir.parent
project_root = app_dir.parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

try:
    from services.database.receipt_service import ReceiptService
    from config.settings import settings
except ImportError as e:
    print(f"⚠️ Could not import existing services: {e}")
    ReceiptService = None
    settings = None

from models.receipt_models import Receipt, TableColumn, TableStats, HealthStatus

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager that wraps your existing ReceiptService
    Provides FastAPI-compatible interface for your database operations
    """
    
    def __init__(self):
        self.receipt_service: Optional[ReceiptService] = None
        self._connected = False
        
    def connect(self) -> bool:
        """Connect to database using your existing ReceiptService"""
        try:
            if ReceiptService is None:
                logger.error("ReceiptService not available - check imports")
                return False
            
            if not self._connected:
                self.receipt_service = ReceiptService()
                self._connected = self.receipt_service.connect()
                
                if self._connected:
                    logger.info("✅ Database connected via ReceiptService")
                else:
                    logger.error("❌ Failed to connect via ReceiptService")
            
            return self._connected
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            self._connected = False
            return False
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            if not self._connected:
                return self.connect()
            
            # Test with a simple query
            with self.receipt_service.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    @contextmanager
    def get_cursor(self):
        """Get database cursor using your existing service"""
        if not self._connected:
            if not self.connect():
                raise Exception("Database not connected")
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                yield cursor
        except Exception as e:
            logger.error(f"Cursor error: {e}")
            raise
    
    def get_table_structure(self) -> List[TableColumn]:
        """Get table structure (like your database_table_viewer.py --structure)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'receipts' 
                    ORDER BY ordinal_position;
                """)
                
                columns = cursor.fetchall()
                return [TableColumn(**dict(col)) for col in columns]
                
        except Exception as e:
            logger.error(f"Error getting table structure: {e}")
            return []
    
    def get_table_stats(self) -> TableStats:
        """Get table statistics (like your database_table_viewer.py --stats)"""
        try:
            with self.get_cursor() as cursor:
                # Total count
                cursor.execute("SELECT COUNT(*) as total FROM receipts")
                total_count = cursor.fetchone()['total']
                
                # Amount statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as count,
                        AVG(ticket_amount) as avg_amount,
                        MIN(ticket_amount) as min_amount,
                        MAX(ticket_amount) as max_amount,
                        SUM(ticket_amount) as total_amount
                    FROM receipts 
                    WHERE ticket_amount IS NOT NULL
                """)
                amount_stats = cursor.fetchone()
                
                # Unique stores
                cursor.execute("SELECT COUNT(DISTINCT store_id) as unique_stores FROM receipts WHERE store_id IS NOT NULL")
                unique_stores = cursor.fetchone()['unique_stores']
                
                # Date range
                cursor.execute("SELECT MIN(processing_date) as earliest, MAX(processing_date) as latest FROM receipts")
                date_range_result = cursor.fetchone()
                date_range = {
                    'earliest': date_range_result['earliest'],
                    'latest': date_range_result['latest']
                } if date_range_result else None
                
                return TableStats(
                    total_receipts=total_count,
                    total_amount=amount_stats['total_amount'],
                    avg_amount=amount_stats['avg_amount'],
                    min_amount=amount_stats['min_amount'],
                    max_amount=amount_stats['max_amount'],
                    unique_stores=unique_stores,
                    date_range=date_range
                )
                
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return TableStats()
    
    def get_receipts(self, limit: int = 50, offset: int = 0) -> List[Receipt]:
        """Get receipts with pagination (like your database_table_viewer.py --sample)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM receipts 
                    ORDER BY id DESC 
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                
                rows = cursor.fetchall()
                return [Receipt.from_db_row(dict(row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting receipts: {e}")
            return []
    
    def get_receipt_by_id(self, receipt_id: int) -> Optional[Receipt]:
        """Get single receipt by ID (like your database_table_viewer.py --record)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT * FROM receipts WHERE id = %s", (receipt_id,))
                row = cursor.fetchone()
                
                if row:
                    return Receipt.from_db_row(dict(row))
                return None
                
        except Exception as e:
            logger.error(f"Error getting receipt {receipt_id}: {e}")
            return None
    
    def search_receipts(self, search_term: str, limit: int = 50) -> List[Receipt]:
        """Search receipts (like your database_table_viewer.py --search)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM receipts 
                    WHERE 
                        receipt_number ILIKE %s OR 
                        store_name ILIKE %s OR
                        CAST(ticket_amount AS TEXT) ILIKE %s
                    ORDER BY id DESC
                    LIMIT %s
                """, (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', limit))
                
                rows = cursor.fetchall()
                return [Receipt.from_db_row(dict(row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error searching receipts: {e}")
            return []
    
    def get_receipts_by_date(self, date_str: str, limit: int = 50) -> List[Receipt]:
        """Get receipts by date (like your database_table_viewer.py --date)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM receipts 
                    WHERE processing_date = %s
                    ORDER BY id DESC
                    LIMIT %s
                """, (date_str, limit))
                
                rows = cursor.fetchall()
                return [Receipt.from_db_row(dict(row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting receipts by date: {e}")
            return []
    
    def get_receipts_by_store(self, store_id: str, limit: int = 50) -> List[Receipt]:
        """Get receipts by store ID"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM receipts 
                    WHERE store_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                """, (store_id, limit))
                
                rows = cursor.fetchall()
                return [Receipt.from_db_row(dict(row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting receipts by store: {e}")
            return []
    
    def get_recent_dates(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent processing dates with counts"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT processing_date, COUNT(*) as count 
                    FROM receipts 
                    GROUP BY processing_date 
                    ORDER BY processing_date DESC 
                    LIMIT %s
                """, (limit,))
                
                rows = cursor.fetchall()
                return [{'processing_date': str(row['processing_date']), 'count': row['count']} for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting recent dates: {e}")
            return []
    
    def get_latest_entries(self, limit: int = 5) -> List[Receipt]:
        """Get latest entries"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM receipts 
                    ORDER BY created_at DESC 
                    LIMIT %s
                """, (limit,))
                
                rows = cursor.fetchall()
                return [Receipt.from_db_row(dict(row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting latest entries: {e}")
            return []
    
    def get_receipt_count(self) -> int:
        """Get total receipt count"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM receipts")
                result = cursor.fetchone()
                return result['count'] if result else 0
                
        except Exception as e:
            logger.error(f"Error getting receipt count: {e}")
            return 0
    
    def health_check(self) -> HealthStatus:
        """Perform health check"""
        try:
            database_connected = self.test_connection()
            total_receipts = self.get_receipt_count() if database_connected else 0
            services_available = ReceiptService is not None and settings is not None
            
            errors = []
            if not database_connected:
                errors.append("Database connection failed")
            if not services_available:
                errors.append("Required services not available")
            
            return HealthStatus(
                database_connected=database_connected,
                total_receipts=total_receipts,
                services_available=services_available,
                errors=errors
            )
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return HealthStatus(
                database_connected=False,
                total_receipts=0,
                services_available=False,
                errors=[str(e)]
            )
    
    def cleanup(self):
        """Cleanup database connections"""
        try:
            if self.receipt_service and self._connected:
                self.receipt_service.disconnect()
                self._connected = False
                logger.info("✅ Database connection closed")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")