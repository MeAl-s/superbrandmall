# app/services/database/receipt_service.py
"""
Main receipt service that coordinates all receipt-related operations
This is the main service that the workers will interact with
"""
import logging
from pathlib import Path
from datetime import date
from typing import Dict, Optional, List, Any
from .database_connection_service import DatabaseConnectionService
from .database_schema_service import DatabaseSchemaService
from .receipt_processing_service import ReceiptProcessingService


class ReceiptService:
    """
    Main receipt service that provides a unified interface for all receipt operations.
    This service coordinates database connections, schema management, and receipt processing.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.ReceiptService")
        
        # Initialize services
        self.db_connection = DatabaseConnectionService()
        self.schema_service = DatabaseSchemaService(self.db_connection)
        self.processing_service = ReceiptProcessingService(self.db_connection)
        
        self._initialized = False
    
    def connect(self) -> bool:
        """Connect to database and initialize services"""
        try:
            if not self.db_connection.connect():
                return False
            
            self._initialized = True
            self.logger.info("Receipt service connected successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect receipt service: {e}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from database"""
        try:
            self.db_connection.disconnect()
            self._initialized = False
            self.logger.info("Receipt service disconnected")
        except Exception as e:
            self.logger.error(f"Error disconnecting receipt service: {e}")
    
    def create_tables(self) -> bool:
        """Create database schema"""
        if not self._initialized:
            self.logger.error("Service not connected")
            return False
        
        return self.schema_service.create_all_tables()
    
    def is_connected(self) -> bool:
        """Check if service is connected and ready"""
        return self._initialized and self.db_connection.is_connected()
    
    # Receipt Processing Methods
    def parse_json_file_for_processing(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Parse JSON file and return receipt data ready for processing"""
        return self.processing_service.parse_json_file(file_path)
    
    def insert_receipt(self, receipt_data: Dict[str, Any]) -> Optional[int]:
        """Insert receipt into database"""
        if not self._initialized:
            self.logger.error("Service not connected")
            return None
        
        return self.processing_service.insert_receipt(receipt_data)
    
    def check_duplicate_receipt(self, receipt_number: str, processing_date: date) -> bool:
        """Check if receipt already exists"""
        if not self._initialized:
            return False
        
        return self.processing_service.check_duplicate(receipt_number, processing_date)
    
    # Statistics and Reporting Methods
    def get_receipt_count(self) -> int:
        """Get total receipt count"""
        if not self._initialized:
            return 0
        
        return self.processing_service.get_receipt_count()
    
    def get_receipt_count_by_date(self, processing_date: date) -> int:
        """Get receipt count for specific date"""
        if not self._initialized:
            return 0
        
        return self.processing_service.get_receipt_count_by_date(processing_date)
    
    def update_daily_stats(self, processing_date: date) -> bool:
        """Update daily statistics"""
        if not self._initialized:
            return False
        
        return self.processing_service.update_daily_stats(processing_date)
    
    def get_daily_stats(self, processing_date: date) -> Optional[Dict]:
        """Get daily statistics"""
        if not self._initialized:
            return None
        
        return self.processing_service.get_daily_stats(processing_date)
    
    # Data Retrieval Methods
    def get_receipt_by_id(self, receipt_id: int) -> Optional[Dict]:
        """Get receipt by ID"""
        if not self._initialized:
            return None
        
        return self.processing_service.get_receipt_by_id(receipt_id)
    
    def get_receipts_by_date(self, processing_date: date) -> List[Dict]:
        """Get all receipts for a specific date"""
        if not self._initialized:
            return []
        
        return self.processing_service.get_receipts_by_date(processing_date)
    
    # Database Utility Methods
    def get_cursor(self):
        """Get database cursor (context manager)"""
        if not self._initialized:
            raise RuntimeError("Service not connected")
        
        return self.db_connection.get_cursor()
    
    def commit(self) -> bool:
        """Commit current transaction"""
        if not self._initialized:
            return False
        
        return self.db_connection.commit()
    
    def rollback(self) -> bool:
        """Rollback current transaction"""
        if not self._initialized:
            return False
        
        return self.db_connection.rollback()
    
    # Schema Management Methods
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        if not self._initialized:
            return False
        
        return self.schema_service.table_exists(table_name)
    
    def reset_schema(self) -> bool:
        """Reset database schema (WARNING: Deletes all data!)"""
        if not self._initialized:
            return False
        
        return self.schema_service.reset_schema()
    
    # Health Check Methods
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health_status = {
            'connected': False,
            'tables_exist': False,
            'total_receipts': 0,
            'today_receipts': 0,
            'errors': []
        }
        
        try:
            # Check connection
            health_status['connected'] = self.is_connected()
            if not health_status['connected']:
                health_status['errors'].append("Database connection failed")
                return health_status
            
            # Check tables
            receipts_exist = self.table_exists('receipts')
            daily_stats_exist = self.table_exists('daily_stats')
            health_status['tables_exist'] = receipts_exist and daily_stats_exist
            
            if not receipts_exist:
                health_status['errors'].append("receipts table missing")
            if not daily_stats_exist:
                health_status['errors'].append("daily_stats table missing")
            
            # Get counts
            health_status['total_receipts'] = self.get_receipt_count()
            health_status['today_receipts'] = self.get_receipt_count_by_date(date.today())
            
        except Exception as e:
            health_status['errors'].append(f"Health check failed: {e}")
            self.logger.error(f"Health check error: {e}")
        
        return health_status