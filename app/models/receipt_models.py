# models/receipt_models.py
"""
Database models for receipt data
Uses the same structure as your existing database
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, date
from decimal import Decimal

@dataclass
class Receipt:
    """
    Receipt model matching your existing database schema
    Based on your ReceiptService and database structure
    """
    id: Optional[int] = None
    receipt_number: Optional[str] = None
    store_name: Optional[str] = None  # Supports Chinese characters like "阳坊 涮肉"
    store_id: Optional[str] = None
    ticket_amount: Optional[Decimal] = None
    print_time: Optional[datetime] = None
    processing_date: Optional[date] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    source_file_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None
    raw_json: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert receipt to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'receipt_number': self.receipt_number,
            'store_name': self.store_name,
            'store_id': self.store_id,
            'ticket_amount': float(self.ticket_amount) if self.ticket_amount else None,
            'print_time': self.print_time.isoformat() if self.print_time else None,
            'processing_date': self.processing_date.isoformat() if self.processing_date else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'processed_at': self.processed_at.isoformat() if self.processed_at else None,
            'source_file_path': self.source_file_path,
            'file_path': self.file_path,
            'original_filename': self.original_filename,
            'raw_json': self.raw_json
        }
    
    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Receipt':
        """Create Receipt from database row (like your cursor.fetchone() result)"""
        return cls(
            id=row.get('id'),
            receipt_number=row.get('receipt_number'),
            store_name=row.get('store_name'),
            store_id=row.get('store_id'),
            ticket_amount=row.get('ticket_amount'),
            print_time=row.get('print_time'),
            processing_date=row.get('processing_date'),
            created_at=row.get('created_at'),
            updated_at=row.get('updated_at'),
            processed_at=row.get('processed_at'),
            source_file_path=row.get('source_file_path'),
            file_path=row.get('file_path'),
            original_filename=row.get('original_filename'),
            raw_json=row.get('raw_json')
        )

@dataclass
class TableColumn:
    """Database table column information"""
    column_name: str
    data_type: str
    is_nullable: str
    column_default: Optional[str] = None
    character_maximum_length: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'column_name': self.column_name,
            'data_type': self.data_type,
            'is_nullable': self.is_nullable == 'YES',
            'column_default': self.column_default,
            'character_maximum_length': self.character_maximum_length
        }

@dataclass
class TableStats:
    """Database table statistics"""
    total_receipts: int = 0
    total_amount: Optional[Decimal] = None
    avg_amount: Optional[Decimal] = None
    min_amount: Optional[Decimal] = None
    max_amount: Optional[Decimal] = None
    unique_stores: int = 0
    date_range: Optional[Dict[str, Optional[date]]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response"""
        return {
            'total_receipts': self.total_receipts,
            'total_amount': float(self.total_amount) if self.total_amount else 0.0,
            'avg_amount': float(self.avg_amount) if self.avg_amount else 0.0,
            'min_amount': float(self.min_amount) if self.min_amount else 0.0,
            'max_amount': float(self.max_amount) if self.max_amount else 0.0,
            'unique_stores': self.unique_stores,
            'date_range': {
                'earliest': self.date_range['earliest'].isoformat() if self.date_range and self.date_range.get('earliest') else None,
                'latest': self.date_range['latest'].isoformat() if self.date_range and self.date_range.get('latest') else None
            } if self.date_range else None
        }

@dataclass
class HealthStatus:
    """Health check status"""
    database_connected: bool = False
    total_receipts: int = 0
    services_available: bool = False
    errors: list = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'database_connected': self.database_connected,
            'total_receipts': self.total_receipts,
            'services_available': self.services_available,
            'errors': self.errors,
            'status': 'healthy' if self.database_connected and self.services_available and not self.errors else 'unhealthy',
            'timestamp': datetime.now().isoformat()
        }