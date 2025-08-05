# Create app\models\receipt.py
# Copy this content to: app\models\receipt.py

from sqlalchemy import Column, Integer, String, Numeric, DateTime, Date, Text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from datetime import datetime, date
from typing import Dict, Any, Optional

Base = declarative_base()

class Receipt(Base):
    """Receipt model matching your existing database schema"""
    
    __tablename__ = "receipts"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Core receipt data (matching your schema exactly)
    receipt_number = Column(String(255), nullable=False)
    store_name = Column(String(255), nullable=True)
    store_id = Column(String(100), nullable=True)
    ticket_amount = Column(Numeric(10, 2), nullable=True)
    print_time = Column(DateTime, nullable=True)
    processing_date = Column(Date, nullable=False)
    
    # Audit fields
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # File tracking
    source_file_path = Column(Text, nullable=True)
    original_filename = Column(String(255), nullable=True)
    raw_json = Column(JSONB, nullable=True)
    
    # Constraints (matching your unique constraint)
    __table_args__ = (
        UniqueConstraint('receipt_number', 'processing_date', name='receipts_number_date_unique'),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert receipt to dictionary for JSON response (production optimized)"""
        return {
            "id": self.id,
            "receipt_number": self.receipt_number,
            "store_name": self.store_name,
            "store_id": self.store_id,
            "ticket_amount": float(self.ticket_amount) if self.ticket_amount else None,
            "print_time": self.print_time.isoformat() if self.print_time else None,
            "processing_date": self.processing_date.isoformat() if self.processing_date else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    def to_dict_minimal(self) -> Dict[str, Any]:
        """Minimal dictionary for large paginated responses (better performance)"""
        return {
            "receipt_number": self.receipt_number,
            "store_name": self.store_name,
            "store_id": self.store_id,
            "ticket_amount": float(self.ticket_amount) if self.ticket_amount else None,
            "print_time": self.print_time.isoformat() if self.print_time else None,
            "processing_date": self.processing_date.isoformat() if self.processing_date else None
        }
    
    def to_dict_with_metadata(self) -> Dict[str, Any]:
        """Full dictionary including file metadata (for admin/debug endpoints)"""
        return {
            **self.to_dict(),
            "source_file_path": self.source_file_path,
            "original_filename": self.original_filename,
            "raw_json": self.raw_json
        }
    
    def __repr__(self) -> str:
        return f"<Receipt(id={self.id}, number='{self.receipt_number}', store='{self.store_name}', amount={self.ticket_amount})>"