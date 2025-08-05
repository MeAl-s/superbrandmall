# schemas/receipt_schemas.py
"""
Pydantic schemas for request/response validation
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from decimal import Decimal

# Request schemas
class ReceiptSearchParams(BaseModel):
    """Schema for receipt search parameters"""
    q: Optional[str] = Field(None, description="Search term for receipt number, store name, or amount")
    store_name: Optional[str] = Field(None, description="Filter by store name (partial match)")
    store_id: Optional[str] = Field(None, description="Filter by exact store ID")
    receipt_number: Optional[str] = Field(None, description="Filter by receipt number (partial match)")
    date_from: Optional[date] = Field(None, description="Filter from date (YYYY-MM-DD)")
    date_to: Optional[date] = Field(None, description="Filter to date (YYYY-MM-DD)")
    amount_min: Optional[float] = Field(None, description="Minimum amount filter")
    amount_max: Optional[float] = Field(None, description="Maximum amount filter")
    limit: int = Field(50, ge=1, le=1000, description="Number of results to return (max 1000)")
    offset: int = Field(0, ge=0, description="Number of results to skip")

# Response schemas
class ReceiptResponse(BaseModel):
    """Schema for receipt response"""
    id: Optional[int] = None
    receipt_number: Optional[str] = None
    store_name: Optional[str] = None
    store_id: Optional[str] = None
    ticket_amount: Optional[float] = None
    print_time: Optional[str] = None  # ISO format string
    processing_date: Optional[str] = None  # ISO format string
    created_at: Optional[str] = None  # ISO format string
    updated_at: Optional[str] = None  # ISO format string
    processed_at: Optional[str] = None  # ISO format string
    source_file_path: Optional[str] = None
    file_path: Optional[str] = None
    original_filename: Optional[str] = None
    raw_json: Optional[Dict[str, Any]] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            Decimal: lambda v: float(v)
        }

class ReceiptListResponse(BaseModel):
    """Schema for receipt list response"""
    receipts: List[ReceiptResponse]
    total_count: int
    limit: int
    offset: int
    has_next: bool
    has_previous: bool

class TableColumnResponse(BaseModel):
    """Schema for table column information"""
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str] = None
    character_maximum_length: Optional[int] = None

class TableInfoResponse(BaseModel):
    """Schema for table information response"""
    table_name: str
    total_columns: int
    total_rows: int
    columns: List[TableColumnResponse]

class StatsResponse(BaseModel):
    """Schema for statistics response"""
    total_receipts: int
    total_amount: float
    avg_amount: float
    min_amount: float
    max_amount: float
    unique_stores: int
    date_range: Optional[Dict[str, Optional[str]]] = None
    recent_dates: List[Dict[str, Any]] = []
    latest_entries: List[ReceiptResponse] = []

class HealthResponse(BaseModel):
    """Schema for health check response"""
    status: str  # "healthy" or "unhealthy"
    database_connected: bool
    total_receipts: int
    services_available: bool
    errors: List[str] = []
    timestamp: str

class ErrorResponse(BaseModel):
    """Schema for error responses"""
    success: bool = False
    error_type: str
    message: str
    details: Optional[str] = None
    timestamp: str

class SuccessResponse(BaseModel):
    """Schema for success responses"""
    success: bool = True
    message: str
    data: Optional[Dict[str, Any]] = None
    timestamp: str

# Pagination schema
class PaginationParams(BaseModel):
    """Schema for pagination parameters"""
    page: int = Field(1, ge=1, description="Page number (starts from 1)")
    limit: int = Field(50, ge=1, le=1000, description="Items per page (max 1000)")
    
    @property
    def offset(self) -> int:
        """Calculate offset from page and limit"""
        return (self.page - 1) * self.limit

class PaginatedResponse(BaseModel):
    """Base schema for paginated responses"""
    total_count: int
    page: int
    limit: int
    total_pages: int
    has_next: bool
    has_previous: bool
    
    @classmethod
    def create(cls, total_count: int, page: int, limit: int) -> 'PaginatedResponse':
        """Create pagination metadata"""
        total_pages = (total_count + limit - 1) // limit  # Ceiling division
        
        return cls(
            total_count=total_count,
            page=page,
            limit=limit,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )

# Search result schemas
class SearchResultResponse(BaseModel):
    """Schema for search results"""
    query: str
    results: List[ReceiptResponse]
    total_found: int
    search_time_ms: float

class DateRangeResponse(BaseModel):
    """Schema for date range results"""
    date: str
    total_found: int
    receipts: List[ReceiptResponse]

class StoreStatsResponse(BaseModel):
    """Schema for store statistics"""
    store_id: str
    store_name: Optional[str]
    total_receipts: int
    total_amount: float
    avg_amount: float
    first_receipt_date: Optional[str]
    last_receipt_date: Optional[str]

class DailyStatsResponse(BaseModel):
    """Schema for daily statistics"""
    processing_date: str
    total_receipts: int
    total_amount: float
    avg_amount: float
    unique_stores: int
    earliest_receipt: Optional[str]
    latest_receipt: Optional[str]