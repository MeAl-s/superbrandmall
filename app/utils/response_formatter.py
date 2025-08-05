# utils/response_formatter.py - Response Utilities
"""
Utilities for formatting API responses
"""
from datetime import datetime
from typing import Dict, Any, Optional

def create_success_response(
    message: str,
    data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a standardized success response"""
    return {
        "success": True,
        "message": message,
        "data": data,
        "timestamp": datetime.now().isoformat()
    }

def create_error_response(
    error_type: str,
    message: str,
    details: Optional[str] = None
) -> Dict[str, Any]:
    """Create a standardized error response"""
    return {
        "success": False,
        "error_type": error_type,
        "message": message,
        "details": details,
        "timestamp": datetime.now().isoformat()
    }

def format_receipt_for_response(receipt_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Format receipt data for API response"""
    # Handle None values and ensure proper formatting
    formatted = {}
    
    for key, value in receipt_dict.items():
        if value is None:
            formatted[key] = None
        elif isinstance(value, datetime):
            formatted[key] = value.isoformat()
        elif key == 'ticket_amount' and value is not None:
            formatted[key] = float(value)
        else:
            formatted[key] = value
    
    return formatted

def create_pagination_metadata(
    total_count: int,
    page: int,
    limit: int
) -> Dict[str, Any]:
    """Create pagination metadata"""
    total_pages = (total_count + limit - 1) // limit
    offset = (page - 1) * limit
    
    return {
        "total_count": total_count,
        "page": page,
        "limit": limit,
        "total_pages": total_pages,
        "offset": offset,
        "has_next": page < total_pages,
        "has_previous": page > 1
    }