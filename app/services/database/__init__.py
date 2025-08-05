# app/services/database/__init__.py
"""
Database services package containing all database-related operations.
"""

try:
    from .database_connection_service import DatabaseConnectionService
except ImportError as e:
    print(f"Warning: Could not import DatabaseConnectionService: {e}")
    DatabaseConnectionService = None

try:
    from .database_schema_service import DatabaseSchemaService
except ImportError as e:
    print(f"Warning: Could not import DatabaseSchemaService: {e}")
    DatabaseSchemaService = None

try:
    from .receipt_processing_service import ReceiptProcessingService
except ImportError as e:
    print(f"Warning: Could not import ReceiptProcessingService: {e}")
    ReceiptProcessingService = None

try:
    from .file_processing_service import FileProcessingService
except ImportError as e:
    print(f"Warning: Could not import FileProcessingService: {e}")
    FileProcessingService = None

try:
    from .receipt_service import ReceiptService
except ImportError as e:
    print(f"Warning: Could not import ReceiptService: {e}")
    ReceiptService = None

__all__ = [
    'ReceiptService',
    'DatabaseConnectionService', 
    'DatabaseSchemaService',
    'ReceiptProcessingService',
    'FileProcessingService',
]
