# app/config/settings.py
"""
Settings configuration for Point Detection application
Updated to match existing database configuration pattern
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlparse

# Load environment variables from your existing .env file
load_dotenv(r"C:\Point Detection\.env")

class DatabaseSettings:
    """Database configuration settings using your existing pattern"""
    
    # Get DATABASE_URL from environment (your existing format)
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    # Parse the DATABASE_URL to get individual components
    @classmethod
    def _parse_database_url(cls):
        """Parse DATABASE_URL into individual components"""
        if not cls.DATABASE_URL:
            # Fallback defaults
            return {
                'host': 'localhost',
                'port': 5432,
                'database': 'receipt_db',
                'user': 'postgres',
                'password': 'password'
            }
        
        try:
            parsed = urlparse(cls.DATABASE_URL)
            return {
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 5432,
                'database': parsed.path.lstrip('/') if parsed.path else 'receipt_db',
                'user': parsed.username or 'postgres',
                'password': parsed.password or 'password'
            }
        except Exception as e:
            print(f"⚠️ Error parsing DATABASE_URL: {e}")
            # Return defaults
            return {
                'host': 'localhost',
                'port': 5432,
                'database': 'receipt_db',
                'user': 'postgres',
                'password': 'password'
            }
    
    @classmethod
    def get_connection_params(cls):
        """Get psycopg2 connection parameters"""
        if cls.DATABASE_URL:
            # Use the direct DATABASE_URL (your existing pattern)
            return {'dsn': cls.DATABASE_URL}
        else:
            # Fall back to individual parameters
            parsed = cls._parse_database_url()
            return {
                'host': parsed['host'],
                'port': parsed['port'],
                'database': parsed['database'],
                'user': parsed['user'],
                'password': parsed['password']
            }
    
    @classmethod
    def test_connection(cls):
        """Test database connection using your existing pattern"""
        print("\n🧪 Testing database connection...")
        try:
            if cls.DATABASE_URL:
                conn = psycopg2.connect(cls.DATABASE_URL)
            else:
                params = cls._parse_database_url()
                conn = psycopg2.connect(**params)
            
            print("✅ Connected to PostgreSQL!")
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            print("📊 Database replied:", result)
            cur.close()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False

class FileSettings:
    """File processing settings"""
    
    # Base directories
    BASE_DIR = Path(r"C:\Point Detection")
    WORKER_DIR = BASE_DIR / "worker"  # ADD THIS - This is what was missing!
    WORKER_DATA_DIR = WORKER_DIR / "data"
    
    # File processing directories
    CONVERTED_TZ_DIR = WORKER_DATA_DIR / "converted_tz"
    INSERTED_TO_DATABASE_DIR = WORKER_DATA_DIR / "inserted_to_database"
    
    # OCR directories - MOVED TO worker/data!
    OCR_FILES_DIR = WORKER_DATA_DIR / "receipt_files"  # Changed from BASE_DIR / "ocr_files"
    OCR_MONITOR_DIR = BASE_DIR / "monitor"
    
    # Data directory (for realtime_detector)
    DATA_DIR = WORKER_DATA_DIR / "real_time_response"  # ADD THIS - This was missing too!

class AuthSettings:
    """Authentication settings for API access - Following working structure"""
    def __init__(self):
        # Load environment variables with working defaults
        self.API_USER = os.getenv('API_USER', 'cpit')
        self.API_PASS = os.getenv('API_PASS', '@abc1234')
        self.ORG_UUID = os.getenv('ORG_UUID', '0003')
        
        # Working URLs from your structure
        self.BASE_URL = 'https://hddc01.superbrandmall.com'
        self.LOGIN_PAGE = f'{self.BASE_URL}/pod-web/static/views/app/login.html'
        self.LOGIN_URL = f'{self.BASE_URL}/pod-web/s/auth/0000/login'
        self.COOKIE_DOMAIN = '.superbrandmall.com'
        self.REQUEST_TIMEOUT = 30
        
        # API endpoints for receipt fetching - From your working demo code
        self.RECEIPT_API = f'{self.BASE_URL}/pod-web/s/web/0000/0003/ticketdata/query'  # Correct endpoint!
        self.RECEIPT_LIST_API = f'{self.BASE_URL}/pod-web/s/web/0000/0003/ticketdata/list'  # Alternative
        self.RECEIPT_SEARCH_API = f'{self.BASE_URL}/pod-web/s/web/0000/0003/ticketdata/search'  # Alternative
        
        # File download API - FORCE CORRECT ENDPOINT FROM .ENV OR DEFAULT
        self.FILE_API = os.getenv('FILE_API', f'{self.BASE_URL}/pod-web/s/web/0000/0003/ticket/getticketfilebyfeaturecode')

class RealtimeDetectorSettings:
    """Settings for realtime detector"""
    def __init__(self):
        self.refresh_interval = 10  # Default refresh interval in seconds (matches your argparse default)
        self.page_size = 50  # Default page size for API calls
        self.max_pages = 10  # Maximum pages to fetch
        self.timeout = 30  # Request timeout in seconds
        self.request_timeout = 30  # Request timeout for auth service

class OCRProcessorSettings:
    """Settings for OCR processor"""
    def __init__(self):
        self.request_timeout = 15  # Request timeout for file downloads (matches demo)
        # Content types from your working demo
        self.content_types = {
            "image/jpeg": ".jpg",
            "image/png": ".png", 
            "application/pdf": ".pdf"
        }

class Settings:
    """Main settings class - compatible with your existing database setup"""
    
    # Database settings (using your existing pattern)
    DATABASE_URL = DatabaseSettings.DATABASE_URL
    
    # Parse database components for backward compatibility
    _db_params = DatabaseSettings._parse_database_url()
    DB_HOST = _db_params['host']
    DB_PORT = _db_params['port'] 
    DB_NAME = _db_params['database']
    DB_USER = _db_params['user']
    DB_PASSWORD = _db_params['password']
    
    # File settings
    WORKER_DIR = FileSettings.WORKER_DIR  # ADD THIS - Missing attribute!
    DATA_DIR = FileSettings.DATA_DIR      # ADD THIS - Missing attribute!
    CONVERTED_TZ_DIR = FileSettings.CONVERTED_TZ_DIR
    INSERTED_TO_DATABASE_DIR = FileSettings.INSERTED_TO_DATABASE_DIR
    OCR_FILES_DIR = FileSettings.OCR_FILES_DIR
    OCR_MONITOR_DIR = FileSettings.WORKER_DATA_DIR / "real_time_response"  # FIX: Use same dir as realtime_detector!
    
    # Realtime detector settings
    realtime_detector = RealtimeDetectorSettings()
    
    # OCR processor settings
    ocr_processor = OCRProcessorSettings()
    
    # Authentication settings - Add these missing attributes!
    API_PASS = AuthSettings().API_PASS
    API_USER = AuthSettings().API_USER
    ORG_UUID = AuthSettings().ORG_UUID
    BASE_URL = AuthSettings().BASE_URL
    LOGIN_PAGE = AuthSettings().LOGIN_PAGE
    LOGIN_URL = AuthSettings().LOGIN_URL
    COOKIE_DOMAIN = AuthSettings().COOKIE_DOMAIN
    REQUEST_TIMEOUT = AuthSettings().REQUEST_TIMEOUT
    
    # API endpoints - Add these missing attributes!
    RECEIPT_API = AuthSettings().RECEIPT_API
    RECEIPT_LIST_API = AuthSettings().RECEIPT_LIST_API
    RECEIPT_SEARCH_API = AuthSettings().RECEIPT_SEARCH_API
    FILE_API = AuthSettings().FILE_API  # Add this for OCR processor!
    
    # Processing settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10))
    SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', 30))
    
    # Logging settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    @classmethod
    def test_database(cls):
        """Test database connection"""
        return DatabaseSettings.test_connection()
    
    @classmethod
    def get_database_connection_params(cls):
        """Get database connection parameters for psycopg2"""
        return DatabaseSettings.get_connection_params()

# Create global settings instance
settings = Settings()

# For backward compatibility
database_settings = DatabaseSettings()
file_settings = FileSettings()

# Debug info
if __name__ == "__main__":
    print("🔧 Settings Debug:")
    print(f"DATABASE_URL: {DatabaseSettings.DATABASE_URL}")
    print(f"Parsed DB Host: {settings.DB_HOST}")
    print(f"Parsed DB Port: {settings.DB_PORT}")
    print(f"Parsed DB Name: {settings.DB_NAME}")
    print(f"Parsed DB User: {settings.DB_USER}")
    print(f"WORKER_DIR: {settings.WORKER_DIR}")
    print(f"DATA_DIR: {settings.DATA_DIR}")
    print(f"Connection params: {settings.get_database_connection_params()}")
    
    # Test connection
    settings.test_database()