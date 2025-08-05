# app/config/fastapi_settings.py
"""
FastAPI-specific settings that extend your existing configuration
"""
import os
from typing import List, Optional
from pydantic import BaseSettings, Field

# Import your existing settings
try:
    from config.settings import settings as base_settings
except ImportError:
    try:
        from app.config.settings import settings as base_settings
    except ImportError:
        print("⚠️ Could not import base settings - using fallback")
        base_settings = None

class FastAPISettings(BaseSettings):
    """FastAPI-specific configuration extending your existing settings"""
    
    # === API Configuration ===
    API_HOST: str = Field(default="0.0.0.0", env="API_HOST")
    API_PORT: int = Field(default=8000, env="API_PORT")
    API_V1_PREFIX: str = Field(default="/api/v1", env="API_V1_PREFIX")
    WEBSOCKET_PREFIX: str = Field(default="/ws", env="WEBSOCKET_PREFIX")
    
    # === CORS Configuration ===
    CORS_ENABLED: bool = Field(default=True, env="CORS_ENABLED")
    CORS_ORIGINS: List[str] = Field(
        default=["*"], 
        env="CORS_ORIGINS",
        description="Comma-separated list of allowed origins"
    )
    CORS_METHODS: List[str] = Field(default=["*"], env="CORS_METHODS")
    CORS_HEADERS: List[str] = Field(default=["*"], env="CORS_HEADERS")
    CORS_CREDENTIALS: bool = Field(default=True, env="CORS_CREDENTIALS")
    
    # === Database Configuration ===
    # Inherit from your existing settings
    DATABASE_URL: Optional[str] = Field(
        default=None, 
        env="DATABASE_URL",
        description="Database URL from your existing configuration"
    )
    DB_POOL_SIZE: int = Field(default=10, env="DB_POOL_SIZE")
    DB_MAX_OVERFLOW: int = Field(default=20, env="DB_MAX_OVERFLOW")
    DB_POOL_TIMEOUT: int = Field(default=30, env="DB_POOL_TIMEOUT")
    DB_POOL_RECYCLE: int = Field(default=3600, env="DB_POOL_RECYCLE")
    
    # === WebSocket Configuration ===
    WEBSOCKET_HEARTBEAT: int = Field(default=30, env="WEBSOCKET_HEARTBEAT")
    WEBSOCKET_MAX_CONNECTIONS: int = Field(default=1000, env="WEBSOCKET_MAX_CONNECTIONS")
    WEBSOCKET_MESSAGE_QUEUE_SIZE: int = Field(default=100, env="WEBSOCKET_MESSAGE_QUEUE_SIZE")
    
    # === Real-time Features ===
    BROADCAST_BATCH_SIZE: int = Field(default=50, env="BROADCAST_BATCH_SIZE")
    NOTIFICATION_ENABLED: bool = Field(default=True, env="NOTIFICATION_ENABLED")
    STATS_UPDATE_INTERVAL: int = Field(default=60, env="STATS_UPDATE_INTERVAL")
    
    # === Performance Configuration ===
    CACHE_ENABLED: bool = Field(default=True, env="CACHE_ENABLED")
    CACHE_TTL: int = Field(default=300, env="CACHE_TTL")
    PAGINATION_DEFAULT_LIMIT: int = Field(default=50, env="PAGINATION_DEFAULT_LIMIT")
    PAGINATION_MAX_LIMIT: int = Field(default=1000, env="PAGINATION_MAX_LIMIT")
    
    # === Security Configuration ===
    RATE_LIMIT_ENABLED: bool = Field(default=True, env="RATE_LIMIT_ENABLED")
    RATE_LIMIT_REQUESTS: int = Field(default=100, env="RATE_LIMIT_REQUESTS")
    RATE_LIMIT_WINDOW: int = Field(default=60, env="RATE_LIMIT_WINDOW")
    
    # === Logging Configuration ===
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    REQUEST_LOGGING: bool = Field(default=True, env="REQUEST_LOGGING")
    
    # === Development Configuration ===
    DEBUG: bool = Field(default=False, env="DEBUG")
    TESTING: bool = Field(default=False, env="TESTING")
    RELOAD: bool = Field(default=False, env="RELOAD")
    
    # === Integration with Your System ===
    WORKER_INTEGRATION: bool = Field(default=True, env="WORKER_INTEGRATION")
    FILE_MONITORING: bool = Field(default=True, env="FILE_MONITORING")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Inherit DATABASE_URL from your existing settings if available
        if not self.DATABASE_URL and base_settings:
            self.DATABASE_URL = getattr(base_settings, 'DATABASE_URL', None)
        
        # Parse CORS_ORIGINS if it's a comma-separated string
        if isinstance(self.CORS_ORIGINS, str):
            self.CORS_ORIGINS = [origin.strip() for origin in self.CORS_ORIGINS.split(",")]
    
    @property
    def database_url(self) -> str:
        """Get database URL with fallback to your existing configuration"""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        
        # Fallback to your existing settings pattern
        if base_settings:
            if hasattr(base_settings, 'DATABASE_URL') and base_settings.DATABASE_URL:
                return base_settings.DATABASE_URL
            
            # Build from individual components if available
            if all(hasattr(base_settings, attr) for attr in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']):
                return (
                    f"postgresql://{base_settings.DB_USER}:{base_settings.DB_PASSWORD}"
                    f"@{base_settings.DB_HOST}:{base_settings.DB_PORT}/{base_settings.DB_NAME}"
                )
        
        # Final fallback
        return os.getenv(
            "DATABASE_URL", 
            "postgresql://postgres:password@localhost:5432/receipt_db"
        )
    
    @property
    def cors_origins_list(self) -> List[str]:
        """Get CORS origins as a properly formatted list"""
        if not self.CORS_ENABLED:
            return []
        return self.CORS_ORIGINS
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.DEBUG or self.TESTING or self.RELOAD
    
    @property
    def sqlalchemy_database_url(self) -> str:
        """Get SQLAlchemy-compatible database URL"""
        url = self.database_url
        # Convert postgres:// to postgresql:// if needed (SQLAlchemy compatibility)
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        return url
    
    def get_integration_info(self) -> dict:
        """Get information about integration with your existing system"""
        return {
            "base_settings_available": base_settings is not None,
            "database_url_source": "inherited" if (base_settings and getattr(base_settings, 'DATABASE_URL', None)) else "environment",
            "worker_integration": self.WORKER_INTEGRATION,
            "file_monitoring": self.FILE_MONITORING,
            "existing_config_detected": bool(base_settings)
        }
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

# Create global settings instance
fastapi_settings = FastAPISettings()

# Debug information (only in development)
if fastapi_settings.is_development:
    print(f"\n🔧 FastAPI Settings Debug:")
    print(f"   Database URL configured: {bool(fastapi_settings.database_url)}")
    print(f"   Base settings available: {base_settings is not None}")
    print(f"   CORS enabled: {fastapi_settings.CORS_ENABLED}")
    print(f"   Debug mode: {fastapi_settings.DEBUG}")
    print(f"   Integration info: {fastapi_settings.get_integration_info()}")
    print(f"   API will run on: {fastapi_settings.API_HOST}:{fastapi_settings.API_PORT}")
    print()