# app/services/database/database_connection_service.py
"""
Database connection service with proper connection management and error handling
"""
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, Dict, Any

# Docker-compatible settings import
try:
    from config.settings import settings
except ImportError:
    try:
        from app.config.settings import settings
    except ImportError:
        print("⚠️ Could not import settings - using fallback configuration")
        settings = None


class DatabaseConnectionService:
    """Manages PostgreSQL database connections with proper error handling"""
    
    def __init__(self):
        self.connection: Optional[psycopg2.connection] = None
        self.logger = logging.getLogger(f"{__name__}.DatabaseConnectionService")
        self._connection_params = self._get_connection_params()
    
    def _get_connection_params(self) -> Dict[str, Any]:
        """Get database connection parameters from settings (using your existing pattern)"""
        if settings is None:
            # Fallback to environment variables if settings not available
            import os
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                return {
                    'dsn': database_url,
                    'cursor_factory': RealDictCursor
                }
            else:
                # Fallback defaults
                return {
                    'host': os.getenv('DB_HOST', 'localhost'),
                    'port': int(os.getenv('DB_PORT', 5432)),
                    'database': os.getenv('DB_NAME', 'myapp_db'),
                    'user': os.getenv('DB_USER', 'myapp_user'),
                    'password': os.getenv('DB_PASSWORD', 'superbrandmall'),
                    'cursor_factory': RealDictCursor
                }
        
        # Use your existing DATABASE_URL pattern
        connection_params = settings.get_database_connection_params()
        
        # Add cursor factory for dict-like results
        connection_params['cursor_factory'] = RealDictCursor
        
        return connection_params
    
    def connect(self) -> bool:
        """Establish database connection using your existing pattern"""
        try:
            if self.connection and not self.connection.closed:
                self.logger.info("Database connection already established")
                return True
            
            # Use your existing connection pattern
            connection_params = self._connection_params
            
            # Check if we have a DSN (DATABASE_URL) or individual params
            if 'dsn' in connection_params:
                self.connection = psycopg2.connect(
                    connection_params['dsn'],
                    cursor_factory=RealDictCursor
                )
            else:
                self.connection = psycopg2.connect(**connection_params)
            
            self.connection.autocommit = False
            
            self.logger.info("Database connection established successfully")
            return True
            
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            self.connection = None
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to database: {e}")
            self.connection = None
            return False
    
    def disconnect(self) -> None:
        """Close database connection"""
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
                self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {e}")
        finally:
            self.connection = None
    
    def is_connected(self) -> bool:
        """Check if database connection is active"""
        try:
            if not self.connection or self.connection.closed:
                return False
            
            # Test connection with a simple query
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                return True
                
        except Exception:
            return False
    
    def reconnect(self) -> bool:
        """Reconnect to database"""
        self.disconnect()
        return self.connect()
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursors with automatic cleanup"""
        if not self.is_connected():
            if not self.reconnect():
                raise psycopg2.Error("Unable to establish database connection")
        
        cursor = None
        try:
            cursor = self.connection.cursor()
            yield cursor
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            self.logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def commit(self) -> bool:
        """Commit current transaction"""
        try:
            if self.connection and not self.connection.closed:
                self.connection.commit()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to commit transaction: {e}")
            return False
    
    def rollback(self) -> bool:
        """Rollback current transaction"""
        try:
            if self.connection and not self.connection.closed:
                self.connection.rollback()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to rollback transaction: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[list]:
        """Execute a query and return results"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                
                if cursor.description:  # Query returns results
                    return cursor.fetchall()
                else:
                    return []
                    
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
            return None
    
    def execute_transaction(self, queries: list) -> bool:
        """Execute multiple queries in a transaction"""
        try:
            with self.get_cursor() as cursor:
                for query_data in queries:
                    if isinstance(query_data, tuple):
                        query, params = query_data
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query_data)
                
                self.commit()
                return True
                
        except Exception as e:
            self.rollback()
            self.logger.error(f"Transaction failed: {e}")
            return False