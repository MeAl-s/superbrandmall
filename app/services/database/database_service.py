# app/services/database/database_service.py - Fixed database service with print_time support
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import Optional
import logging

class DatabaseService:
    """Base database connection service with enhanced print_time support"""
    
    def __init__(self):
        self.connection: Optional[psycopg2.connection] = None
        self.logger = logging.getLogger(__name__)
        
        # Load environment variables from your .env file
        load_dotenv(r"C:\Point Detection\.env")
        
    def connect(self) -> bool:
        """Connect to database using your existing DATABASE_URL"""
        try:
            dsn = os.getenv("DATABASE_URL")
            if not dsn:
                raise ValueError("DATABASE_URL not found in environment variables")
            
            # Connect WITHOUT autocommit for DDL operations
            self.connection = psycopg2.connect(dsn)
            self.connection.autocommit = False  # Changed to False for proper transaction control
            
            # Test connection
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                result = cursor.fetchone()
            self.connection.commit()
                
            print("✅ Database connection established")
            self.logger.info("Database connection established")
            return True
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            self.logger.error(f"Database connection failed: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("🔌 Database connection closed")
            self.logger.info("Database connection closed")
    
    def get_cursor(self, dictionary=True):
        """Get database cursor"""
        if not self.connection:
            raise RuntimeError("Database not connected")
        
        if dictionary:
            return self.connection.cursor(cursor_factory=RealDictCursor)
        else:
            return self.connection.cursor()
    
    def execute_query(self, query: str, params=None, commit=True):
        """Execute a query and return results"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:  # SELECT query
                    result = cursor.fetchall()
                else:  # INSERT/UPDATE/DELETE
                    result = cursor.rowcount
                
                if commit:
                    self.connection.commit()
                
                return result
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def create_tables(self) -> bool:
        """Create database tables with print_time support"""
        if not self.connection:
            if not self.connect():
                return False
                
        try:
            print("🚀 Creating/updating database tables for print_time processing...")
            
            # Start a transaction for all DDL operations
            self.connection.autocommit = False
            
            # Create tables step by step
            self._create_receipts_table()
            self._create_daily_stats_table()
            
            # Commit tables before creating indexes
            self.connection.commit()
            print("✅ Tables committed to database")
            
            # Now create indexes after tables are committed
            self._create_indexes()
            
            # Final commit
            self.connection.commit()
            
            # Set autocommit back to True for normal operations
            self.connection.autocommit = True
            
            print("✅ Database tables created/verified successfully with print_time support")
            return True
                
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            self.logger.error(f"Error creating tables: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def _create_receipts_table(self):
        """UPDATED: Create receipts table with enhanced print_time structure"""
        with self.get_cursor(dictionary=False) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS receipts (
                    id SERIAL PRIMARY KEY,
                    receipt_number VARCHAR(255) NOT NULL,
                    store_name VARCHAR(255),
                    store_id VARCHAR(100),
                    ticket_amount DECIMAL(10,2),
                    print_time TIMESTAMP,
                    processing_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_file_path TEXT,
                    original_filename VARCHAR(255),
                    raw_json JSONB,
                    
                    CONSTRAINT receipts_number_date_unique 
                        UNIQUE(receipt_number, processing_date)
                );
            """)
        print("✅ Created/verified receipts table with print_time support")
        
        # Add any missing columns for existing tables
        self._add_missing_columns()
    
    def _create_daily_stats_table(self):
        """UPDATED: Create enhanced daily stats table with time tracking"""
        with self.get_cursor(dictionary=False) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id SERIAL PRIMARY KEY,
                    processing_date DATE UNIQUE NOT NULL,
                    total_receipts INTEGER DEFAULT 0,
                    total_amount DECIMAL(12,2) DEFAULT 0,
                    avg_amount DECIMAL(10,2) DEFAULT 0,
                    unique_stores INTEGER DEFAULT 0,
                    earliest_receipt TIMESTAMP,
                    latest_receipt TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        print("✅ Created/verified daily_stats table with time tracking")
    
    def _add_missing_columns(self):
        """Add missing columns to existing tables for backward compatibility"""
        try:
            with self.get_cursor(dictionary=False) as cursor:
                # Check and add print_time column if missing
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'receipts' AND column_name = 'print_time';
                """)
                
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE receipts ADD COLUMN print_time TIMESTAMP;")
                    print("✅ Added print_time column to existing receipts table")
                
                # Check and add time tracking columns to daily_stats if missing
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'daily_stats' AND column_name = 'earliest_receipt';
                """)
                
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE daily_stats ADD COLUMN earliest_receipt TIMESTAMP;")
                    cursor.execute("ALTER TABLE daily_stats ADD COLUMN latest_receipt TIMESTAMP;")
                    print("✅ Added time tracking columns to existing daily_stats table")
                    
        except Exception as e:
            print(f"⚠️ Note: Could not add missing columns (table might not exist yet): {e}")
    
    def _create_indexes(self):
        """UPDATED: Create comprehensive indexes for print_time processing"""
        try:
            # Enhanced index set for better performance with print_time queries
            indexes = [
                ("idx_receipts_processing_date", "CREATE INDEX IF NOT EXISTS idx_receipts_processing_date ON receipts(processing_date);"),
                ("idx_receipts_print_time", "CREATE INDEX IF NOT EXISTS idx_receipts_print_time ON receipts(print_time);"),
                ("idx_receipts_store_id", "CREATE INDEX IF NOT EXISTS idx_receipts_store_id ON receipts(store_id);"),
                ("idx_receipts_amount", "CREATE INDEX IF NOT EXISTS idx_receipts_amount ON receipts(ticket_amount);"),
                ("idx_receipts_receipt_number", "CREATE INDEX IF NOT EXISTS idx_receipts_receipt_number ON receipts(receipt_number);"),
                ("idx_receipts_store_date", "CREATE INDEX IF NOT EXISTS idx_receipts_store_date ON receipts(store_id, processing_date);"),
                ("idx_receipts_date_time", "CREATE INDEX IF NOT EXISTS idx_receipts_date_time ON receipts(processing_date, print_time);"),
                ("idx_daily_stats_date", "CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(processing_date);")
            ]
            
            for index_name, index_sql in indexes:
                try:
                    with self.get_cursor(dictionary=False) as cursor:
                        cursor.execute(index_sql)
                    print(f"✅ Created/verified index: {index_name}")
                except Exception as e:
                    # If index already exists, that's fine
                    if "already exists" in str(e):
                        print(f"ℹ️ Index {index_name} already exists")
                    else:
                        raise
            
            print("✅ All indexes created/verified for optimized print_time queries")
            
        except Exception as e:
            print(f"❌ Error creating indexes: {e}")
            raise
    
    def get_table_info(self, table_name: str = 'receipts') -> dict:
        """Get detailed information about a table structure"""
        try:
            with self.get_cursor() as cursor:
                # Get table structure
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    ORDER BY ordinal_position;
                """, (table_name,))
                
                columns = cursor.fetchall()
                
                # Get table statistics
                cursor.execute(f"SELECT COUNT(*) as total_rows FROM {table_name};")
                total_rows = cursor.fetchone()['total_rows']
                
                # Get recent activity for receipts table
                if table_name == 'receipts':
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as todays_receipts,
                            MIN(print_time) as earliest_today,
                            MAX(print_time) as latest_today
                        FROM receipts 
                        WHERE processing_date = CURRENT_DATE;
                    """)
                    today_stats = cursor.fetchone()
                else:
                    today_stats = {}
                
                return {
                    'table_name': table_name,
                    'columns': [dict(col) for col in columns],
                    'total_rows': total_rows,
                    'today_stats': dict(today_stats) if today_stats else {}
                }
                
        except Exception as e:
            print(f"❌ Error getting table info: {e}")
            return {}
    
    def verify_print_time_setup(self) -> bool:
        """Verify that print_time processing is set up correctly"""
        try:
            with self.get_cursor() as cursor:
                # Check if print_time column exists
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'receipts' AND column_name = 'print_time';
                """)
                
                if not cursor.fetchone():
                    print("❌ print_time column missing from receipts table")
                    return False
                
                # Check if time tracking columns exist in daily_stats
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'daily_stats' AND column_name IN ('earliest_receipt', 'latest_receipt');
                """)
                
                time_columns = cursor.fetchall()
                if len(time_columns) < 2:
                    print("❌ Time tracking columns missing from daily_stats table")
                    return False
                
                print("✅ Print_time processing setup verified successfully")
                return True
                
        except Exception as e:
            print(f"❌ Error verifying print_time setup: {e}")
            return False
    
    def migrate_existing_data(self):
        """Migrate existing data to support print_time processing"""
        try:
            print("🔄 Checking for data migration needs...")
            
            with self.get_cursor() as cursor:
                # Check if there are receipts without print_time that have raw_json
                cursor.execute("""
                    SELECT COUNT(*) as needs_migration
                    FROM receipts 
                    WHERE print_time IS NULL 
                    AND raw_json IS NOT NULL 
                    AND raw_json->>'print_time' IS NOT NULL;
                """)
                
                needs_migration = cursor.fetchone()['needs_migration']
                
                if needs_migration > 0:
                    print(f"📊 Found {needs_migration} receipts that need print_time migration")
                    
                    # Update print_time from raw_json
                    cursor.execute("""
                        UPDATE receipts 
                        SET print_time = (raw_json->>'print_time')::timestamp
                        WHERE print_time IS NULL 
                        AND raw_json IS NOT NULL 
                        AND raw_json->>'print_time' IS NOT NULL
                        AND raw_json->>'print_time' != 'unknown';
                    """)
                    
                    migrated = cursor.rowcount
                    self.connection.commit()
                    
                    print(f"✅ Successfully migrated {migrated} receipts with print_time data")
                else:
                    print("✅ No data migration needed")
                    
        except Exception as e:
            print(f"⚠️ Data migration completed with warnings: {e}")
            if self.connection:
                self.connection.rollback()
    
    def optimize_for_print_time(self):
        """Optimize database for print_time-based queries"""
        try:
            print("⚡ Optimizing database for print_time queries...")
            
            with self.get_cursor(dictionary=False) as cursor:
                # Update table statistics
                cursor.execute("ANALYZE receipts;")
                cursor.execute("ANALYZE daily_stats;")
                
                # Create partial indexes for better performance
                partial_indexes = [
                    ("idx_receipts_recent_print_time", 
                     "CREATE INDEX IF NOT EXISTS idx_receipts_recent_print_time ON receipts(print_time) WHERE print_time >= CURRENT_DATE - INTERVAL '30 days';"),
                    ("idx_receipts_today", 
                     "CREATE INDEX IF NOT EXISTS idx_receipts_today ON receipts(store_id, print_time) WHERE processing_date = CURRENT_DATE;")
                ]
                
                for index_name, index_sql in partial_indexes:
                    try:
                        cursor.execute(index_sql)
                        print(f"✅ Created optimization index: {index_name}")
                    except Exception as e:
                        if "already exists" not in str(e):
                            print(f"⚠️ Could not create {index_name}: {e}")
                
                self.connection.commit()
                print("✅ Database optimization completed")
                
        except Exception as e:
            print(f"⚠️ Optimization completed with warnings: {e}")
    
    def print_setup_summary(self):
        """Print a summary of the database setup"""
        print("\n" + "="*70)
        print("📊 DATABASE SETUP SUMMARY")
        print("="*70)
        
        # Get receipts table info
        receipts_info = self.get_table_info('receipts')
        daily_stats_info = self.get_table_info('daily_stats')
        
        print(f"📋 Receipts Table:")
        print(f"   Total rows: {receipts_info.get('total_rows', 0):,}")
        print(f"   Columns: {len(receipts_info.get('columns', []))}")
        
        if receipts_info.get('today_stats'):
            today = receipts_info['today_stats']
            print(f"   Today's receipts: {today.get('todays_receipts', 0)}")
            if today.get('earliest_today'):
                print(f"   Today's time range: {today['earliest_today']} to {today.get('latest_today', 'N/A')}")
        
        print(f"\n📊 Daily Stats Table:")
        print(f"   Total rows: {daily_stats_info.get('total_rows', 0):,}")
        print(f"   Columns: {len(daily_stats_info.get('columns', []))}")
        
        # Verify setup
        setup_ok = self.verify_print_time_setup()
        print(f"\n✅ Print_time processing: {'Ready' if setup_ok else 'Needs setup'}")
        print("="*70)