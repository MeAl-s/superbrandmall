# app/services/database/database_schema_service.py
"""
Database schema management service for creating and maintaining database structure
"""
import logging
from typing import Dict, List
from .database_connection_service import DatabaseConnectionService


class DatabaseSchemaService:
    """Manages database schema creation and updates"""
    
    def __init__(self, connection_service: DatabaseConnectionService):
        self.db = connection_service
        self.logger = logging.getLogger(f"{__name__}.DatabaseSchemaService")
    
    def create_all_tables(self) -> bool:
        """Create all required database tables"""
        print("🚀 Creating/updating database tables...")
        
        try:
            # Create tables in dependency order
            if not self._create_receipts_table():
                return False
            
            # Add any missing columns to existing tables
            if not self._add_missing_receipt_columns():
                return False
            
            if not self._create_daily_stats_table():
                return False
            
            if not self._create_indexes():
                return False
            
            print("✅ All database tables created/verified successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create database schema: {e}")
            print(f"❌ Error creating database schema: {e}")
            return False
    
    def _add_missing_receipt_columns(self) -> bool:
        """Add missing columns to existing receipts table"""
        try:
            # Define the new columns we need
            new_columns = {
                'store_id': 'VARCHAR(50)',
                'original_print_time': 'VARCHAR(100)',
                'timezone_conversion': 'VARCHAR(50)'
            }
            
            # Check which columns exist
            existing_columns = self.get_table_info('receipts')['columns']
            existing_names = {col['column_name'] for col in existing_columns}
            
            # Add missing columns
            columns_to_add = []
            for col_name, col_definition in new_columns.items():
                if col_name not in existing_names:
                    columns_to_add.append(f"ADD COLUMN IF NOT EXISTS {col_name} {col_definition}")
            
            if columns_to_add:
                # Add columns one by one to avoid conflicts
                with self.db.get_cursor() as cursor:
                    for column_def in columns_to_add:
                        try:
                            alter_query = f"ALTER TABLE receipts {column_def};"
                            cursor.execute(alter_query)
                            print(f"✅ Added column: {column_def}")
                        except Exception as e:
                            # Handle case where column already exists
                            if "already exists" in str(e).lower():
                                print(f"⏭️ Column already exists: {column_def}")
                            else:
                                self.logger.warning(f"Could not add column {column_def}: {e}")
                    
                    self.db.commit()
                
                print(f"✅ Added {len(columns_to_add)} missing columns to receipts table")
            else:
                print("✅ All required columns already exist in receipts table")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding missing columns to receipts table: {e}")
            print(f"⚠️ Could not add missing columns to receipts table: {e}")
            return False
    
    def _create_receipts_table(self) -> bool:
        """Create the main receipts table (updated for your JSON format)"""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS receipts (
                id SERIAL PRIMARY KEY,
                receipt_number VARCHAR(100) NOT NULL,
                store_name VARCHAR(255),
                store_id VARCHAR(50),
                ticket_amount DECIMAL(10,2),
                print_time TIME,
                processing_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data JSONB,
                original_print_time VARCHAR(100),
                timezone_conversion VARCHAR(50),
                
                -- Ensure unique receipt per processing date
                CONSTRAINT unique_receipt_per_date UNIQUE (receipt_number, processing_date)
            );
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(create_table_query)
                self.db.commit()
            
            print("✅ Created/verified receipts table with new fields")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create receipts table: {e}")
            print(f"❌ Failed to create receipts table: {e}")
            return False
    
    def _create_daily_stats_table(self) -> bool:
        """Create the daily statistics table"""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS daily_stats (
                id SERIAL PRIMARY KEY,
                processing_date DATE NOT NULL UNIQUE,
                total_receipts INTEGER DEFAULT 0,
                total_amount DECIMAL(12,2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(create_table_query)
                self.db.commit()
            
            print("✅ Created/verified daily_stats table")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create daily_stats table: {e}")
            print(f"❌ Failed to create daily_stats table: {e}")
            return False
    
    def _create_indexes(self) -> bool:
        """Create database indexes for performance (with error handling for missing columns)"""
        try:
            # Basic indexes that should always work
            basic_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_receipts_processing_date ON receipts(processing_date);",
                "CREATE INDEX IF NOT EXISTS idx_receipts_receipt_number ON receipts(receipt_number);",
                "CREATE INDEX IF NOT EXISTS idx_receipts_store_name ON receipts(store_name);",
                "CREATE INDEX IF NOT EXISTS idx_receipts_created_at ON receipts(created_at);",
                "CREATE INDEX IF NOT EXISTS idx_daily_stats_processing_date ON daily_stats(processing_date);"
            ]
            
            # Optional indexes for new columns (might not exist yet)
            optional_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_receipts_store_id ON receipts(store_id);",
            ]
            
            with self.db.get_cursor() as cursor:
                # Create basic indexes
                for index_query in basic_indexes:
                    try:
                        cursor.execute(index_query)
                        print(f"✅ Created index: {index_query.split('idx_')[1].split(' ON')[0]}")
                    except Exception as e:
                        self.logger.warning(f"Could not create basic index: {e}")
                
                # Create optional indexes (ignore errors for missing columns)
                for index_query in optional_indexes:
                    try:
                        cursor.execute(index_query)
                        print(f"✅ Created optional index: {index_query.split('idx_')[1].split(' ON')[0]}")
                    except Exception as e:
                        if "does not exist" in str(e).lower():
                            print(f"⏭️ Skipping index for missing column: {index_query.split('idx_')[1].split(' ON')[0]}")
                        else:
                            self.logger.warning(f"Could not create optional index: {e}")
                
                self.db.commit()
            
            print("✅ Created/verified database indexes")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create indexes: {e}")
            print(f"❌ Failed to create indexes: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        try:
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()
                return result['exists'] if result else False
                
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict:
        """Get information about a table structure"""
        try:
            query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (table_name,))
                columns = cursor.fetchall()
                
                return {
                    'table_name': table_name,
                    'exists': len(columns) > 0,
                    'columns': columns
                }
                
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            return {'table_name': table_name, 'exists': False, 'columns': []}
    
    def add_missing_columns(self, table_name: str, required_columns: Dict[str, str]) -> bool:
        """Add missing columns to an existing table"""
        try:
            existing_columns = self.get_table_info(table_name)['columns']
            existing_names = {col['column_name'] for col in existing_columns}
            
            missing_columns = []
            for col_name, col_definition in required_columns.items():
                if col_name not in existing_names:
                    missing_columns.append(f"ADD COLUMN {col_name} {col_definition}")
            
            if missing_columns:
                alter_query = f"ALTER TABLE {table_name} {', '.join(missing_columns)};"
                
                with self.db.get_cursor() as cursor:
                    cursor.execute(alter_query)
                    self.db.commit()
                
                print(f"✅ Added {len(missing_columns)} missing columns to {table_name}")
                return True
            else:
                print(f"✅ No missing columns in {table_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error adding missing columns to {table_name}: {e}")
            print(f"⚠️ Could not add missing columns to {table_name}: {e}")
            return False
    
    def drop_table(self, table_name: str, cascade: bool = False) -> bool:
        """Drop a table (use with caution!)"""
        try:
            cascade_clause = "CASCADE" if cascade else "RESTRICT"
            query = f"DROP TABLE IF EXISTS {table_name} {cascade_clause};"
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query)
                self.db.commit()
            
            print(f"✅ Dropped table {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error dropping table {table_name}: {e}")
            print(f"❌ Failed to drop table {table_name}: {e}")
            return False
    
    def reset_schema(self) -> bool:
        """Reset the entire database schema (DANGER: Deletes all data!)"""
        try:
            # Drop tables in reverse dependency order
            tables_to_drop = ['daily_stats', 'receipts']
            
            for table in tables_to_drop:
                if not self.drop_table(table, cascade=True):
                    return False
            
            # Recreate all tables
            return self.create_all_tables()
            
        except Exception as e:
            self.logger.error(f"Error resetting schema: {e}")
            print(f"❌ Failed to reset schema: {e}")
            return False