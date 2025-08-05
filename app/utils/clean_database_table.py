# database_cleanup.py - Safe database cleanup utility
import sys
import os
from pathlib import Path
from datetime import datetime
import argparse
import shutil

# Add project paths
current_dir = Path(__file__).parent
app_dir = current_dir.parent if current_dir.name == 'workers' else current_dir
project_root = app_dir.parent if app_dir.name == 'app' else app_dir.parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from services.database.receipt_service import ReceiptService

class DatabaseCleanup:
    """Utility to safely clean up database and file system for fresh start"""
    
    def __init__(self):
        self.receipt_service = ReceiptService()
        self.backup_dir = Path("backups") / datetime.now().strftime("backup_%Y%m%d_%H%M%S")
        
    def connect(self) -> bool:
        """Connect to database"""
        return self.receipt_service.connect()
    
    def show_current_state(self):
        """Show current database and file system state"""
        print("\n" + "="*80)
        print("🔍 CURRENT DATABASE STATE")
        print("="*80)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                # Check if tables exist
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('receipts', 'daily_stats');
                """)
                tables = [row['table_name'] for row in cursor.fetchall()]
                
                if 'receipts' in tables:
                    cursor.execute("SELECT COUNT(*) as count FROM receipts;")
                    receipt_count = cursor.fetchone()['count']
                    
                    cursor.execute("""
                        SELECT 
                            MIN(processing_date) as earliest_date,
                            MAX(processing_date) as latest_date,
                            COUNT(DISTINCT processing_date) as unique_dates
                        FROM receipts;
                    """)
                    date_stats = cursor.fetchone()
                    
                    print(f"📊 Receipts table: {receipt_count:,} records")
                    if receipt_count > 0:
                        print(f"   📅 Date range: {date_stats['earliest_date']} to {date_stats['latest_date']}")
                        print(f"   📅 Unique dates: {date_stats['unique_dates']}")
                    
                    # Show breakdown by date
                    if receipt_count > 0:
                        cursor.execute("""
                            SELECT processing_date, COUNT(*) as count
                            FROM receipts 
                            GROUP BY processing_date 
                            ORDER BY processing_date DESC
                            LIMIT 10;
                        """)
                        recent_dates = cursor.fetchall()
                        
                        print(f"   📋 Recent dates:")
                        for date_row in recent_dates:
                            print(f"      {date_row['processing_date']}: {date_row['count']:,} receipts")
                else:
                    print("📊 Receipts table: Does not exist")
                
                if 'daily_stats' in tables:
                    cursor.execute("SELECT COUNT(*) as count FROM daily_stats;")
                    stats_count = cursor.fetchone()['count']
                    print(f"📈 Daily stats table: {stats_count:,} records")
                else:
                    print("📈 Daily stats table: Does not exist")
                    
        except Exception as e:
            print(f"❌ Error checking database state: {e}")
        
        # Check file system
        print(f"\n📁 FILE SYSTEM STATE")
        print("-"*80)
        
        paths_to_check = [
            ("Converted TZ", Path(r"C:\Point Detection\worker\data\converted_tz")),
            ("Inserted to DB", Path(r"C:\Point Detection\worker\data\inserted_to_database")),
            ("Matched Non-Delivery", Path(r"C:\Point Detection\worker\data\matched_non_delivery")),
        ]
        
        for name, path in paths_to_check:
            if path.exists():
                json_files = list(path.rglob("*.json"))
                print(f"📂 {name}: {len(json_files)} JSON files")
                
                # Show date folders if any
                date_folders = [d for d in path.iterdir() if d.is_dir() and len(d.name) == 10]
                if date_folders:
                    print(f"   📅 Date folders: {len(date_folders)}")
                    for folder in sorted(date_folders)[-5:]:  # Show last 5
                        folder_files = len(list(folder.glob("*.json")))
                        print(f"      {folder.name}: {folder_files} files")
            else:
                print(f"📂 {name}: Directory does not exist")
    
    def backup_database(self) -> bool:
        """Create a backup of current database data"""
        print(f"\n💾 Creating database backup...")
        
        try:
            # Create backup directory
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            
            with self.receipt_service.get_cursor() as cursor:
                # Check if receipts table exists and has data
                cursor.execute("""
                    SELECT COUNT(*) as count FROM receipts;
                """)
                receipt_count = cursor.fetchone()['count']
                
                if receipt_count == 0:
                    print("ℹ️ No data to backup (receipts table is empty)")
                    return True
                
                # Export receipts data
                cursor.execute("""
                    SELECT 
                        id, receipt_number, store_name, store_id, ticket_amount,
                        print_time, processing_date, created_at, updated_at,
                        source_file_path, original_filename
                    FROM receipts 
                    ORDER BY id;
                """)
                
                receipts = cursor.fetchall()
                
                # Save to CSV
                import csv
                backup_file = self.backup_dir / "receipts_backup.csv"
                
                with open(backup_file, 'w', newline='', encoding='utf-8') as f:
                    if receipts:
                        writer = csv.DictWriter(f, fieldnames=receipts[0].keys())
                        writer.writeheader()
                        for row in receipts:
                            # Convert date/datetime objects to strings
                            clean_row = {}
                            for key, value in row.items():
                                if hasattr(value, 'isoformat'):
                                    clean_row[key] = value.isoformat()
                                else:
                                    clean_row[key] = value
                            writer.writerow(clean_row)
                
                # Export daily stats if exists
                try:
                    cursor.execute("SELECT * FROM daily_stats ORDER BY processing_date;")
                    daily_stats = cursor.fetchall()
                    
                    if daily_stats:
                        stats_file = self.backup_dir / "daily_stats_backup.csv"
                        with open(stats_file, 'w', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=daily_stats[0].keys())
                            writer.writeheader()
                            for row in daily_stats:
                                clean_row = {}
                                for key, value in row.items():
                                    if hasattr(value, 'isoformat'):
                                        clean_row[key] = value.isoformat()
                                    else:
                                        clean_row[key] = value
                                writer.writerow(clean_row)
                        
                except Exception as e:
                    print(f"⚠️ Could not backup daily_stats: {e}")
                
                print(f"✅ Database backup created: {backup_file}")
                print(f"   📊 Backed up {receipt_count:,} receipts")
                return True
                
        except Exception as e:
            print(f"❌ Backup failed: {e}")
            return False
    
    def clear_database_tables(self) -> bool:
        """Clear all data from database tables"""
        print(f"\n🗑️ Clearing database tables...")
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                # Clear tables in order (foreign keys first if any)
                tables_to_clear = ['daily_stats', 'receipts']
                
                for table in tables_to_clear:
                    # Check if table exists
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM information_schema.tables 
                        WHERE table_name = %s;
                    """, (table,))
                    
                    if cursor.fetchone()['count'] > 0:
                        # Get count before deletion
                        cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
                        before_count = cursor.fetchone()['count']
                        
                        if before_count > 0:
                            # Delete all data
                            cursor.execute(f"DELETE FROM {table};")
                            
                            # Reset auto-increment if it's the receipts table
                            if table == 'receipts':
                                cursor.execute("ALTER SEQUENCE receipts_id_seq RESTART WITH 1;")
                            elif table == 'daily_stats':
                                cursor.execute("ALTER SEQUENCE daily_stats_id_seq RESTART WITH 1;")
                            
                            print(f"✅ Cleared {table}: {before_count:,} records deleted")
                        else:
                            print(f"ℹ️ {table}: Already empty")
                    else:
                        print(f"ℹ️ {table}: Table does not exist")
                
                # Commit changes
                self.receipt_service.connection.commit()
                print(f"✅ Database cleanup completed")
                return True
                
        except Exception as e:
            print(f"❌ Database cleanup failed: {e}")
            if self.receipt_service.connection:
                self.receipt_service.connection.rollback()
            return False
    
    def backup_processed_files(self) -> bool:
        """Backup processed files before moving them back"""
        print(f"\n📁 Backing up processed files...")
        
        inserted_path = Path(r"C:\Point Detection\worker\data\inserted_to_database")
        
        if not inserted_path.exists():
            print("ℹ️ No inserted_to_database directory found")
            return True
        
        json_files = list(inserted_path.rglob("*.json"))
        
        if not json_files:
            print("ℹ️ No files in inserted_to_database directory")
            return True
        
        try:
            # Create file backup directory
            file_backup_dir = self.backup_dir / "processed_files"
            file_backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy the entire structure
            if inserted_path.exists():
                backup_dest = file_backup_dir / "inserted_to_database"
                shutil.copytree(inserted_path, backup_dest, dirs_exist_ok=True)
                
                print(f"✅ Backed up {len(json_files)} processed files to: {file_backup_dir}")
                return True
                
        except Exception as e:
            print(f"❌ File backup failed: {e}")
            return False
    
    def move_files_back_to_converted_tz(self) -> bool:
        """Move files from inserted_to_database back to converted_tz"""
        print(f"\n🔄 Moving files back to converted_tz...")
        
        inserted_path = Path(r"C:\Point Detection\worker\data\inserted_to_database")
        converted_tz_path = Path(r"C:\Point Detection\worker\data\converted_tz")
        
        if not inserted_path.exists():
            print("ℹ️ No inserted_to_database directory found")
            return True
        
        # Create converted_tz directory if it doesn't exist
        converted_tz_path.mkdir(parents=True, exist_ok=True)
        
        moved_count = 0
        
        try:
            # Move files back maintaining date folder structure
            for date_folder in inserted_path.iterdir():
                if date_folder.is_dir():
                    # Create corresponding date folder in converted_tz
                    dest_date_folder = converted_tz_path / date_folder.name
                    dest_date_folder.mkdir(parents=True, exist_ok=True)
                    
                    # Move all JSON files
                    for json_file in date_folder.glob("*.json"):
                        dest_file = dest_date_folder / json_file.name
                        
                        # Handle name conflicts
                        if dest_file.exists():
                            timestamp = datetime.now().strftime("%H%M%S%f")[:9]
                            stem = json_file.stem
                            suffix = json_file.suffix
                            dest_file = dest_date_folder / f"{stem}_restored_{timestamp}{suffix}"
                        
                        shutil.move(str(json_file), str(dest_file))
                        moved_count += 1
                    
                    # Remove empty date folder
                    if not any(date_folder.iterdir()):
                        date_folder.rmdir()
            
            # Remove empty inserted_to_database folder if completely empty
            if inserted_path.exists() and not any(inserted_path.iterdir()):
                inserted_path.rmdir()
            
            print(f"✅ Moved {moved_count} files back to converted_tz")
            return True
            
        except Exception as e:
            print(f"❌ Failed to move files back: {e}")
            return False
    
    def full_cleanup(self, skip_backup: bool = False) -> bool:
        """Perform complete cleanup - database and files"""
        print("\n" + "="*80)
        print("🧹 STARTING FULL CLEANUP")
        print("="*80)
        
        success = True
        
        # Step 1: Show current state
        self.show_current_state()
        
        # Step 2: Create backups (unless skipped)
        if not skip_backup:
            if not self.backup_database():
                print("⚠️ Database backup failed, but continuing...")
            
            if not self.backup_processed_files():
                print("⚠️ File backup failed, but continuing...")
        else:
            print("⚠️ Skipping backups as requested")
        
        # Step 3: Move files back to converted_tz
        if not self.move_files_back_to_converted_tz():
            print("⚠️ Failed to move files back, but continuing...")
            success = False
        
        # Step 4: Clear database
        if not self.clear_database_tables():
            print("❌ Database cleanup failed")
            success = False
        
        # Step 5: Show final state
        print(f"\n" + "="*80)
        print("✅ CLEANUP COMPLETED" if success else "⚠️ CLEANUP COMPLETED WITH WARNINGS")
        print("="*80)
        
        self.show_current_state()
        
        if not skip_backup:
            print(f"\n💾 Backups saved to: {self.backup_dir}")
        
        return success
    
    def cleanup(self):
        """Cleanup database connection"""
        if self.receipt_service:
            self.receipt_service.disconnect()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Database and File System Cleanup Utility")
    parser.add_argument('--show-only', action='store_true', 
                       help='Only show current state, do not perform cleanup')
    parser.add_argument('--skip-backup', action='store_true',
                       help='Skip creating backups (faster but less safe)')
    parser.add_argument('--database-only', action='store_true',
                       help='Only clear database, do not move files')
    parser.add_argument('--files-only', action='store_true',
                       help='Only move files back, do not clear database')
    parser.add_argument('--force', action='store_true',
                       help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    cleanup = DatabaseCleanup()
    
    try:
        # Connect to database
        if not cleanup.connect():
            print("❌ Failed to connect to database")
            return 1
        
        # Show only mode
        if args.show_only:
            cleanup.show_current_state()
            return 0
        
        # Confirmation prompt unless forced
        if not args.force:
            cleanup.show_current_state()
            print(f"\n" + "="*80)
            print("⚠️  WARNING: This will clear all receipt data and move files back!")
            print("="*80)
            
            if not args.skip_backup:
                print("✅ Backups will be created before cleanup")
            else:
                print("❌ No backups will be created (--skip-backup)")
            
            response = input("\nContinue with cleanup? (yes/no): ").lower().strip()
            if response not in ['yes', 'y']:
                print("❌ Cleanup cancelled")
                return 0
        
        # Perform specific cleanup based on arguments
        if args.database_only:
            success = cleanup.clear_database_tables()
        elif args.files_only:
            success = cleanup.move_files_back_to_converted_tz()
        else:
            success = cleanup.full_cleanup(skip_backup=args.skip_backup)
        
        if success:
            print(f"\n🎉 Ready for fresh start! You can now run:")
            print(f"   python app/workers/enhanced_realtime_database_inserter.py --process-all")
            return 0
        else:
            print(f"\n⚠️ Cleanup completed with some issues")
            return 1
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        cleanup.cleanup()

if __name__ == "__main__":
    exit(main())

# Quick usage examples:
# python database_cleanup.py --show-only                    # Just show current state
# python database_cleanup.py                               # Full cleanup with backups
# python database_cleanup.py --skip-backup --force         # Fast cleanup, no prompts
# python database_cleanup.py --database-only               # Only clear database
# python database_cleanup.py --files-only                  # Only move files back