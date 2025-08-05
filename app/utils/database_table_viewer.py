# database_table_viewer.py - Utility to view database table structure and contents
import sys
import os
from pathlib import Path
from datetime import datetime
import argparse
from typing import List, Dict, Any
import json

# Add project paths (same as your enhanced_realtime_database_inserter.py)
current_dir = Path(__file__).parent
app_dir = current_dir.parent if current_dir.name == 'workers' else current_dir
project_root = app_dir.parent if app_dir.name == 'app' else app_dir.parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from services.database.receipt_service import ReceiptService

class DatabaseTableViewer:
    """Utility to view and analyze database table contents"""
    
    def __init__(self):
        self.receipt_service = ReceiptService()
    
    def connect(self) -> bool:
        """Connect to database"""
        return self.receipt_service.connect()
    
    def show_table_structure(self):
        """Show the structure of the receipts table"""
        print("\n" + "="*80)
        print("🏗️  DATABASE TABLE STRUCTURE")
        print("="*80)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                # Get table structure (PostgreSQL specific)
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = 'receipts' 
                    ORDER BY ordinal_position;
                """)
                
                columns = cursor.fetchall()
                
                if not columns:
                    print("❌ No 'receipts' table found or no columns detected")
                    return
                
                print(f"📋 Table: receipts")
                print(f"📊 Total columns: {len(columns)}")
                print("\n" + "-"*80)
                print(f"{'COLUMN NAME':<25} {'TYPE':<20} {'NULL?':<8} {'DEFAULT':<15} {'LENGTH':<10}")
                print("-"*80)
                
                for col in columns:
                    column_name = col['column_name']
                    data_type = col['data_type']
                    is_nullable = 'YES' if col['is_nullable'] == 'YES' else 'NO'
                    default_val = str(col['column_default'])[:14] if col['column_default'] else 'None'
                    max_length = str(col['character_maximum_length']) if col['character_maximum_length'] else 'N/A'
                    
                    print(f"{column_name:<25} {data_type:<20} {is_nullable:<8} {default_val:<15} {max_length:<10}")
                
                print("-"*80)
                
        except Exception as e:
            print(f"❌ Error getting table structure: {e}")
    
    def show_table_stats(self):
        """Show basic statistics about the table"""
        print("\n" + "="*80)
        print("📊 DATABASE TABLE STATISTICS")
        print("="*80)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                # Total count
                cursor.execute("SELECT COUNT(*) as total FROM receipts;")
                total_count = cursor.fetchone()['total']
                
                # Count by processing date
                cursor.execute("""
                    SELECT processing_date, COUNT(*) as count 
                    FROM receipts 
                    GROUP BY processing_date 
                    ORDER BY processing_date DESC 
                    LIMIT 10;
                """)
                date_counts = cursor.fetchall()
                
                # Latest entries
                cursor.execute("""
                    SELECT processing_date, receipt_number, store_name, ticket_amount 
                    FROM receipts 
                    ORDER BY id DESC 
                    LIMIT 5;
                """)
                latest_entries = cursor.fetchall()
                
                # Amount statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as count,
                        AVG(ticket_amount) as avg_amount,
                        MIN(ticket_amount) as min_amount,
                        MAX(ticket_amount) as max_amount,
                        SUM(ticket_amount) as total_amount
                    FROM receipts 
                    WHERE ticket_amount IS NOT NULL;
                """)
                amount_stats = cursor.fetchone()
                
                print(f"📈 Total receipts: {total_count:,}")
                
                if amount_stats['count'] > 0:
                    print(f"💰 Amount statistics:")
                    print(f"   Average: ${amount_stats['avg_amount']:.2f}")
                    print(f"   Minimum: ${amount_stats['min_amount']:.2f}")
                    print(f"   Maximum: ${amount_stats['max_amount']:.2f}")
                    print(f"   Total: ${amount_stats['total_amount']:.2f}")
                
                print(f"\n📅 Recent dates (top 10):")
                for date_count in date_counts:
                    print(f"   {date_count['processing_date']}: {date_count['count']:,} receipts")
                
                print(f"\n🆕 Latest 5 entries:")
                print(f"{'DATE':<12} {'RECEIPT #':<15} {'STORE':<20} {'AMOUNT':<10}")
                print("-"*60)
                for entry in latest_entries:
                    store_name = (entry['store_name'] or 'Unknown')[:19]
                    amount = f"${entry['ticket_amount']:.2f}" if entry['ticket_amount'] else "N/A"
                    print(f"{entry['processing_date']:<12} {entry['receipt_number']:<15} {store_name:<20} {amount:<10}")
                
        except Exception as e:
            print(f"❌ Error getting table statistics: {e}")
    
    def show_sample_data(self, limit: int = 10):
        """Show sample data from the table"""
        print(f"\n" + "="*120)
        print(f"📋 SAMPLE DATA (First {limit} rows)")
        print("="*120)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                cursor.execute(f"""
                    SELECT * FROM receipts 
                    ORDER BY id ASC 
                    LIMIT {limit};
                """)
                
                rows = cursor.fetchall()
                
                if not rows:
                    print("📭 No data found in receipts table")
                    return
                
                # Get column names
                column_names = list(rows[0].keys()) if rows else []
                
                # Print header
                header = " | ".join([f"{col:<15}" for col in column_names])
                print(header)
                print("-" * len(header))
                
                # Print rows
                for row in rows:
                    row_data = []
                    for col in column_names:
                        value = row[col]
                        if value is None:
                            display_value = "NULL"
                        elif isinstance(value, (dict, list)):
                            display_value = json.dumps(value)[:13] + "..."
                        else:
                            display_value = str(value)[:15]
                        row_data.append(f"{display_value:<15}")
                    
                    print(" | ".join(row_data))
                
        except Exception as e:
            print(f"❌ Error getting sample data: {e}")
    
    def show_data_by_date(self, date_str: str, limit: int = 20):
        """Show data for a specific date"""
        print(f"\n" + "="*120)
        print(f"📅 DATA FOR DATE: {date_str} (Limit: {limit})")
        print("="*120)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        id, receipt_number, store_name, ticket_amount, 
                        processed_at, file_path
                    FROM receipts 
                    WHERE processing_date = %s
                    ORDER BY id DESC
                    LIMIT {limit};
                """, (date_str,))
                
                rows = cursor.fetchall()
                
                if not rows:
                    print(f"📭 No data found for date: {date_str}")
                    return
                
                print(f"📊 Found {len(rows)} receipts for {date_str}")
                print()
                
                # Print formatted data
                print(f"{'ID':<6} {'RECEIPT #':<20} {'STORE':<25} {'AMOUNT':<12} {'PROCESSED AT':<20}")
                print("-"*90)
                
                for row in rows:
                    store_name = (row['store_name'] or 'Unknown')[:24]
                    amount = f"${row['ticket_amount']:.2f}" if row['ticket_amount'] else "N/A"
                    processed_at = row['processed_at'].strftime('%Y-%m-%d %H:%M:%S') if row['processed_at'] else 'N/A'
                    
                    print(f"{row['id']:<6} {row['receipt_number']:<20} {store_name:<25} {amount:<12} {processed_at:<20}")
                
        except Exception as e:
            print(f"❌ Error getting data for date {date_str}: {e}")
    
    def search_receipts(self, search_term: str, limit: int = 20):
        """Search for receipts containing a term"""
        print(f"\n" + "="*120)
        print(f"🔍 SEARCH RESULTS FOR: '{search_term}' (Limit: {limit})")
        print("="*120)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        id, processing_date, receipt_number, store_name, 
                        ticket_amount, processed_at
                    FROM receipts 
                    WHERE 
                        receipt_number ILIKE %s OR 
                        store_name ILIKE %s OR
                        CAST(ticket_amount AS TEXT) ILIKE %s
                    ORDER BY id DESC
                    LIMIT {limit};
                """, (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
                
                rows = cursor.fetchall()
                
                if not rows:
                    print(f"📭 No receipts found matching: '{search_term}'")
                    return
                
                print(f"📊 Found {len(rows)} matching receipts")
                print()
                
                # Print formatted results
                print(f"{'ID':<6} {'DATE':<12} {'RECEIPT #':<20} {'STORE':<25} {'AMOUNT':<12}")
                print("-"*82)
                
                for row in rows:
                    store_name = (row['store_name'] or 'Unknown')[:24]
                    amount = f"${row['ticket_amount']:.2f}" if row['ticket_amount'] else "N/A"
                    
                    print(f"{row['id']:<6} {row['processing_date']:<12} {row['receipt_number']:<20} {store_name:<25} {amount:<12}")
                
        except Exception as e:
            print(f"❌ Error searching for '{search_term}': {e}")
    
    def show_full_record(self, record_id: int):
        """Show complete details for a specific record"""
        print(f"\n" + "="*80)
        print(f"🔍 COMPLETE RECORD DETAILS (ID: {record_id})")
        print("="*80)
        
        try:
            with self.receipt_service.get_cursor() as cursor:
                cursor.execute("SELECT * FROM receipts WHERE id = %s;", (record_id,))
                row = cursor.fetchone()
                
                if not row:
                    print(f"❌ No record found with ID: {record_id}")
                    return
                
                # Print all fields
                for key, value in row.items():
                    if isinstance(value, (dict, list)):
                        print(f"{key:<20}: {json.dumps(value, indent=2)}")
                    elif isinstance(value, datetime):
                        print(f"{key:<20}: {value.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        print(f"{key:<20}: {value}")
                
        except Exception as e:
            print(f"❌ Error getting record {record_id}: {e}")
    
    def cleanup(self):
        """Cleanup database connection"""
        if self.receipt_service:
            self.receipt_service.disconnect()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Database Table Viewer for Receipt Data")
    parser.add_argument('--structure', action='store_true', help='Show table structure')
    parser.add_argument('--stats', action='store_true', help='Show table statistics')
    parser.add_argument('--sample', type=int, default=10, help='Show sample data (default: 10 rows)')
    parser.add_argument('--date', type=str, help='Show data for specific date (YYYY-MM-DD)')
    parser.add_argument('--search', type=str, help='Search for receipts containing term')
    parser.add_argument('--record', type=int, help='Show complete details for specific record ID')
    parser.add_argument('--limit', type=int, default=20, help='Limit results (default: 20)')
    parser.add_argument('--all', action='store_true', help='Show everything (structure, stats, sample)')
    
    args = parser.parse_args()
    
    # Create viewer
    viewer = DatabaseTableViewer()
    
    try:
        # Connect to database
        if not viewer.connect():
            print("❌ Failed to connect to database")
            return 1
        
        print("✅ Connected to database successfully")
        
        # Show everything if --all is specified
        if args.all:
            viewer.show_table_structure()
            viewer.show_table_stats()
            viewer.show_sample_data(args.sample)
        else:
            # Show specific views based on arguments
            if args.structure:
                viewer.show_table_structure()
            
            if args.stats:
                viewer.show_table_stats()
            
            if args.sample and not (args.date or args.search or args.record):
                viewer.show_sample_data(args.sample)
            
            if args.date:
                viewer.show_data_by_date(args.date, args.limit)
            
            if args.search:
                viewer.search_receipts(args.search, args.limit)
            
            if args.record:
                viewer.show_full_record(args.record)
            
            # If no specific arguments, show basic info
            if not any([args.structure, args.stats, args.sample, args.date, 
                       args.search, args.record]):
                print("\n🔍 No specific view requested. Showing basic overview:")
                viewer.show_table_structure()
                viewer.show_table_stats()
                viewer.show_sample_data(5)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        viewer.cleanup()

if __name__ == "__main__":
    exit(main())

# Usage examples:
# python database_table_viewer.py --all                    # Show everything
# python database_table_viewer.py --structure              # Show table structure only
# python database_table_viewer.py --stats                  # Show statistics only
# python database_table_viewer.py --sample 20              # Show 20 sample records
# python database_table_viewer.py --date 2025-07-29       # Show data for specific date
# python database_table_viewer.py --search "Starbucks"     # Search for receipts
# python database_table_viewer.py --record 123             # Show complete record #123