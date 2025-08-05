# app/services/database/receipt_processing_service.py
"""
Receipt processing service for handling receipt data operations
"""
import json
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional, List, Any
from decimal import Decimal, InvalidOperation
from .database_connection_service import DatabaseConnectionService


class ReceiptProcessingService:
    """Handles receipt data processing and database operations"""
    
    def __init__(self, connection_service: DatabaseConnectionService):
        self.db = connection_service
        self.logger = logging.getLogger(f"{__name__}.ReceiptProcessingService")
    
    def parse_json_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Parse JSON file and extract receipt data"""
        try:
            if not file_path.exists():
                self.logger.error(f"File does not exist: {file_path}")
                return None
            
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Validate required fields
            if not self._validate_receipt_data(data):
                return None
            
            # Extract and normalize receipt data
            receipt_data = self._extract_receipt_data(data)
            receipt_data['raw_data'] = data  # Store original JSON
            
            return receipt_data
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in file {file_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
            return None
    
    def _validate_receipt_data(self, data: Dict) -> bool:
        """Validate that receipt data contains required fields"""
        # Check for receipt number in multiple possible field names
        receipt_number_fields = ['receipt_number', 'number']
        has_receipt_number = any(field in data and data[field] for field in receipt_number_fields)
        
        if not has_receipt_number:
            self.logger.error(f"Missing required field: receipt_number (checked: {receipt_number_fields})")
            return False
        
        return True
    
    def _extract_receipt_data(self, data: Dict) -> Dict[str, Any]:
        """Extract and normalize receipt data from JSON (handles your actual format)"""
        receipt_data = {}
        
        # Handle receipt number (multiple possible field names)
        receipt_data['receipt_number'] = (
            data.get('receipt_number') or 
            data.get('number') or 
            ''
        ).strip()
        
        # Handle store information
        receipt_data['store_name'] = str(data.get('store_name', '')).strip() or None
        receipt_data['store_id'] = str(data.get('store_id', '')).strip() or None
        
        # Handle ticket amount (multiple possible field names)
        ticket_amount = data.get('ticket_amount') or data.get('ticketAmount')
        receipt_data['ticket_amount'] = self._parse_decimal(ticket_amount)
        
        # Handle print time (extract time portion from datetime string)
        print_time = data.get('print_time')
        receipt_data['print_time'] = self._parse_time_from_datetime(print_time)
        
        # Store additional fields
        receipt_data['original_print_time'] = data.get('original_print_time')
        receipt_data['timezone_conversion'] = data.get('timezone_conversion')
        
        return receipt_data
    
    def _parse_decimal(self, value: Any) -> Optional[Decimal]:
        """Parse decimal value safely"""
        if value is None:
            return None
        
        try:
            # Handle string values that might have currency symbols
            if isinstance(value, str):
                # Remove common currency symbols and whitespace
                cleaned = value.replace('$', '').replace(',', '').strip()
                if not cleaned:
                    return None
                return Decimal(cleaned)
            
            # Handle numeric values
            return Decimal(str(value))
            
        except (InvalidOperation, ValueError, TypeError):
            self.logger.warning(f"Could not parse decimal value: {value}")
            return None
    
    def _parse_time_from_datetime(self, value: Any) -> Optional[datetime.time]:
        """Parse time from datetime string (handles your format: '2025-07-30 09:27:42')"""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
                
                # Try parsing full datetime and extract time
                datetime_formats = [
                    '%Y-%m-%d %H:%M:%S',     # 2025-07-30 09:27:42
                    '%Y-%m-%d %H:%M',        # 2025-07-30 09:27
                    '%d/%m/%Y %H:%M:%S',     # 30/07/2025 09:27:42
                    '%d/%m/%Y %H:%M',        # 30/07/2025 09:27
                ]
                
                for fmt in datetime_formats:
                    try:
                        parsed_datetime = datetime.strptime(value, fmt)
                        return parsed_datetime.time()
                    except ValueError:
                        continue
                
                # If datetime parsing fails, try just time formats
                time_formats = [
                    '%H:%M:%S',
                    '%H:%M',
                    '%I:%M:%S %p',
                    '%I:%M %p'
                ]
                
                for fmt in time_formats:
                    try:
                        parsed_time = datetime.strptime(value, fmt).time()
                        return parsed_time
                    except ValueError:
                        continue
                
                self.logger.warning(f"Could not parse datetime/time format: {value}")
                return None
            
            # Handle datetime objects
            elif isinstance(value, datetime):
                return value.time()
            
            # Handle time objects
            elif hasattr(value, 'hour'):
                return value
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error parsing datetime/time {value}: {e}")
            return None
    
    def insert_receipt(self, receipt_data: Dict[str, Any]) -> Optional[int]:
        """Insert receipt into database and return receipt ID"""
        try:
            # Ensure processing_date is set
            if 'processing_date' not in receipt_data:
                receipt_data['processing_date'] = date.today()
            
            insert_query = """
            INSERT INTO receipts 
            (receipt_number, store_name, ticket_amount, print_time, processing_date, raw_data, store_id, original_print_time, timezone_conversion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
            
            params = (
                receipt_data['receipt_number'],
                receipt_data.get('store_name'),
                receipt_data.get('ticket_amount'),
                receipt_data.get('print_time'),
                receipt_data['processing_date'],
                json.dumps(receipt_data.get('raw_data', {})),
                receipt_data.get('store_id'),
                receipt_data.get('original_print_time'),
                receipt_data.get('timezone_conversion')
            )
            
            with self.db.get_cursor() as cursor:
                cursor.execute(insert_query, params)
                result = cursor.fetchone()
                receipt_id = result['id'] if result else None
                
                if receipt_id:
                    self.db.commit()
                    self.logger.info(f"Inserted receipt {receipt_data['receipt_number']} with ID {receipt_id}")
                    return receipt_id
                else:
                    self.db.rollback()
                    return None
                    
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Error inserting receipt {receipt_data.get('receipt_number', 'Unknown')}: {e}")
            return None
    
    def check_duplicate(self, receipt_number: str, processing_date: date) -> bool:
        """Check if receipt already exists for the given date"""
        try:
            query = """
            SELECT id FROM receipts 
            WHERE receipt_number = %s AND processing_date = %s
            LIMIT 1;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (receipt_number, processing_date))
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            self.logger.error(f"Error checking duplicate for {receipt_number}: {e}")
            return False
    
    def get_receipt_by_id(self, receipt_id: int) -> Optional[Dict]:
        """Get receipt by ID"""
        try:
            query = """
            SELECT * FROM receipts WHERE id = %s;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (receipt_id,))
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            self.logger.error(f"Error getting receipt {receipt_id}: {e}")
            return None
    
    def get_receipts_by_date(self, processing_date: date) -> List[Dict]:
        """Get all receipts for a specific date"""
        try:
            query = """
            SELECT * FROM receipts 
            WHERE processing_date = %s
            ORDER BY created_at DESC;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (processing_date,))
                results = cursor.fetchall()
                return [dict(row) for row in results] if results else []
                
        except Exception as e:
            self.logger.error(f"Error getting receipts for date {processing_date}: {e}")
            return []
    
    def get_receipt_count(self) -> int:
        """Get total receipt count"""
        try:
            query = "SELECT COUNT(*) as count FROM receipts;"
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result['count'] if result else 0
                
        except Exception as e:
            self.logger.error(f"Error getting receipt count: {e}")
            return 0
    
    def get_receipt_count_by_date(self, processing_date: date) -> int:
        """Get receipt count for a specific date"""
        try:
            query = """
            SELECT COUNT(*) as count FROM receipts 
            WHERE processing_date = %s;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (processing_date,))
                result = cursor.fetchone()
                return result['count'] if result else 0
                
        except Exception as e:
            self.logger.error(f"Error getting receipt count for {processing_date}: {e}")
            return 0
    
    def update_daily_stats(self, processing_date: date) -> bool:
        """Update daily statistics for a specific date"""
        try:
            # Calculate stats for the date
            stats_query = """
            SELECT 
                COUNT(*) as total_receipts,
                COALESCE(SUM(ticket_amount), 0) as total_amount
            FROM receipts 
            WHERE processing_date = %s;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(stats_query, (processing_date,))
                stats = cursor.fetchone()
                
                if not stats:
                    return False
                
                # Upsert daily stats
                upsert_query = """
                INSERT INTO daily_stats (processing_date, total_receipts, total_amount)
                VALUES (%s, %s, %s)
                ON CONFLICT (processing_date)
                DO UPDATE SET 
                    total_receipts = EXCLUDED.total_receipts,
                    total_amount = EXCLUDED.total_amount,
                    updated_at = CURRENT_TIMESTAMP;
                """
                
                cursor.execute(upsert_query, (
                    processing_date,
                    stats['total_receipts'],
                    stats['total_amount']
                ))
                
                self.db.commit()
                self.logger.info(f"Updated daily stats for {processing_date}")
                return True
                
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Error updating daily stats for {processing_date}: {e}")
            return False
    
    def get_daily_stats(self, processing_date: date) -> Optional[Dict]:
        """Get daily statistics for a specific date"""
        try:
            query = """
            SELECT * FROM daily_stats 
            WHERE processing_date = %s;
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (processing_date,))
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            self.logger.error(f"Error getting daily stats for {processing_date}: {e}")
            return None