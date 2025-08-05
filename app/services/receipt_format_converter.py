# app/services/receipt_format_converter.py - Converts various receipt formats to standard format
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import re

# Add project paths
current_file = Path(__file__)
app_dir = current_file.parent.parent
project_root = app_dir.parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

class ReceiptFormatConverter:
    """Converts various receipt data formats to standardized format for timezone processing"""
    
    def __init__(self):
        self.debug_mode = True
        
        # Standard field names that timezone worker expects
        self.standard_fields = {
            'number': 'number',
            'store_name': 'store_name', 
            'store_id': 'store_id',
            'ticketAmount': 'ticketAmount',
            'print_time': 'print_time'
        }
        
        # Mapping of various field names to standard ones
        self.field_mappings = {
            # Number field variants
            'number': ['number', 'receipt_number', 'receiptNumber', 'receiptNo', 'receipt_no', 
                      'ticketNumber', 'ticket_number', 'id', 'receipt_id', 'receiptId',
                      'transaction_id', 'transactionId', 'order_number', 'orderNumber',
                      'invoice_number', 'invoiceNumber', 'bill_number', 'billNumber',
                      'ref_number', 'refNumber', 'reference_number', 'referenceNumber'],
            
            # Store name variants
            'store_name': ['store_name', 'storeName', 'shop_name', 'shopName', 
                          'merchant_name', 'merchantName', 'business_name', 'businessName',
                          'company_name', 'companyName', 'retailer_name', 'retailerName',
                          'outlet_name', 'outletName', 'branch_name', 'branchName'],
            
            # Store ID variants  
            'store_id': ['store_id', 'storeId', 'shop_code', 'shopCode', 'store_code', 'storeCode',
                        'merchant_id', 'merchantId', 'business_id', 'businessId',
                        'outlet_id', 'outletId', 'branch_id', 'branchId', 'location_id', 'locationId'],
            
            # Amount variants
            'ticketAmount': ['ticketAmount', 'ticket_amount', 'totalAmount', 'total_amount', 
                           'total', 'amount', 'grandTotal', 'grand_total', 'final_amount', 'finalAmount',
                           'net_amount', 'netAmount', 'payable_amount', 'payableAmount',
                           'invoice_total', 'invoiceTotal', 'bill_total', 'billTotal',
                           'subtotal', 'sub_total', 'sum', 'price', 'cost'],
            
            # Time variants
            'print_time': ['print_time', 'printTime', 'timestamp', 'dateTime', 'date_time',
                          'created_at', 'createdAt', 'transaction_time', 'transactionTime',
                          'purchase_time', 'purchaseTime', 'sale_time', 'saleTime',
                          'issued_at', 'issuedAt', 'receipt_time', 'receiptTime',
                          'order_time', 'orderTime', 'billing_time', 'billingTime']
        }
    
    def detect_receipt_format(self, data: Dict[str, Any]) -> str:
        """Detect what format the receipt data is in"""
        
        if not isinstance(data, dict):
            return "invalid"
        
        # Check for already standardized format
        standard_fields_found = sum(1 for field in self.standard_fields.keys() if field in data)
        if standard_fields_found >= 4:
            return "standard"
        
        # Check for response data format (nested in 'record')
        if 'record' in data and isinstance(data['record'], dict):
            return "response_nested"
        
        # Check for various receipt data formats
        receipt_like_fields = 0
        for standard_field, variants in self.field_mappings.items():
            for variant in variants:
                if variant in data and data[variant] not in [None, '', 'unknown', 'null']:
                    receipt_like_fields += 1
                    break
        
        if receipt_like_fields >= 3:
            return "variant_fields"
        
        # Check for raw response format
        if any(key in data for key in ['new_receipts', 'receipts', 'response_data']):
            return "raw_response"
        
        # Check for OCR data
        if 'ocr_metadata' in data or ('data' in data and isinstance(data['data'], str) and len(data['data']) > 100):
            return "ocr_data"
        
        return "unknown"
    
    def convert_to_standard_format(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert receipt data to standard format that timezone worker expects"""
        
        try:
            format_type = self.detect_receipt_format(data)
            
            if self.debug_mode:
                print(f"   🔍 Detected format: {format_type}")
            
            if format_type == "standard":
                if self.debug_mode:
                    print(f"   ✅ Already in standard format")
                return data
            
            elif format_type == "ocr_data":
                if self.debug_mode:
                    print(f"   ❌ Cannot convert OCR data to receipt format")
                return None
            
            elif format_type == "invalid":
                if self.debug_mode:
                    print(f"   ❌ Invalid data format")
                return None
            
            # Convert based on detected format
            if format_type == "response_nested":
                return self._convert_nested_response(data)
            elif format_type == "variant_fields":
                return self._convert_variant_fields(data)
            elif format_type == "raw_response":
                return self._convert_raw_response(data)
            else:
                return self._attempt_generic_conversion(data)
                
        except Exception as e:
            if self.debug_mode:
                print(f"   ❌ Error converting format: {e}")
            return None
    
    def _convert_nested_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert response data with nested 'record' field"""
        if self.debug_mode:
            print(f"   🔄 Converting nested response format...")
        
        record_data = data.get('record', {})
        if not isinstance(record_data, dict):
            return None
        
        return self._extract_standard_fields(record_data)
    
    def _convert_variant_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data with variant field names to standard names"""
        if self.debug_mode:
            print(f"   🔄 Converting variant field names...")
        
        return self._extract_standard_fields(data)
    
    def _convert_raw_response(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert raw response format with receipt arrays"""
        if self.debug_mode:
            print(f"   🔄 Converting raw response format...")
        
        # Try to extract from new_receipts array
        if 'new_receipts' in data and isinstance(data['new_receipts'], list) and data['new_receipts']:
            receipt_data = data['new_receipts'][0]  # Take first receipt
            return self._extract_standard_fields(receipt_data)
        
        # Try to extract from receipts array
        if 'receipts' in data and isinstance(data['receipts'], list) and data['receipts']:
            receipt_data = data['receipts'][0]  # Take first receipt
            return self._extract_standard_fields(receipt_data)
        
        return None
    
    def _attempt_generic_conversion(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Attempt generic conversion for unknown formats"""
        if self.debug_mode:
            print(f"   🔄 Attempting generic conversion...")
        
        # Try to extract any recognizable fields
        converted = self._extract_standard_fields(data)
        
        # Check if we got enough fields to be useful
        valid_fields = sum(1 for v in converted.values() if v != 'unknown')
        if valid_fields >= 2:  # Need at least 2 valid fields
            return converted
        
        return None
    
    def _extract_standard_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and standardize fields from any receipt data structure"""
        
        standard_data = {
            'number': 'unknown',
            'store_name': 'unknown',
            'store_id': 'unknown', 
            'ticketAmount': 'unknown',
            'print_time': 'unknown'
        }
        
        # Extract each standard field using variant mappings
        for standard_field, variants in self.field_mappings.items():
            for variant in variants:
                if variant in data and data[variant] not in [None, '', 'unknown', 'null']:
                    value = data[variant]
                    
                    # Special handling for different field types
                    if standard_field == 'ticketAmount':
                        # Convert amount to number if possible
                        if isinstance(value, (int, float)):
                            standard_data[standard_field] = value
                        elif isinstance(value, str):
                            # Try to extract numeric value from string
                            numeric_value = self._extract_numeric_amount(value)
                            standard_data[standard_field] = numeric_value if numeric_value is not None else value
                        else:
                            standard_data[standard_field] = str(value)
                    elif standard_field == 'print_time':
                        # Standardize time format
                        standard_data[standard_field] = self._standardize_time_format(str(value))
                    else:
                        # For other fields, convert to string
                        standard_data[standard_field] = str(value)
                    
                    break  # Found a value, stop looking for variants
        
        # Add conversion metadata
        standard_data['_converted_by'] = 'format_converter'
        standard_data['_converted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        standard_data['_original_format'] = self.detect_receipt_format(data)
        
        return standard_data
    
    def _extract_numeric_amount(self, amount_str: str) -> Optional[float]:
        """Extract numeric amount from string"""
        try:
            # Remove common currency symbols and whitespace
            cleaned = re.sub(r'[^\d.,\-]', '', str(amount_str))
            
            # Handle different decimal separators
            if ',' in cleaned and '.' in cleaned:
                # Assume comma is thousands separator
                cleaned = cleaned.replace(',', '')
            elif ',' in cleaned and cleaned.count(',') == 1:
                # Check if comma might be decimal separator
                parts = cleaned.split(',')
                if len(parts[1]) <= 2:  # Likely decimal separator
                    cleaned = cleaned.replace(',', '.')
            
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def _standardize_time_format(self, time_str: str) -> str:
        """Standardize time format to YYYY-MM-DD HH:MM:SS"""
        try:
            # Common time formats to try
            time_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y%m%d_%H%M%S',
                '%Y-%m-%d',
                '%m/%d/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S',
                '%Y/%m/%d %H:%M:%S'
            ]
            
            for fmt in time_formats:
                try:
                    dt = datetime.strptime(time_str, fmt)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            
            # If no format matched, return original
            return time_str
            
        except Exception:
            return time_str
    
    def validate_converted_data(self, data: Dict[str, Any]) -> bool:
        """Validate that converted data has minimum required fields"""
        
        if not isinstance(data, dict):
            return False
        
        # Check for minimum required fields
        required_fields = ['number', 'print_time']
        for field in required_fields:
            if field not in data or data[field] == 'unknown':
                return False
        
        # Check that at least 3 out of 5 standard fields are present
        valid_fields = sum(1 for field in self.standard_fields.keys() 
                          if field in data and data[field] != 'unknown')
        
        return valid_fields >= 3
    
    def convert_with_validation(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert data to standard format with validation"""
        
        if self.debug_mode:
            print(f"   🔄 Starting format conversion...")
        
        # Attempt conversion
        converted_data = self.convert_to_standard_format(data)
        
        if converted_data is None:
            if self.debug_mode:
                print(f"   ❌ Conversion failed")
            return None
        
        # Validate converted data
        if not self.validate_converted_data(converted_data):
            if self.debug_mode:
                print(f"   ❌ Converted data failed validation")
            return None
        
        if self.debug_mode:
            print(f"   ✅ Successfully converted to standard format")
            print(f"       Number: {converted_data.get('number', 'unknown')}")
            print(f"       Store: {converted_data.get('store_name', 'unknown')}")
            print(f"       Amount: {converted_data.get('ticketAmount', 'unknown')}")
            print(f"       Time: {converted_data.get('print_time', 'unknown')}")
        
        return converted_data

# Global converter instance
receipt_format_converter = ReceiptFormatConverter()