# app/services/receipt_matcher/matching_service.py - FIXED with robust field detection
import time
from typing import Dict, Any, Optional, List

class MatchingService:
    """Enhanced Receipt matching service with robust field detection"""
    
    def __init__(self):
        # Add debugging to track matching attempts
        self.debug_mode = True
        
    def extract_receipt_fields(self, response_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract key fields from response data - ENHANCED with more robust detection"""
        fields = {
            'number': 'unknown',
            'store_name': 'unknown', 
            'store_id': 'unknown',
            'ticketAmount': 'unknown',
            'print_time': 'unknown'
        }
        
        # Handle different JSON structures
        data_to_check = response_data
        
        # If there's a 'record' field, prioritize that
        if isinstance(response_data, dict) and 'record' in response_data:
            data_to_check = response_data['record']
        
        if isinstance(data_to_check, dict):
            # Enhanced number extraction - try ALL possible field names
            number_fields = [
                'number', 'receipt_number', 'receiptNumber', 'receiptNo', 
                'receipt_no', 'ticketNumber', 'ticket_number', 'id', 
                'receipt_id', 'receiptId', 'transaction_id', 'transactionId',
                'order_number', 'orderNumber', 'invoice_number', 'invoiceNumber',
                'bill_number', 'billNumber', 'ref_number', 'refNumber',
                'reference_number', 'referenceNumber', 'serial_number', 'serialNumber'
            ]
            
            for field in number_fields:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown', 'null']:
                    fields['number'] = str(data_to_check[field])
                    break
            
            # Enhanced store name extraction
            store_name_fields = [
                'shopName', 'shop_name', 'storeName', 'store_name', 
                'merchant_name', 'merchantName', 'business_name', 'businessName',
                'company_name', 'companyName', 'retailer_name', 'retailerName',
                'outlet_name', 'outletName', 'branch_name', 'branchName'
            ]
            
            for field in store_name_fields:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown', 'null']:
                    fields['store_name'] = str(data_to_check[field])
                    break
            
            # Enhanced store ID extraction
            store_id_fields = [
                'shopCode', 'shop_code', 'storeId', 'store_id', 'storeCode', 'store_code',
                'merchant_id', 'merchantId', 'business_id', 'businessId', 
                'outlet_id', 'outletId', 'branch_id', 'branchId', 'location_id', 'locationId'
            ]
            
            for field in store_id_fields:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown', 'null']:
                    fields['store_id'] = str(data_to_check[field])
                    break
            
            # Enhanced amount extraction
            amount_fields = [
                'totalAmount', 'total_amount', 'total', 'amount', 'grandTotal', 'grand_total', 
                'ticketAmount', 'ticket_amount', 'final_amount', 'finalAmount',
                'net_amount', 'netAmount', 'payable_amount', 'payableAmount',
                'invoice_total', 'invoiceTotal', 'bill_total', 'billTotal',
                'subtotal', 'sub_total', 'sum', 'price', 'cost'
            ]
            
            for field in amount_fields:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown', 'null']:
                    # Handle both string and numeric amounts
                    amount_value = data_to_check[field]
                    if isinstance(amount_value, (int, float)):
                        fields['ticketAmount'] = str(amount_value)
                        break
                    elif isinstance(amount_value, str) and amount_value.strip():
                        fields['ticketAmount'] = amount_value.strip()
                        break
            
            # Enhanced time extraction
            time_fields = [
                'printTime', 'print_time', 'timestamp', 'dateTime', 'date_time', 
                'created_at', 'createdAt', 'transaction_time', 'transactionTime',
                'purchase_time', 'purchaseTime', 'sale_time', 'saleTime',
                'issued_at', 'issuedAt', 'receipt_time', 'receiptTime',
                'order_time', 'orderTime', 'billing_time', 'billingTime'
            ]
            
            for field in time_fields:
                if field in data_to_check and data_to_check[field] not in [None, '', 'unknown', 'null']:
                    fields['print_time'] = str(data_to_check[field])
                    break
        
        # Debug output for problematic cases
        if fields['number'] == 'unknown':
            print(f"🔍 DEBUG: Could not extract number from data: {list(data_to_check.keys())[:10]}")
        
        return fields
    
    def find_matching_response(self, number: str, response_map: Dict[str, Any], file_service) -> Optional[Dict[str, Any]]:
        """Enhanced matching with better debugging and fuzzy matching"""
        
        if self.debug_mode and len(response_map) % 100 == 0:
            print(f"🔍 DEBUG: Looking for '{number}' in {len(response_map)} response variants")
        
        # Strategy 1: Direct exact match
        if number in response_map:
            if self.debug_mode:
                print(f"✅ EXACT MATCH: Found {number}")
            return response_map[number]
        
        # Strategy 2: Try all filename variants
        variants = file_service.create_filename_variants(number)
        for variant in variants:
            if variant in response_map:
                if self.debug_mode:
                    print(f"✅ VARIANT MATCH: {number} → {variant}")
                return response_map[variant]
        
        # Strategy 3: Enhanced fuzzy matching for common patterns
        fuzzy_matches = self._try_fuzzy_matching(number, response_map)
        if fuzzy_matches:
            if self.debug_mode:
                print(f"✅ FUZZY MATCH: {number} → {fuzzy_matches[0]}")
            return response_map[fuzzy_matches[0]]
        
        # Strategy 4: Partial matching for long numbers
        partial_matches = self._try_partial_matching(number, response_map)
        if partial_matches:
            if self.debug_mode:
                print(f"✅ PARTIAL MATCH: {number} → {partial_matches[0]}")
            return response_map[partial_matches[0]]
        
        # Strategy 5: Advanced pattern matching for complex numbers
        pattern_matches = self._try_pattern_matching(number, response_map)
        if pattern_matches:
            if self.debug_mode:
                print(f"✅ PATTERN MATCH: {number} → {pattern_matches[0]}")
            return response_map[pattern_matches[0]]
        
        # Debug: Show what we tried to match
        if self.debug_mode:
            print(f"❌ NO MATCH: Tried {len(variants)} variants for '{number}'")
            if len(variants) > 1:
                print(f"   Variants: {variants[:3]}{'...' if len(variants) > 3 else ''}")
        
        return None
    
    def _try_pattern_matching(self, number: str, response_map: Dict[str, Any]) -> List[str]:
        """Try pattern-based matching for complex receipt numbers"""
        pattern_candidates = []
        
        # Extract numeric parts only
        numeric_only = ''.join(filter(str.isdigit, number))
        if len(numeric_only) < 6:  # Too short for pattern matching
            return []
        
        for key in response_map.keys():
            key_numeric = ''.join(filter(str.isdigit, key))
            
            # Check if numeric parts match exactly
            if numeric_only == key_numeric and len(numeric_only) >= 6:
                pattern_candidates.append(key)
            
            # Check for substring matches in longer numbers
            elif len(numeric_only) >= 8 and len(key_numeric) >= 8:
                if numeric_only in key_numeric or key_numeric in numeric_only:
                    # Ensure significant overlap
                    overlap_ratio = len(set(numeric_only) & set(key_numeric)) / max(len(numeric_only), len(key_numeric))
                    if overlap_ratio >= 0.7:
                        pattern_candidates.append(key)
        
        return pattern_candidates[:1]
    
    def _try_fuzzy_matching(self, number: str, response_map: Dict[str, Any]) -> List[str]:
        """Try fuzzy matching for common OCR/encoding issues"""
        fuzzy_candidates = []
        
        # Common OCR substitutions - expanded
        ocr_substitutions = {
            '0': ['O', 'o', 'Q'],
            'O': ['0', 'o', 'Q'],
            'o': ['0', 'O', 'Q'],
            'Q': ['0', 'O', 'o'],
            '1': ['l', 'I', '|', 'i'],
            'l': ['1', 'I', '|', 'i'],
            'I': ['1', 'l', '|', 'i'],
            'i': ['1', 'l', 'I', '|'],
            '5': ['S', 's'],
            'S': ['5', 's'],
            's': ['5', 'S'],
            '8': ['B', 'b'],
            'B': ['8', 'b'],
            'b': ['8', 'B'],
            '6': ['G', 'g'],
            'G': ['6', 'g'],
            'g': ['6', 'G'],
            '2': ['Z', 'z'],
            'Z': ['2', 'z'],
            'z': ['2', 'Z']
        }
        
        # Generate fuzzy variants
        for key in response_map.keys():
            if self._is_fuzzy_match(number, key, ocr_substitutions):
                fuzzy_candidates.append(key)
        
        return fuzzy_candidates[:1]
    
    def _try_partial_matching(self, number: str, response_map: Dict[str, Any]) -> List[str]:
        """Try partial matching for long receipt numbers"""
        partial_candidates = []
        
        # Only try partial matching for longer numbers
        if len(number) < 8:
            return []
        
        for key in response_map.keys():
            # Check if number is contained in key or vice versa
            if len(key) >= 8:
                if number in key or key in number:
                    # Ensure significant overlap (at least 80%)
                    overlap = len(set(number) & set(key))
                    min_length = min(len(number), len(key))
                    if overlap / min_length >= 0.8:
                        partial_candidates.append(key)
        
        return partial_candidates[:1]
    
    def _is_fuzzy_match(self, number1: str, number2: str, substitutions: Dict[str, List[str]]) -> bool:
        """Check if two numbers are fuzzy matches with enhanced tolerance"""
        if abs(len(number1) - len(number2)) > 3:  # Increased tolerance
            return False
        
        if len(number1) < 6:  # Too short for fuzzy matching
            return False
        
        differences = 0
        max_differences = max(2, len(number1) // 8)  # Increased tolerance: allow 12.5% differences
        
        min_len = min(len(number1), len(number2))
        for i in range(min_len):
            char1 = number1[i]
            char2 = number2[i] if i < len(number2) else ''
            
            if char1 != char2:
                # Check if it's a known substitution
                if char2 in substitutions.get(char1, []):
                    continue  # Valid substitution, don't count as difference
                else:
                    differences += 1
                    if differences > max_differences:
                        return False
        
        return True