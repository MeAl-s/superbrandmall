# app/services/delivery_scanner/detection_service.py - Delivery detection service
import re
from typing import Tuple, List

class DetectionService:
    """Handles delivery keyword detection - extracted from your detection functions"""
    
    def __init__(self):
        # Only 4 keywords to check - exactly like your original
        self.keywords = ['美团', '京东', '饿了么', '配送']
    
    def detect_spaced_keyword(self, text: str, keyword: str) -> Tuple[bool, str]:
        """Detect keyword with various separators - EXACT logic from your detect_spaced_keyword function"""
        # Convert keyword to individual characters
        chars = list(keyword)
        
        # Enhanced separator patterns for OCR variations - exactly like your original
        # Common OCR misreads: spaces, underscores, dashes, dots, commas, pipes, etc.
        separators = r'[\s_\-\.\,\|\:\;\!\?\*\+\=\(\)\[\]\{\}\<\>\~\`\^\&\%\$\#\@]{0,5}'  # Max 5 separator characters
        
        # Build pattern: char + separator + char + separator + ...
        pattern = separators.join(chars)
        
        # Multiple search strategies to catch different cases - exactly like your original
        search_patterns = [
            # Strategy 1: With word boundaries (strict)
            f'(?:^|[\\s\\n\\r]){pattern}(?=[\\s\\n\\r]|$)',
            
            # Strategy 2: Looser boundaries (for embedded text)
            f'(?<![\\u4e00-\\u9fff\\w]){pattern}(?![\\u4e00-\\u9fff\\w])',
            
            # Strategy 3: Very loose (just the pattern)
            pattern
        ]
        
        for search_pattern in search_patterns:
            try:
                # Search for pattern (case insensitive)
                matches = re.findall(search_pattern, text, re.IGNORECASE | re.UNICODE)
                
                if matches:
                    # Validate each match to avoid false positives
                    for match in matches:
                        if self._validate_match(match, keyword):
                            return True, match
                            
            except re.error:
                # If regex fails, try next pattern
                continue
        
        return False, ""
    
    def _validate_match(self, match: str, keyword: str) -> bool:
        """Validate that the match is reasonable - EXACT logic from your validate_match function"""
        if not match:
            return False
        
        # Clean the match for analysis
        match_clean = match.strip()
        
        # Basic length check - shouldn't be too long
        if len(match_clean) > len(keyword) * 6:  # Max 6x original length
            return False
        
        # Count different types of characters
        keyword_chars = set(keyword)
        separator_chars = set(' _-.,:;!?*+=()[]{}.<>~`^&%$#@|\\/')
        
        match_keyword_chars = [c for c in match_clean if c in keyword_chars]
        match_separator_chars = [c for c in match_clean if c in separator_chars]
        match_other_chars = [c for c in match_clean if c not in keyword_chars and c not in separator_chars]
        
        # Should contain all keyword characters
        if len(set(match_keyword_chars)) < len(keyword_chars):
            return False
        
        # Shouldn't have too many random characters
        if len(match_other_chars) > len(keyword) // 2:  # Max half as many random chars as keyword length
            return False
        
        # Separator to keyword ratio check
        if len(match_separator_chars) > len(keyword) * 3:  # Max 3x separators
            return False
        
        # Character order check - keyword characters should appear in correct order
        keyword_positions = []
        for kw_char in keyword:
            pos = match_clean.find(kw_char)
            if pos >= 0:
                keyword_positions.append(pos)
                # Remove found character to handle duplicates
                match_clean = match_clean[:pos] + ' ' + match_clean[pos+1:]
            else:
                return False
        
        # Positions should be in ascending order (characters appear in sequence)
        if keyword_positions != sorted(keyword_positions):
            return False
        
        return True
    
    def check_delivery_keywords(self, text: str) -> Tuple[bool, List[str]]:
        """Check for delivery keywords - EXACT logic from your check_delivery_keywords function"""
        if not text:
            return False, []
        
        found_keywords = []
        
        for keyword in self.keywords:
            # Method 1: Direct match (fastest) - exactly like your original
            if keyword in text:
                found_keywords.append(f"{keyword} (direct)")
                continue
            
            # Method 2: Flexible spacing detection - exactly like your original
            found, match = self.detect_spaced_keyword(text, keyword)
            if found:
                found_keywords.append(f"{keyword} (spaced: '{match.strip()}')")
        
        return len(found_keywords) > 0, found_keywords
    
    def get_keywords(self) -> List[str]:
        """Get list of delivery keywords"""
        return self.keywords.copy()