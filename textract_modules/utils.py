"""
Utility functions for text processing, parsing, and data extraction.
Contains helper functions used across multiple modules.
"""

import re
import logging
from typing import Optional, Tuple, Dict, Any, List 
from .config import CURRENCY_SYMBOLS, INVOICE_NUMBER_PATTERNS, TOTAL_KEYWORDS, DATE_KEYWORDS, PAYMENT_TERMS_KEYWORDS, SUMMARY_INDICATORS, HEADER_PATTERNS

logger = logging.getLogger(__name__)

def clean_currency_text(text: str) -> Tuple[Optional[float], str]:
    """
    Extract currency symbol and clean numeric value from text.
    Handles different currency formats including European (1.234,56) and US (1,234.56).
    
    Args:
        text: Input text containing currency information
        
    Returns:
        Tuple of (numeric_value, currency_symbol)
    """
    if not text:
        return None, ""
        
    # Find currency symbols
    currency = ""
    for symbol in CURRENCY_SYMBOLS:
        if symbol in text:
            currency = symbol
            break
            
    # Clean numeric value - handle European and US formats
    cleaned = text
    for symbol in CURRENCY_SYMBOLS:
        cleaned = cleaned.replace(symbol, "")
    cleaned = cleaned.replace(",", "").strip()
    
    # Handle European decimal format (1.234,56 -> 1234.56)
    if "." in cleaned and cleaned.count(".") > 1:
        # Multiple dots - European thousand separator
        parts = cleaned.split(".")
        if len(parts[-1]) == 2:  # Last part is likely cents
            cleaned = "".join(parts[:-1]) + "." + parts[-1]
        else:
            cleaned = cleaned.replace(".", "")
            
    try:
        # Extract numeric value using regex
        match = re.search(r'[-+]?\d*\.?\d+', cleaned)
        if match:
            value = float(match.group())
            return value, currency
    except Exception as e:
        logger.debug(f"Could not parse currency value from '{text}': {str(e)}")
        
    return None, currency

def parse_amount_with_currency(text: str) -> Dict[str, Any]:
    """
    Parse amount and return with currency info.
    
    Args:
        text: Input text containing amount
        
    Returns:
        Dictionary with value, currency, and formatted string
    """
    if not text or text == "":
        return {"value": "", "currency": "", "formatted": ""}
        
    value, currency = clean_currency_text(str(text))
    
    if isinstance(value, (int, float)):
        formatted = f"{value:,.2f}"
        if currency:
            formatted = f"{currency} {formatted}"
        return {"value": value, "currency": currency, "formatted": formatted}
    else:
        return {"value": text, "currency": "", "formatted": str(text)}

def find_invoice_number(kv_pairs: Dict, all_text: str) -> Optional[str]:
    """
    Enhanced invoice number detection from key-value pairs or text patterns.
    First checks form fields, then uses regex patterns on full text.
    
    Args:
        kv_pairs: Dictionary of key-value pairs from forms
        all_text: Combined text from all document elements
        
    Returns:
        Extracted invoice number or None if not found
    """
    # Check key-value pairs first (form fields)
    for k, v in kv_pairs.items():
        if any(term in k for term in ["invoice number", "invoice no", "invoice #", "inv number", "inv no"]):
            if v and v.strip():
                logger.debug(f"Found invoice number in key-value pair: {v.strip()}")
                return v.strip()
                
    # Search in all text with patterns (fallback)
    for pattern in INVOICE_NUMBER_PATTERNS:
        match = re.search(pattern, all_text, re.IGNORECASE)
        if match:
            invoice_number = match.group(1).strip()
            logger.debug(f"Found invoice number with regex pattern: {invoice_number}")
            return invoice_number
            
    logger.warning("Invoice number not found")
    return None

def find_invoice_total(kv_pairs: Dict, table_totals: Dict, all_text: str) -> Dict[str, Any]:
    """
    Enhanced total detection with better priority and currency handling.
    Uses a priority system to select the most reliable total amount.
    
    Args:
        kv_pairs: Dictionary of key-value pairs from forms
        table_totals: Dictionary of totals found in tables
        all_text: Combined text from all document elements
        
    Returns:
        Dictionary with total amount information
    """
    best_total = None
    best_priority = 0
    best_source = ""
    
    # Priority 1: Search in key-value pairs with enhanced matching
    for k, v in kv_pairs.items():
        k_lower = k.lower().strip()
        for keyword, priority in TOTAL_KEYWORDS:
            if keyword in k_lower:
                amount_info = parse_amount_with_currency(v)
                if isinstance(amount_info["value"], (int, float)) and amount_info["value"] > 0:
                    # Boost priority for more specific matches
                    actual_priority = priority
                    
                    # Extra boost for exact matches
                    if k_lower == keyword or k_lower.replace(":", "").strip() == keyword:
                        actual_priority += 5
                        
                    # Boost for VAT/tax inclusive amounts
                    if any(term in k_lower for term in ["incl", "including", "with vat", "with tax"]):
                        actual_priority += 3
                        
                    if actual_priority > best_priority:
                        best_total = amount_info
                        best_priority = actual_priority
                        best_source = f"KV: {k}"
    
    # Priority 2: Enhanced table totals search
    table_candidates = []
    for total_text, value in table_totals.items():
        total_text_lower = total_text.lower()
        for keyword, priority in TOTAL_KEYWORDS:
            if keyword in total_text_lower:
                amount_info = parse_amount_with_currency(value)
                if isinstance(amount_info["value"], (int, float)) and amount_info["value"] > 0:
                    # Boost for comprehensive totals
                    actual_priority = priority
                    
                    # Special handling for specific patterns
                    if "fees and disbursements" in total_text_lower:
                        actual_priority += 10  # Very high priority
                    elif "gross" in total_text_lower and ("incl" in total_text_lower or "vat" in total_text_lower):
                        actual_priority += 8  # High priority for gross including VAT
                    elif "grand total" in total_text_lower:
                        actual_priority += 7
                    elif "final" in total_text_lower:
                        actual_priority += 6
                        
                    table_candidates.append((amount_info, actual_priority, f"Table: {total_text[:50]}"))
    
    # Find best table candidate
    if table_candidates:
        table_candidates.sort(key=lambda x: x[1], reverse=True)  # Sort by priority
        best_table = table_candidates[0]
        if best_table[1] > best_priority:
            best_total = best_table[0]
            best_priority = best_table[1]
            best_source = best_table[2]
    
    # Priority 3: Regex patterns in all text (fallback)
    if best_priority < 5:  # Only use regex if we haven't found a good match
        patterns = [
            (r'total\s+fees\s+and\s+disbursements[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 20),
            (r'gross\s+amount\s+incl[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 18),
            (r'grand\s+total[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 15),
            (r'final\s+total[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 15),
            (r'total\s+due[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 12),
            (r'amount\s+due[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 12),
            (r'invoice\s+total[^0-9]*([€$£¥₹]?\s*[0-9,]+\.?\d*)', 10)
        ]
        
        for pattern, priority in patterns:
            matches = re.finditer(pattern, all_text, re.IGNORECASE)
            for match in matches:
                amount_info = parse_amount_with_currency(match.group(1))
                if isinstance(amount_info["value"], (int, float)) and amount_info["value"] > 0:
                    if priority > best_priority:
                        best_total = amount_info
                        best_priority = priority
                        best_source = f"Regex: {match.group(0)[:50]}"
    
    # Debug output
    if best_total and isinstance(best_total["value"], (int, float)):
        logger.debug(f"Selected total: {best_total['formatted']} from {best_source} (priority: {best_priority})")
    
    return best_total if best_total else {"value": None, "currency": "", "formatted": ""}

def match_header_enhanced(header_text: str) -> Optional[str]:
    """
    More flexible header matching for table columns.
    Tries exact matches first, then partial matches with common header patterns.
    
    Args:
        header_text: Text from a table header cell
        
    Returns:
        Matched field name or None if no match
    """
    if not header_text or not isinstance(header_text, str):
        return None
        
    header_lower = header_text.lower().strip()
    # Remove common punctuation and symbols for better matching
    header_clean = re.sub(r'[^\w\s/]', ' ', header_lower)  # Keep forward slash for "hrs/qty"
    header_clean = ' '.join(header_clean.split())  # Remove extra spaces
    
    # Check for exact matches first
    for field, patterns in HEADER_PATTERNS.items():
        for pattern in patterns:
            # Exact match
            if header_clean == pattern:
                return field
                
            # Check if pattern appears as a whole word
            if re.search(r'\b' + re.escape(pattern) + r'\b', header_clean):
                return field
                
            # Also check if the pattern is a substring (for compound headers like "Hrs/Qty")
            if pattern in header_clean:
                return field
                
    return None

def is_summary_row(row_data: List) -> bool:
    """
    Check if a row contains summary/total information.
    Prevents treating summary rows as line items.
    
    Args:
        row_data: List of cell values from a table row
        
    Returns:
        True if row contains summary indicators, False otherwise
    """
    row_text_combined = " ".join([str(cell) for cell in row_data]).lower()
    for indicator in SUMMARY_INDICATORS:
        if indicator in row_text_combined:
            return True
    return False