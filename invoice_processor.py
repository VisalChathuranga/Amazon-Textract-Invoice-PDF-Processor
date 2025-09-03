import boto3
import hashlib
import json
import re
import os
import time
from datetime import datetime

# ===== CONFIGURATION =====
AWS_REGION = "us-east-1"
BUCKET_NAME = "visal-invoice-processing-bucket"
INVOICE_DIR = "invoices"
RESULTS_DIR = "results"

# Ensure results folder exists
os.makedirs(RESULTS_DIR, exist_ok=True)

s3_client = boto3.client("s3", region_name=AWS_REGION)
textract_client = boto3.client("textract", region_name=AWS_REGION)

# ===== SAFE PRINT FUNCTION =====
def safe_print(message):
    """Print messages with safe encoding handling."""
    try:
        print(message)
    except UnicodeEncodeError:
        # Replace problematic unicode characters with safe alternatives
        safe_message = message.encode('ascii', errors='replace').decode('ascii')
        print(safe_message)

# ===== UTILITY FUNCTIONS =====
def file_md5(file_path):
    """Return MD5 hash of a file for duplicate checking."""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def s3_file_exists(key, local_hash):
    """Check if file already exists in S3 with same hash (stored in metadata)."""
    try:
        obj = s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return obj["Metadata"].get("md5", "") == local_hash
    except s3_client.exceptions.ClientError:
        return False

def upload_invoices():
    """Upload new invoices to S3 if not already present or changed."""
    uploaded_files = []
    for file in os.listdir(INVOICE_DIR):
        if not file.lower().endswith(".pdf"):
            continue

        path = os.path.join(INVOICE_DIR, file)
        file_hash = file_md5(path)

        if not s3_file_exists(file, file_hash):
            safe_print(f"[UPLOAD] Uploading {file}...")
            with open(path, "rb") as f:
                s3_client.upload_fileobj(
                    f,
                    BUCKET_NAME,
                    file,
                    ExtraArgs={"Metadata": {"md5": file_hash}},
                )
            uploaded_files.append(file)
        else:
            safe_print(f"[SKIP] Skipping {file} (no changes)")
    return uploaded_files

# ===== TEXTRACT FUNCTIONS =====
def start_textract_job(s3_key):
    """Start asynchronous Textract job."""
    response = textract_client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": BUCKET_NAME, "Name": s3_key}},
        FeatureTypes=["TABLES", "FORMS"],
    )
    return response["JobId"]

def is_job_complete(job_id):
    """Poll Textract until job completes."""
    safe_print("[WAIT] Waiting for Textract to complete...")
    while True:
        response = textract_client.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        if status == "SUCCEEDED":
            return response
        elif status == "FAILED":
            safe_print(f"[ERROR] Textract job failed: {response.get('StatusMessage', 'Unknown error')}")
            return None
        safe_print("   Job still processing...")
        time.sleep(5)

# ===== TEXT EXTRACTION HELPERS =====
def extract_text_from_block(block, block_map):
    """Get text from WORD/LINE children of a block."""
    text = ""
    if "Relationships" in block:
        for rel in block["Relationships"]:
            if rel["Type"] == "CHILD":
                for cid in rel["Ids"]:
                    if cid in block_map:
                        child = block_map[cid]
                        if child["BlockType"] in ["WORD", "LINE"]:
                            text += child["Text"] + " "
    return text.strip()

def clean_currency_text(text):
    """Extract currency symbol and clean numeric value."""
    if not text:
        return None, ""
    
    # Find currency symbols
    currency_symbols = ["€", "$", "£", "¥", "₹"]
    currency = ""
    for symbol in currency_symbols:
        if symbol in text:
            currency = symbol
            break
    
    # Clean numeric value - handle European and US formats
    cleaned = text.replace(",", "").replace("€", "").replace("$", "").replace("£", "").strip()
    
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
    except:
        pass
    
    return text, currency

def parse_amount_with_currency(text):
    """Parse amount and return with currency info."""
    if not text or text == "":
        return {"value": "", "currency": "", "formatted": ""}
    
    value, currency = clean_currency_text(str(text))
    if isinstance(value, (int, float)):
        formatted = f"{value:,.2f}"
        if currency:
            formatted = f"{formatted} {currency}"
        return {"value": value, "currency": currency, "formatted": formatted}
    else:
        return {"value": text, "currency": "", "formatted": str(text)}

# ===== FIELD EXTRACTION =====
def find_invoice_number(kv_pairs, all_text):
    """Enhanced invoice number detection."""
    # Check key-value pairs first
    for k, v in kv_pairs.items():
        if any(term in k for term in ["invoice number", "invoice no", "invoice #", "inv number", "inv no"]):
            if v and v.strip():
                return v.strip()
    
    # Search in all text with patterns
    patterns = [
        r'invoice\s*(?:number|no|#)[\s:]*([A-Z0-9\-]+)',
        r'inv[\s\-]*(?:number|no|#)[\s:]*([A-Z0-9\-]+)',
        r'invoice[\s:]+([A-Z0-9\-]{3,})',
        r'#([A-Z0-9\-]{3,})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, all_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return None

def find_invoice_total(kv_pairs, table_totals, all_text):
    """Enhanced total detection with better priority and currency handling."""
    
    # Priority keywords - ordered from most specific to least specific
    # Higher index = higher priority (final totals)
    total_keywords = [
        ("total", 1),
        ("subtotal", 2),
        ("amount due", 8),
        ("invoice total", 7),
        ("grand total", 9),
        ("final total", 10),
        ("total due", 8),
        ("gross total", 11),
        ("gross amount", 12),
        ("total amount", 6),
        ("net amount", 3),
        ("total incl", 13),  # Total including VAT/tax
        ("incl. vat", 14),   # Including VAT
        ("including vat", 14),
        ("total fees and disbursements", 15),  # Very specific - should have highest priority
        ("balance due", 8),
        ("amount payable", 7)
    ]
    
    best_total = None
    best_priority = 0
    best_source = ""
    
    # Priority 1: Search in key-value pairs with enhanced matching
    for k, v in kv_pairs.items():
        k_lower = k.lower().strip()
        
        for keyword, priority in total_keywords:
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
        
        for keyword, priority in total_keywords:
            if keyword in total_text_lower:
                amount_info = parse_amount_with_currency(value)
                if isinstance(amount_info["value"], (int, float)) and amount_info["value"] > 0:
                    # Boost for comprehensive totals
                    actual_priority = priority
                    
                    # Special handling for specific patterns
                    if "fees and disbursements" in total_text_lower:
                        actual_priority += 10  # Very high priority
                    elif "gross" in total_text_lower and ("incl" in total_text_lower or "vat" in total_text_lower):
                        actual_priority += 8   # High priority for gross including VAT
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
    
    # Debug output (optional - can be removed in production)
    if best_total and isinstance(best_total["value"], (int, float)):
        print(f"[DEBUG] Selected total: {best_total['formatted']} from {best_source} (priority: {best_priority})")
    
    return best_total if best_total else {"value": None, "currency": "", "formatted": ""}

def extract_line_items_from_tables(tables, block_map):
    """IMPROVED line item extraction with better table processing and debugging."""
    all_line_items = []
    
    # Enhanced header matching with more flexible patterns
    header_patterns = {
        "description": [
            "description", "service", "item", "details", "work performed", 
            "service description", "product", "article", "task", "work", "desc"
        ],
        "quantity": [
            "qty", "quantity", "hours", "units", "hrs/qty", "hrs", "units",
            "amount", "number", "count", "hrs", "hours", "hrs qty", "hrs/qty"
        ],
        "unitprice": [
            "unit price", "price", "rate", "rate/price", "unit cost", 
            "price per unit", "cost", "rate", "unit rate", "each", "rate price"
        ],
        "amount": [
            "amount", "total", "sub total", "line total", "extended amount",
            "sum", "value", "cost", "charge", "price", "subtotal", "sub_total"
        ]
    }

    # Words that indicate summary/total rows (should be excluded from line items)
    summary_indicators = [
        "subtotal", "sub total", "sub-total", "sub_total",
        "total", "grand total", "final total",
        "net total", "gross total",
        "vat", "tax", "vat 19%", "sales tax",
        "discount", "adjustment", "fee discount",
        "balance", "amount due", "total due",
        "fees and disbursements",
        "gross amount", "net amount",
        "incl. vat", "including vat", "excl. vat",
        "adjusted fees", "total adjusted"
    ]
    
    def match_header_enhanced(header_text):
        """More flexible header matching."""
        if not header_text or not isinstance(header_text, str):
            return None
            
        header_lower = header_text.lower().strip()
        
        # Remove common punctuation and symbols for better matching
        header_clean = re.sub(r'[^\w\s/]', ' ', header_lower)  # Keep forward slash for "hrs/qty"
        header_clean = ' '.join(header_clean.split())  # Remove extra spaces
        
        # Check for exact matches first
        for field, patterns in header_patterns.items():
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
    
    def is_summary_row(row_data):
        """Check if a row contains summary/total information."""
        row_text_combined = " ".join([str(cell) for cell in row_data]).lower()
        
        for indicator in summary_indicators:
            if indicator in row_text_combined:
                return True
        return False
    
    print(f"[DEBUG] Found {len(tables)} table pages to process")
    
    for page, rows_dict in tables.items():
        if not rows_dict:
            print(f"[DEBUG] Page {page}: No rows found")
            continue
            
        # Convert to sorted rows
        rows = []
        max_cols = max((max(r.keys()) for r in rows_dict.values()), default=0)
        for r in sorted(rows_dict.keys()):
            row = [rows_dict[r].get(c, "") for c in range(1, max_cols + 1)]
            rows.append(row)
        
        print(f"[DEBUG] Page {page}: Found {len(rows)} rows with {max_cols} columns")
        
        # Print first few rows for debugging
        for i, row in enumerate(rows[:3]):
            print(f"[DEBUG] Row {i}: {row}")
        
        if len(rows) < 2:  # Need at least header + 1 data row
            print(f"[DEBUG] Page {page}: Not enough rows ({len(rows)})")
            continue
        
        # Try to identify headers with more flexibility
        potential_headers = []
        for i in range(min(4, len(rows))):  # Check first 4 rows for headers
            header_matches = [match_header_enhanced(cell) for cell in rows[i]]
            match_count = sum(1 for h in header_matches if h is not None)
            
            print(f"[DEBUG] Row {i} header matches: {header_matches} (count: {match_count})")
            
            if match_count >= 2:  # At least 2 recognized headers
                potential_headers.append((i, header_matches, match_count))
        
        if not potential_headers:
            print(f"[DEBUG] Page {page}: No headers found")
            # Try a more relaxed approach - look for any row with common header-like words
            for i, row in enumerate(rows[:3]):
                row_text = " ".join(row).lower()
                if any(word in row_text for word in ["service", "description", "rate", "price", "amount", "total", "qty", "hours"]):
                    print(f"[DEBUG] Row {i} might be a header based on keywords: {row}")
                    # Try to manually map this row
                    manual_headers = []
                    for cell in row:
                        manual_headers.append(match_header_enhanced(cell))
                    if sum(1 for h in manual_headers if h is not None) >= 1:
                        potential_headers.append((i, manual_headers, sum(1 for h in manual_headers if h is not None)))
            
            if not potential_headers:
                print(f"[DEBUG] Page {page}: Still no headers found, skipping table")
                continue

        # Use the best header row (most matches, or first one with good matches)
        header_row_idx, headers, match_count = max(potential_headers, key=lambda x: (x[2], -x[0]))
        print(f"[DEBUG] Using header row {header_row_idx} with headers: {headers}")
        
        data_rows = rows[header_row_idx + 1:]
        print(f"[DEBUG] Processing {len(data_rows)} data rows")
        
        for row_idx, row in enumerate(data_rows):
            if not any(cell.strip() for cell in row):  # Skip empty rows
                print(f"[DEBUG] Skipping empty row {row_idx}")
                continue
            
            print(f"[DEBUG] Processing data row {row_idx}: {row}")
            
            # Check if this is a summary row
            if is_summary_row(row):
                print(f"[DEBUG] Skipping summary row {row_idx}: {row}")
                continue
                
            line_item = {}
            has_meaningful_data = False
            
            for i, cell_value in enumerate(row):
                if i >= len(headers) or not headers[i]:
                    continue
                    
                field = headers[i]
                cell_value = str(cell_value).strip()
                
                if not cell_value:
                    continue
                
                print(f"[DEBUG] Processing field '{field}' = '{cell_value}'")
                
                if field == "description":
                    # Additional check: if description contains summary terms, skip this row
                    desc_lower = cell_value.lower()
                    if any(indicator in desc_lower for indicator in summary_indicators):
                        print(f"[DEBUG] Description contains summary term, skipping row")
                        has_meaningful_data = False
                        break  # Break out of the cell loop for this row
                    
                    line_item["Description"] = cell_value
                    has_meaningful_data = True
                    print(f"[DEBUG] Added description: {cell_value}")
                    
                elif field == "quantity":
                    qty_info = parse_amount_with_currency(cell_value)
                    if isinstance(qty_info["value"], (int, float)):
                        line_item["Quantity"] = qty_info["value"]
                        has_meaningful_data = True
                        print(f"[DEBUG] Added quantity: {qty_info['value']}")
                    else:
                        # Sometimes quantity is just a number without formatting
                        try:
                            qty_val = float(cell_value)
                            line_item["Quantity"] = qty_val
                            has_meaningful_data = True
                            print(f"[DEBUG] Added quantity (parsed): {qty_val}")
                        except:
                            print(f"[DEBUG] Could not parse quantity: {cell_value}")
                            
                elif field == "unitprice":
                    price_info = parse_amount_with_currency(cell_value)
                    line_item["UnitPrice"] = price_info
                    if isinstance(price_info["value"], (int, float)):
                        has_meaningful_data = True
                        print(f"[DEBUG] Added unit price: {price_info}")
                    
                elif field == "amount":
                    amount_info = parse_amount_with_currency(cell_value)
                    line_item["Amount"] = amount_info
                    if isinstance(amount_info["value"], (int, float)):
                        has_meaningful_data = True
                        print(f"[DEBUG] Added amount: {amount_info}")
            
            # Only add line items that have meaningful business data
            if has_meaningful_data:
                print(f"[DEBUG] Adding line item: {line_item}")
                all_line_items.append(line_item)
            else:
                print(f"[DEBUG] Skipping row - no meaningful data found")
    
    print(f"[DEBUG] Total line items extracted: {len(all_line_items)}")
    return all_line_items

# ===== MAIN EXTRACTION FUNCTION =====
def extract_invoice_data(textract_output):
    """Extract all required invoice data with improved accuracy."""
    safe_print("[EXTRACT] Extracting invoice data...")
    
    # Initialize result structure
    invoice_data = {
        "InvoiceNumber": None,
        "InvoiceDate": None,
        "LineItems": [],
        "InvoiceTotal": {"value": None, "currency": "", "formatted": ""},
        "PaymentTerms": None
    }
    
    if not textract_output or "Blocks" not in textract_output:
        return invoice_data
    
    # Build block map for easy lookup
    block_map = {block["Id"]: block for block in textract_output["Blocks"]}
    
    # Collect all text for pattern matching
    all_text = ""
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "LINE":
            all_text += block["Text"] + " "
    
    # ===== EXTRACT KEY-VALUE PAIRS =====
    key_blocks = {}
    value_blocks = {}
    
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "KEY_VALUE_SET":
            if "KEY" in block.get("EntityTypes", []):
                key_blocks[block["Id"]] = block
            else:
                value_blocks[block["Id"]] = block
    
    # Build key-value relationships
    kv_pairs = {}
    for key_id, key_block in key_blocks.items():
        key_text = extract_text_from_block(key_block, block_map).lower().strip()
        value_text = ""
        
        if "Relationships" in key_block:
            for rel in key_block["Relationships"]:
                if rel["Type"] == "VALUE":
                    for value_id in rel["Ids"]:
                        if value_id in value_blocks:
                            value_text = extract_text_from_block(value_blocks[value_id], block_map).strip()
        
        if key_text and value_text:
            kv_pairs[key_text] = value_text
    
    # ===== EXTRACT TABLES =====
    tables = {}
    for block in textract_output["Blocks"]:
        if block["BlockType"] == "CELL":
            page = block.get("Page", 1)
            row = block["RowIndex"]
            col = block["ColumnIndex"]
            text = extract_text_from_block(block, block_map)
            
            if page not in tables:
                tables[page] = {}
            if row not in tables[page]:
                tables[page][row] = {}
            tables[page][row][col] = text
    
    # ===== EXTRACT SPECIFIC FIELDS =====
    
    # 1. Invoice Number
    invoice_data["InvoiceNumber"] = find_invoice_number(kv_pairs, all_text)
    
    # 2. Invoice Date
    date_keywords = ["invoice date", "date", "bill date", "issued"]
    for k, v in kv_pairs.items():
        if any(keyword in k for keyword in date_keywords) and v.strip():
            invoice_data["InvoiceDate"] = v.strip()
            break
    
    # 3. Payment Terms
    for k, v in kv_pairs.items():
        if any(term in k for term in ["payment", "terms", "due"]) and len(v) > 20:
            invoice_data["PaymentTerms"] = v.strip()
            break
    
    # 4. Line Items (with improved extraction)
    invoice_data["LineItems"] = extract_line_items_from_tables(tables, block_map)
    
    # 5. Invoice Total
    table_totals = {}
    for page, rows in tables.items():
        for row_idx, row_data in rows.items():
            row_text = " ".join(row_data.values()).lower()
            if "total" in row_text:
                table_totals[row_text] = " ".join(row_data.values())
    
    invoice_data["InvoiceTotal"] = find_invoice_total(kv_pairs, table_totals, all_text)
    
    return invoice_data

# ===== OUTPUT AND MAIN FUNCTIONS =====
def save_results(invoice_file, results):
    """Save results with pretty formatting."""
    base_name = os.path.splitext(invoice_file)[0]
    out_path = os.path.join(RESULTS_DIR, f"{base_name}_results.json")
    
    # Create a formatted version for better readability
    formatted_results = {
        "InvoiceNumber": results["InvoiceNumber"],
        "InvoiceDate": results["InvoiceDate"],
        "InvoiceTotal": results["InvoiceTotal"]["formatted"] if results["InvoiceTotal"]["formatted"] else results["InvoiceTotal"]["value"],
        "LineItems": results["LineItems"],
        "PaymentTerms": results["PaymentTerms"]
    }
    
    with open(out_path, "w", encoding='utf-8') as f:
        json.dump(formatted_results, f, indent=4, ensure_ascii=False)
    
    safe_print(f"[SAVE] Results saved: {out_path}")

def print_summary(invoice_file, results):
    """Print a nice summary of extracted data."""
    safe_print(f"\n[SUMMARY] INVOICE SUMMARY: {invoice_file}")
    safe_print("=" * 50)
    safe_print(f"Invoice Number: {results['InvoiceNumber'] or 'Not found'}")
    safe_print(f"Invoice Date: {results['InvoiceDate'] or 'Not found'}")
    safe_print(f"Total Amount: {results['InvoiceTotal']['formatted'] or 'Not found'}")
    safe_print(f"Line Items: {len(results['LineItems'])} items found")
    
    if results['LineItems']:
        safe_print("\nLine Items:")
        for i, item in enumerate(results['LineItems'][:5], 1):  # Show first 5
            desc = item.get('Description', 'N/A')[:50]
            qty = item.get('Quantity', 'N/A')
            amount = item.get('Amount', {})
            if isinstance(amount, dict):
                amount_str = amount.get('formatted', 'N/A')
            else:
                amount_str = str(amount)
            safe_print(f"  {i}. {desc}... | Qty: {qty} | Amount: {amount_str}")
        
        if len(results['LineItems']) > 5:
            safe_print(f"  ... and {len(results['LineItems']) - 5} more items")
    
    safe_print("\n" + "=" * 50)

def process_invoices():
    """Main processing pipeline."""
    safe_print("[START] Starting Invoice Processing Pipeline")
    safe_print("=" * 60)
    
    # Upload invoices to S3
    uploaded_files = upload_invoices()
    
    # Process each PDF
    for file in os.listdir(INVOICE_DIR):
        if not file.lower().endswith(".pdf"):
            continue
        
        safe_print(f"\n[PROCESS] Processing: {file}")
        safe_print("-" * 40)
        
        try:
            # Start Textract job
            job_id = start_textract_job(file)
            
            # Wait for completion
            textract_output = is_job_complete(job_id)
            
            if textract_output is None:
                safe_print(f"[ERROR] Failed to process {file}")
                continue
            
            # Extract invoice data
            results = extract_invoice_data(textract_output)
            
            # Save and display results
            save_results(file, results)
            print_summary(file, results)
            
        except Exception as e:
            safe_print(f"[ERROR] Error processing {file}: {str(e)}")
            continue
    
    safe_print(f"\n[COMPLETE] Processing complete! Check the '{RESULTS_DIR}' folder for detailed results.")

if __name__ == "__main__":
    process_invoices()