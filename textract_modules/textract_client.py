"""
Main Textract processing client with comprehensive document analysis capabilities.
Orchestrates the entire PDF-to-structured-data pipeline.
"""

import boto3
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from botocore.exceptions import ClientError
from pathlib import Path
import concurrent.futures
import json
import os
import re

from .s3_sync import S3FolderSync
from .formatters import MarkdownFormatter
from .utils import (
    parse_amount_with_currency, 
    find_invoice_number, 
    find_invoice_total, 
    match_header_enhanced, 
    is_summary_row
)
from .config import (
    DATE_KEYWORDS, 
    PAYMENT_TERMS_KEYWORDS, 
    SUMMARY_INDICATORS
)

logger = logging.getLogger(__name__)

class TextractPDFProcessor:
    """
    Comprehensive AWS Textract processor for PDF documents with all features:
    - Layout extraction (titles, headers, paragraphs, etc.)
    - Forms detection
    - Tables extraction
    - Custom queries
    - Signature detection
    """
    
    def __init__(self, region_name='us-east-1'):
        """Initialize AWS clients with region configuration."""
        try:
            self.textract = boto3.client('textract', region_name=region_name)
            self.s3 = boto3.client('s3', region_name=region_name)
            logger.info(f"Successfully initialized AWS clients in region: {region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {str(e)}")
            raise
    
    def start_document_analysis(self, bucket_name: str, document_name: str, 
                               feature_types: List[str], 
                               queries: Optional[List[Dict]] = None) -> Optional[str]:
        """
        Start asynchronous document analysis with specified features
        """
        try:
            logger.info(f"Starting document analysis for s3://{bucket_name}/{document_name}")
            logger.info(f"Feature types requested: {', '.join(feature_types)}")
            
            request_params = {
                'DocumentLocation': {
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': document_name
                    }
                },
                'FeatureTypes': feature_types
            }
            
            # Add queries if provided
            if queries and 'QUERIES' in feature_types:
                request_params['QueriesConfig'] = {'Queries': queries}
                logger.info(f"Added {len(queries)} custom queries to analysis")
            
            response = self.textract.start_document_analysis(**request_params)
            job_id = response['JobId']
            logger.info(f"Analysis started successfully. Job ID: {job_id}")
            return job_id
            
        except ClientError as e:
            logger.error(f"Failed to start document analysis: {str(e)}")
            return None
    
    def wait_for_completion(self, job_id: str, max_wait_time: int = 300) -> Optional[Dict]:
        """Poll Textract job status until completion or timeout."""
        logger.info(f"Waiting for job {job_id} to complete...")
        start_time = time.time()
        
        while True:
            try:
                response = self.textract.get_document_analysis(JobId=job_id)
                status = response['JobStatus']
                
                if status == 'SUCCEEDED':
                    logger.info("Document analysis completed successfully")
                    return response
                elif status == 'FAILED':
                    logger.error(f"Document analysis failed: {response.get('StatusMessage', 'Unknown error')}")
                    return None
                
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed > max_wait_time:
                    logger.error(f"Timeout waiting for analysis completion ({max_wait_time}s exceeded)")
                    return None
                
                logger.info(f"Status: {status}. Waiting... ({int(elapsed)}s elapsed)")
                time.sleep(5)
                
            except ClientError as e:
                logger.error(f"Error checking job status: {str(e)}")
                return None
    
    def get_all_pages(self, job_id: str) -> List[Dict]:
        """Retrieve all pages of results for multi-page documents."""
        logger.info("Retrieving all pages of analysis results...")
        pages = []
        next_token = None
        page_count = 0
        
        while True:
            try:
                if next_token:
                    response = self.textract.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token
                    )
                else:
                    response = self.textract.get_document_analysis(JobId=job_id)
                
                pages.append(response)
                page_count += 1
                logger.info(f"Retrieved page {page_count} of results")
                
                if 'NextToken' not in response:
                    break
                next_token = response['NextToken']
                
            except ClientError as e:
                logger.error(f"Error retrieving page {page_count + 1}: {str(e)}")
                break
        
        logger.info(f"Total pages retrieved: {page_count}")
        return pages
    
    def extract_layout_elements(self, blocks: List[Dict]) -> Dict[str, List]:
        """Extract LAYOUT blocks into categorized elements (titles, headers, etc.)"""
        logger.info("Extracting layout elements...")
        
        layout = {
            'titles': [],
            'headers': [],
            'section_headers': [],
            'paragraphs': [],
            'lists': [],
            'page_numbers': [],
            'footers': [],
            'figure_areas': [],
            'key_value_areas': []
        }
        
        for block in blocks:
            if block.get('BlockType') == 'LAYOUT_TEXT':
                text = self._get_text_from_block(block, blocks)
                confidence = block.get('Confidence', 0)
                
                if 'LAYOUT_TITLE' in str(block):
                    layout['titles'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_HEADER' in str(block):
                    layout['headers'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_SECTION_HEADER' in str(block):
                    layout['section_headers'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_TEXT' in str(block):
                    layout['paragraphs'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_LIST' in str(block):
                    layout['lists'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_PAGE_NUMBER' in str(block):
                    layout['page_numbers'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_FOOTER' in str(block):
                    layout['footers'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_FIGURE' in str(block):
                    layout['figure_areas'].append({'text': text, 'confidence': confidence})
                elif 'LAYOUT_KEY_VALUE' in str(block):
                    layout['key_value_areas'].append({'text': text, 'confidence': confidence})
        
        for category, items in layout.items():
            logger.info(f"Found {len(items)} {category}")
        
        return layout
    
    def extract_forms(self, blocks: List[Dict]) -> List[Dict]:
        """Extract KEY_VALUE_SET blocks into key-value pairs."""
        logger.info("Extracting form data...")
        
        key_map = {}
        value_map = {}
        block_map = {}
        
        # Create maps for quick lookup
        for block in blocks:
            block_id = block.get('Id')
            block_map[block_id] = block
            
            if block.get('BlockType') == 'KEY_VALUE_SET':
                if 'KEY' in block.get('EntityTypes', []):
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block
        
        forms = []
        for key_id, key_block in key_map.items():
            value_block = self._find_value_block(key_block, value_map)
            
            key_text = self._get_text_from_block(key_block, blocks)
            value_text = ''
            
            if value_block:
                value_text = self._get_text_from_block(value_block, blocks)
            
            if key_text:
                forms.append({
                    'key': key_text,
                    'value': value_text,
                    'confidence': key_block.get('Confidence', 0)
                })
        
        logger.info(f"Extracted {len(forms)} form fields")
        return forms
    
    def extract_tables(self, blocks: List[Dict]) -> List[Dict]:
        """Extract TABLE blocks into structured row-column data."""
        logger.info("Extracting table data...")
        
        tables = []
        table_blocks = [b for b in blocks if b.get('BlockType') == 'TABLE']
        
        for table_block in table_blocks:
            table = {
                'confidence': table_block.get('Confidence', 0),
                'rows': [],
                'row_count': 0,
                'column_count': 0
            }
            
            # Get all cells for this table
            cells = []
            if 'Relationships' in table_block:
                for relationship in table_block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            cell_block = next((b for b in blocks if b.get('Id') == child_id), None)
                            if cell_block and cell_block.get('BlockType') == 'CELL':
                                cells.append(cell_block)
            
            # Organize cells by row and column
            rows = {}
            max_col = 0
            
            for cell in cells:
                row_index = cell.get('RowIndex', 1)
                col_index = cell.get('ColumnIndex', 1)
                
                if row_index not in rows:
                    rows[row_index] = {}
                
                cell_text = self._get_text_from_cell(cell, blocks)
                rows[row_index][col_index] = cell_text
                max_col = max(max_col, col_index)
            
            # Convert to list format
            for row_index in sorted(rows.keys()):
                row_data = []
                for col_index in range(1, max_col + 1):
                    row_data.append(rows[row_index].get(col_index, ''))
                table['rows'].append(row_data)
            
            table['row_count'] = len(table['rows'])
            table['column_count'] = max_col
            tables.append(table)
        
        logger.info(f"Extracted {len(tables)} tables")
        return tables
    
    def extract_queries(self, blocks: List[Dict]) -> List[Dict]:
        """Extract QUERY blocks and their associated answers."""
        logger.info("Extracting query results...")
        
        queries = []
        query_blocks = [b for b in blocks if b.get('BlockType') == 'QUERY']
        
        for query_block in query_blocks:
            query_result = {
                'query': query_block.get('Query', {}).get('Text', ''),
                'alias': query_block.get('Query', {}).get('Alias', ''),
                'answer': '',
                'confidence': 0
            }
            
            # Find associated answer
            if 'Relationships' in query_block:
                for relationship in query_block['Relationships']:
                    if relationship['Type'] == 'ANSWER':
                        for answer_id in relationship['Ids']:
                            answer_block = next((b for b in blocks if b.get('Id') == answer_id), None)
                            if answer_block:
                                query_result['answer'] = self._get_text_from_block(answer_block, blocks)
                                query_result['confidence'] = answer_block.get('Confidence', 0)
            
            queries.append(query_result)
        
        logger.info(f"Extracted {len(queries)} query results")
        return queries
    
    def extract_signatures(self, blocks: List[Dict]) -> List[Dict]:
        """Extract SIGNATURE blocks with confidence scores."""
        logger.info("Extracting signatures...")
        
        signatures = []
        signature_blocks = [b for b in blocks if b.get('BlockType') == 'SIGNATURE']
        
        for sig_block in signature_blocks:
            signature = {
                'page': sig_block.get('Page', 1),
                'confidence': sig_block.get('Confidence', 0),
                'geometry': {
                    'boundingBox': sig_block.get('Geometry', {}).get('BoundingBox', {}),
                    'polygon': sig_block.get('Geometry', {}).get('Polygon', [])
                }
            }
            signatures.append(signature)
        
        logger.info(f"Detected {len(signatures)} signatures")
        return signatures
    
    def _get_text_from_block(self, block: Dict, blocks: List[Dict]) -> str:
        """Extract text content from block by resolving child relationships."""
        text = ''
        
        if 'Text' in block:
            return block['Text']
        
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        child_block = next((b for b in blocks if b.get('Id') == child_id), None)
                        if child_block:
                            if child_block.get('BlockType') == 'WORD':
                                text += child_block.get('Text', '') + ' '
                            elif child_block.get('BlockType') == 'SELECTION_ELEMENT':
                                if child_block.get('SelectionStatus') == 'SELECTED':
                                    text += 'X '
        
        return text.strip()
    
    def _get_text_from_cell(self, cell: Dict, blocks: List[Dict]) -> str:
        """Extract text content from table cells."""
        text = ''
        
        if 'Relationships' in cell:
            for relationship in cell['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        child_block = next((b for b in blocks if b.get('Id') == child_id), None)
                        if child_block and child_block.get('BlockType') == 'WORD':
                            text += child_block.get('Text', '') + ' '
        
        return text.strip()
    
    def _find_value_block(self, key_block: Dict, value_map: Dict) -> Optional[Dict]:
        """Find value block associated with a key block using relationships."""
        if 'Relationships' in key_block:
            for relationship in key_block['Relationships']:
                if relationship['Type'] == 'VALUE':
                    for value_id in relationship['Ids']:
                        if value_id in value_map:
                            return value_map[value_id]
        return None
    
    def process_single_document(self, filename: str, bucket_name: str, 
                               s3_key: str,
                               enable_layout: bool = True,
                               enable_forms: bool = True,
                               enable_tables: bool = True,
                               enable_queries: bool = True,
                               enable_signatures: bool = True,
                               custom_queries: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Complete processing pipeline for a single PDF document:
        1. Start Textract analysis with configured features
        2. Wait for completion and retrieve all pages
        3. Extract different element types based on enabled features
        4. Return structured results with metadata
        """
        
        results = {
            'metadata': {
                'file_path': filename,
                'processing_time': None,
                'status': 'processing'
            },
            'layout': {},
            'forms': [],
            'tables': [],
            'queries': [],
            'signatures': []
        }
        
        start_time = time.time()
        
        try:
            # Prepare feature types
            feature_types = []
            if enable_layout:
                feature_types.append('LAYOUT')
            if enable_forms:
                feature_types.append('FORMS')
            if enable_tables:
                feature_types.append('TABLES')
            if enable_signatures:
                feature_types.append('SIGNATURES')
            
            # Prepare queries
            queries = None
            if enable_queries and custom_queries:
                feature_types.append('QUERIES')
                queries = [{'Text': q, 'Alias': f'Q{i+1}'} for i, q in enumerate(custom_queries)]
            
            # Start analysis
            job_id = self.start_document_analysis(bucket_name, s3_key, feature_types, queries)
            if not job_id:
                raise Exception("Failed to start document analysis")
            
            # Wait for completion
            response = self.wait_for_completion(job_id)
            if not response:
                raise Exception("Document analysis failed or timed out")
            
            # Get all pages
            all_pages = self.get_all_pages(job_id)
            
            # Combine all blocks
            all_blocks = []
            for page in all_pages:
                all_blocks.extend(page.get('Blocks', []))
            
            logger.info(f"Total blocks extracted: {len(all_blocks)}")
            
            # Extract different elements based on enabled features
            if enable_layout:
                results['layout'] = self.extract_layout_elements(all_blocks)
            
            if enable_forms:
                results['forms'] = self.extract_forms(all_blocks)
            
            if enable_tables:
                results['tables'] = self.extract_tables(all_blocks)
            
            if enable_queries:
                results['queries'] = self.extract_queries(all_blocks)
            
            if enable_signatures:
                results['signatures'] = self.extract_signatures(all_blocks)
            
            # Update metadata
            processing_time = time.time() - start_time
            results['metadata']['processing_time'] = f"{processing_time:.2f} seconds"
            results['metadata']['status'] = 'completed'
            results['metadata']['total_blocks'] = len(all_blocks)
            
            logger.info(f"Document processing completed for {filename} in {processing_time:.2f} seconds")
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")
            results['metadata']['status'] = 'failed'
            results['metadata']['error'] = str(e)
            processing_time = time.time() - start_time
            results['metadata']['processing_time'] = f"{processing_time:.2f} seconds"
            return results
    
    def extract_line_items_from_tables(self, tables: Dict) -> List[Dict]:
        """
        Advanced table processing to extract invoice line items:
        - Header row identification with flexible matching
        - Data row processing with field type mapping
        - Summary row detection and filtering
        - Currency parsing and validation
        """
        all_line_items = []
        
        logger.debug(f"Found {len(tables)} table pages to process")
        
        for page, rows_dict in tables.items():
            if not rows_dict:
                logger.debug(f"Page {page}: No rows found")
                continue
                
            # Convert to sorted rows
            rows = []
            max_cols = max((max(r.keys()) for r in rows_dict.values()), default=0)
            
            for r in sorted(rows_dict.keys()):
                row = [rows_dict[r].get(c, "") for c in range(1, max_cols + 1)]
                rows.append(row)
                
            logger.debug(f"Page {page}: Found {len(rows)} rows with {max_cols} columns")
            
            # Print first few rows for debugging
            for i, row in enumerate(rows[:3]):
                logger.debug(f"Row {i}: {row}")
                
            if len(rows) < 2:  # Need at least header + 1 data row
                logger.debug(f"Page {page}: Not enough rows ({len(rows)})")
                continue
                
            # Try to identify headers with more flexibility
            potential_headers = []
            for i in range(min(4, len(rows))):  # Check first 4 rows for headers
                header_matches = [match_header_enhanced(cell) for cell in rows[i]]
                match_count = sum(1 for h in header_matches if h is not None)
                logger.debug(f"Row {i} header matches: {header_matches} (count: {match_count})")
                
                if match_count >= 2:  # At least 2 recognized headers
                    potential_headers.append((i, header_matches, match_count))
                    
            if not potential_headers:
                logger.debug(f"Page {page}: No headers found")
                # Try a more relaxed approach - look for any row with common header-like words
                for i, row in enumerate(rows[:3]):
                    row_text = " ".join(row).lower()
                    if any(word in row_text for word in ["service", "description", "rate", "price", "amount", "total", "qty", "hours"]):
                        logger.debug(f"Row {i} might be a header based on keywords: {row}")
                        # Try to manually map this row
                        manual_headers = []
                        for cell in row:
                            manual_headers.append(match_header_enhanced(cell))
                        if sum(1 for h in manual_headers if h is not None) >= 1:
                            potential_headers.append((i, manual_headers, sum(1 for h in manual_headers if h is not None)))
                            
            if not potential_headers:
                logger.debug(f"Page {page}: Still no headers found, skipping table")
                continue
                
            # Use the best header row (most matches, or first one with good matches)
            header_row_idx, headers, match_count = max(potential_headers, key=lambda x: (x[2], -x[0]))
            logger.debug(f"Using header row {header_row_idx} with headers: {headers}")
            
            data_rows = rows[header_row_idx + 1:]
            logger.debug(f"Processing {len(data_rows)} data rows")
            
            for row_idx, row in enumerate(data_rows):
                if not any(cell.strip() for cell in row):  # Skip empty rows
                    logger.debug(f"Skipping empty row {row_idx}")
                    continue
                    
                logger.debug(f"Processing data row {row_idx}: {row}")
                
                # Check if this is a summary row
                if is_summary_row(row):
                    logger.debug(f"Skipping summary row {row_idx}: {row}")
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
                        
                    logger.debug(f"Processing field '{field}' = '{cell_value}'")
                    
                    if field == "description":
                        # Additional check: if description contains summary terms, skip this row
                        desc_lower = cell_value.lower()
                        if any(indicator in desc_lower for indicator in SUMMARY_INDICATORS):
                            logger.debug("Description contains summary term, skipping row")
                            has_meaningful_data = False
                            break  # Break out of the cell loop for this row
                            
                        line_item["Description"] = cell_value
                        has_meaningful_data = True
                        logger.debug(f"Added description: {cell_value}")
                        
                    elif field == "quantity":
                        qty_info = parse_amount_with_currency(cell_value)
                        if isinstance(qty_info["value"], (int, float)):
                            line_item["Quantity"] = qty_info["value"]
                            has_meaningful_data = True
                            logger.debug(f"Added quantity: {qty_info['value']}")
                        else:
                            # Sometimes quantity is just a number without formatting
                            try:
                                qty_val = float(cell_value)
                                line_item["Quantity"] = qty_val
                                has_meaningful_data = True
                                logger.debug(f"Added quantity (parsed): {qty_val}")
                            except:
                                logger.debug(f"Could not parse quantity: {cell_value}")
                                
                    elif field == "unitprice":
                        price_info = parse_amount_with_currency(cell_value)
                        line_item["UnitPrice"] = price_info
                        if isinstance(price_info["value"], (int, float)):
                            has_meaningful_data = True
                            logger.debug(f"Added unit price: {price_info}")
                            
                    elif field == "amount":
                        amount_info = parse_amount_with_currency(cell_value)
                        line_item["Amount"] = amount_info
                        if isinstance(amount_info["value"], (int, float)):
                            has_meaningful_data = True
                            logger.debug(f"Added amount: {amount_info}")
                
                # Only add line items that have meaningful business data
                if has_meaningful_data:
                    logger.debug(f"Adding line item: {line_item}")
                    all_line_items.append(line_item)
                else:
                    logger.debug("Skipping row - no meaningful data found")
                    
        logger.debug(f"Total line items extracted: {len(all_line_items)}")
        return all_line_items
    
    def parse_extracted_data(self, results: Dict) -> Dict:
        """
        Convert raw Textract results into structured invoice data:
        - Build key-value pairs from forms
        - Combine all text for pattern matching
        - Extract invoice number, date, totals, payment terms
        - Process tables for line items
        - Override with query results when available
        """
        parsed = {
            "InvoiceNumber": None,
            "InvoiceDate": None,
            "LineItems": [],
            "InvoiceTotal": {"value": None, "currency": "", "formatted": ""},
            "PaymentTerms": None
        }
        
        # Build kv_pairs from forms
        kv_pairs = {}
        for form in results.get('forms', []):
            key = form.get('key', '').lower().strip()
            value = form.get('value', '').strip()
            if key and value:
                kv_pairs[key] = value
        
        # Build all_text from layout and other elements
        layout = results.get('layout', {})
        all_text_parts = []
        all_text_parts.extend([t.get('text', '') for t in layout.get('titles', [])])
        all_text_parts.extend([h.get('text', '') for h in layout.get('headers', [])])
        all_text_parts.extend([sh.get('text', '') for sh in layout.get('section_headers', [])])
        all_text_parts.extend([p.get('text', '') for p in layout.get('paragraphs', [])])
        all_text_parts.extend([l.get('text', '') for l in layout.get('lists', [])])
        
        # Add forms text
        all_text_parts.extend([k + " " + v for k, v in kv_pairs.items()])
        
        # Add tables text
        for table in results.get('tables', []):
            for row in table.get('rows', []):
                all_text_parts.append(" ".join([str(cell) for cell in row]))
        
        all_text = " ".join(all_text_parts)
        
        # Build tables_dict for line items extraction
        tables_dict = {}
        for table_idx, table in enumerate(results.get('tables', [])):
            page = table_idx + 1
            tables_dict[page] = {}
            for row_idx, row in enumerate(table['rows'], 1):
                tables_dict[page][row_idx] = {}
                for col_idx, cell in enumerate(row, 1):
                    tables_dict[page][row_idx][col_idx] = cell
        
        # Build table_totals for total extraction
        table_totals = {}
        for page, rows in tables_dict.items():
            for row_idx, row_data in rows.items():
                row_values = list(row_data.values())
                row_text = " ".join([str(cell) for cell in row_values]).lower()
                if "total" in row_text:
                    table_totals[row_text] = " ".join([str(cell) for cell in row_values])
        
        # Extract fields using previous logic
        parsed["InvoiceNumber"] = find_invoice_number(kv_pairs, all_text)
        
        for k, v in kv_pairs.items():
            if any(keyword in k for keyword in DATE_KEYWORDS) and v.strip():
                parsed["InvoiceDate"] = v.strip()
                break
        
        for k, v in kv_pairs.items():
            if any(term in k for term in PAYMENT_TERMS_KEYWORDS) and len(v) > 20:
                parsed["PaymentTerms"] = v.strip()
                break
        
        parsed["LineItems"] = self.extract_line_items_from_tables(tables_dict)
        
        parsed["InvoiceTotal"] = find_invoice_total(kv_pairs, table_totals, all_text)
        
        # Override with queries if available and better
        query_map = {
            "What is the invoice number?": "InvoiceNumber",
            "What is the invoice date?": "InvoiceDate",
            "What is the total amount?": "InvoiceTotal",
            "What is the payment terms?": "PaymentTerms",
        }
        
        for query in results.get('queries', []):
            q_text = query.get('query', '')
            answer = query.get('answer')
            if q_text in query_map and answer:
                if q_text == "What is the total amount?":
                    amount_info = parse_amount_with_currency(answer)
                    if isinstance(amount_info["value"], (int, float)):
                        parsed[query_map[q_text]] = amount_info
                else:
                    parsed[query_map[q_text]] = answer
        
        return parsed
    
    def process_folder(self, folder_path: str, bucket_name: str,
                      s3_prefix: str = "invoices/",
                      output_dir: str = "textract_output",
                      enable_layout: bool = True,
                      enable_forms: bool = True,
                      enable_tables: bool = True,
                      enable_queries: bool = True,
                      enable_signatures: bool = True,
                      custom_queries: Optional[List[str]] = None,
                      max_parallel: int = 3) -> None:
        """
        Batch processing orchestration for folder of PDFs:
        1. Sync local folder with S3 using S3FolderSync
        2. Process files in parallel with ThreadPoolExecutor
        3. Generate individual and summary reports
        4. Save parsed JSON outputs
        5. Display results and statistics
        """
        logger.info("="*80)
        logger.info("STARTING BATCH DOCUMENT PROCESSING")
        logger.info("="*80)
        logger.info(f"Folder: {folder_path}")
        logger.info(f"S3 Bucket: {bucket_name}")
        logger.info(f"Output Directory: {output_dir}")
        logger.info("="*80)
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize S3 sync
        syncer = S3FolderSync(self.s3, bucket_name, s3_prefix)
        
        # Sync folder with S3
        logger.info("\nSYNCING LOCAL FOLDER WITH S3...")
        uploaded, skipped, deleted = syncer.sync_folder(folder_path)
        
        logger.info("\nSYNC SUMMARY:")
        logger.info(f"  Uploaded: {len(uploaded)} files")
        for file in uploaded:
            logger.info(f"     - {file}")
        
        logger.info(f"  Skipped (unchanged): {len(skipped)} files")
        for file in skipped:
            logger.info(f"     - {file}")
        
        logger.info(f"  Deleted from S3: {len(deleted)} files")
        for file in deleted:
            logger.info(f"     - {file}")
        
        # Get all PDF files to process
        pdf_files = list(Path(folder_path).glob("*.pdf"))
        
        if not pdf_files:
            logger.warning("No PDF files found in the specified folder!")
            return
        
        logger.info(f"\nFOUND {len(pdf_files)} PDF FILES TO PROCESS")
        
        # Process files
        all_results = []
        all_parsed = []
        formatter = MarkdownFormatter()
        
        # Process files with limited parallelism
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
            future_to_file = {}
            
            for pdf_file in pdf_files:
                filename = pdf_file.name
                s3_key = f"{s3_prefix}{filename}"
                
                logger.info(f"\nSubmitting {filename} for processing...")
                
                future = executor.submit(
                    self.process_single_document,
                    filename,
                    bucket_name,
                    s3_key,
                    enable_layout,
                    enable_forms,
                    enable_tables,
                    enable_queries,
                    enable_signatures,
                    custom_queries
                )
                future_to_file[future] = filename
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    results = future.result()
                    all_results.append((filename, results))
                    
                    # Parse into required fields
                    parsed = self.parse_extracted_data(results)
                    all_parsed.append((filename, parsed))
                    
                    # Save individual markdown report (intermediate)
                    md_content = formatter.format_results(results, filename)
                    md_filename = f"{Path(filename).stem}_report.md"
                    md_path = os.path.join(output_dir, md_filename)
                    
                    with open(md_path, 'w', encoding='utf-8') as f:
                        f.write(md_content)
                    
                    logger.info(f"Completed processing {filename}")
                    logger.info(f"   Report saved to: {md_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to process {filename}: {str(e)}")
                    all_results.append((filename, {
                        'metadata': {
                            'status': 'failed',
                            'error': str(e),
                            'processing_time': 'N/A'
                        }
                    }))
                    all_parsed.append((filename, {"error": str(e)}))
        
        # Create summary report
        logger.info("\nCreating summary report...")
        summary_content = formatter.create_summary_report(all_results)
        summary_path = os.path.join(output_dir, "SUMMARY_REPORT.md")
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_content)
        
        logger.info(f"Summary report saved to: {summary_path}")
        
        # Save final parsed JSON for all documents
        final_outputs_dir = os.path.join(output_dir, "final_outputs")
        os.makedirs(final_outputs_dir, exist_ok=True)

        for filename, parsed in all_parsed:
            json_filename = f"{Path(filename).stem}_extracted.json"
            json_path = os.path.join(final_outputs_dir, json_filename)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(parsed, f, indent=2, ensure_ascii=False)
            logger.info(f"Extracted JSON saved to: {json_path}")
                
        # Display final output in terminal
        print("\nExtracted Invoice Data Summary:")
        print("=" * 50)
        for filename, parsed in all_parsed:
            print(f"\n{filename}:")
            print(json.dumps(parsed, indent=2))
        print("=" * 50)
                
        # Print final statistics
        logger.info("\n" + "="*80)
        logger.info("BATCH PROCESSING COMPLETED")
        logger.info("="*80)
        
        successful = sum(1 for _, r in all_results if r['metadata'].get('status') == 'completed')
        failed = sum(1 for _, r in all_results if r['metadata'].get('status') == 'failed')
        
        logger.info(f"Successful: {successful}/{len(pdf_files)}")
        logger.info(f"Failed: {failed}/{len(pdf_files)}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("="*80)
        
        # Log quick summary
        logger.info("\nQUICK SUMMARY")
        logger.info("="*50)
        logger.info(f"Total files processed: {len(pdf_files)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"\nReports saved to: {output_dir}/")
        logger.info(f"Main summary: {summary_path}")
        logger.info("="*50)