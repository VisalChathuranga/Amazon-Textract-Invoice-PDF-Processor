"""
Markdown formatting classes for generating human-readable reports.
Converts Textract JSON output into structured markdown documents.
"""
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class MarkdownFormatter:
    """Formats Textract extraction results into well-structured markdown reports."""
    
    @staticmethod
    def format_results(results: Dict, filename: str) -> str:
        """
        Creates detailed markdown report for a single document:
        - Metadata section with processing status
        - Layout elements (titles, headers, paragraphs)
        - Form fields as markdown table
        - Tables with row/column formatting
        - Query answers and signature detections
        """
        md = []
        
        # Header
        md.append(f"# Document Analysis Report: {filename}")
        md.append(f"\n*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
        
        # Metadata section
        md.append("## Processing Metadata\n")
        metadata = results.get('metadata', {})
        md.append(f"- **Status:** {metadata.get('status', 'Unknown')}")
        md.append(f"- **Processing Time:** {metadata.get('processing_time', 'N/A')}")
        md.append(f"- **Total Blocks:** {metadata.get('total_blocks', 0)}")
        
        # Layout Elements
        layout = results.get('layout', {})
        if any(layout.values()):
            md.append("\n## Document Structure\n")
            
            # Titles
            if layout.get('titles'):
                md.append("### Document Titles\n")
                for i, title in enumerate(layout['titles'], 1):
                    md.append(f"{i}. **{title.get('text', '')}** *(Confidence: {title.get('confidence', 0):.1f}%)*")
            
            # Headers
            if layout.get('headers'):
                md.append("\n### Headers\n")
                for header in layout['headers']:
                    md.append(f"- {header.get('text', '')} *(Confidence: {header.get('confidence', 0):.1f}%)*")
            
            # Section Headers
            if layout.get('section_headers'):
                md.append("\n### Section Headers\n")
                for header in layout['section_headers']:
                    md.append(f"- **{header.get('text', '')}** *(Confidence: {header.get('confidence', 0):.1f}%)*")
            
            # Paragraphs (first few)
            if layout.get('paragraphs'):
                md.append("\n### Main Content (Sample)\n")
                for para in layout['paragraphs'][:3]:  # Show first 3 paragraphs
                    text = para.get('text', '')
                    if len(text) > 200:
                        text = text[:200] + "..."
                    md.append(f"\n> {text}\n")
                if len(layout['paragraphs']) > 3:
                    md.append(f"\n*... and {len(layout['paragraphs']) - 3} more paragraphs*")
            
            # Lists
            if layout.get('lists'):
                md.append("\n### Lists Found\n")
                for lst in layout['lists']:
                    md.append(f"- {lst.get('text', '')}")
        
        # Forms Data
        if results.get('forms'):
            md.append("\n## Form Fields\n")
            md.append("\n| Field | Value | Confidence |")
            md.append("|-------|-------|------------|")
            for form in results['forms'][:20]:  # Limit to 20 fields for readability
                key = form.get('key', '').replace('|', '\\|')
                value = form.get('value', '').replace('|', '\\|')
                confidence = form.get('confidence', 0)
                md.append(f"| {key} | {value} | {confidence:.1f}% |")
            
            if len(results['forms']) > 20:
                md.append(f"\n*... and {len(results['forms']) - 20} more fields*")
        
        # Tables
        if results.get('tables'):
            md.append("\n## Tables\n")
            for i, table in enumerate(results['tables'], 1):
                md.append(f"\n### Table {i}")
                md.append(f"*Dimensions: {table['row_count']} rows × {table['column_count']} columns*")
                md.append(f"*Confidence: {table['confidence']:.1f}%*\n")
                
                if table.get('rows'):
                    # Create markdown table
                    rows = table['rows'][:10]  # Limit to first 10 rows
                    if rows:
                        # Header row
                        header = rows[0] if rows else []
                        md.append("| " + " | ".join(str(cell).replace('|', '\\|') for cell in header) + " |")
                        md.append("|" + "---|" * len(header))
                        
                        # Data rows
                        for row in rows[1:]:
                            md.append("| " + " | ".join(str(cell).replace('|', '\\|') for cell in row) + " |")
                        
                        if len(table['rows']) > 10:
                            md.append(f"\n*... and {len(table['rows']) - 10} more rows*")
        
        # Query Results
        if results.get('queries'):
            md.append("\n## Custom Query Results\n")
            for query in results['queries']:
                md.append(f"\n**Q:** {query.get('query', '')}")
                md.append(f"**A:** {query.get('answer', 'No answer found')}")
                md.append(f"*Confidence: {query.get('confidence', 0):.1f}%*\n")
        
        # Signatures
        if results.get('signatures'):
            md.append("\n## Signatures\n")
            md.append(f"\n**Total signatures detected:** {len(results['signatures'])}\n")
            for i, sig in enumerate(results['signatures'], 1):
                md.append(f"- **Signature {i}:** Page {sig.get('page', 'N/A')}, Confidence: {sig.get('confidence', 0):.1f}%")
        
        # Footer
        md.append("\n---")
        md.append("\n*This document was automatically generated by AWS Textract*")
        
        return "\n".join(md)
    
    @staticmethod
    def create_summary_report(all_results: List[Tuple[str, Dict]]) -> str:
        """
        Generates batch processing summary report:
        - Overview table with document status and statistics
        - Aggregate counts of forms, tables, signatures
        - Links to individual document reports
        """
        md = []
        
        # Header
        md.append("# Batch Processing Summary Report")
        md.append(f"\n*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
        md.append(f"**Total Documents Processed:** {len(all_results)}\n")
        
        # Summary table
        md.append("## Document Overview\n")
        md.append("| Document | Status | Processing Time | Forms | Tables | Signatures |")
        md.append("|----------|--------|-----------------|-------|--------|------------|")
        
        total_forms = 0
        total_tables = 0
        total_signatures = 0
        
        for filename, results in all_results:
            status = results['metadata'].get('status', 'Unknown')
            time = results['metadata'].get('processing_time', 'N/A')
            forms = len(results.get('forms', []))
            tables = len(results.get('tables', []))
            signatures = len(results.get('signatures', []))
            
            total_forms += forms
            total_tables += tables
            total_signatures += signatures
            
            status_text = "Completed" if status == "completed" else "Failed"
            md.append(f"| {filename} | {status_text} {status} | {time} | {forms} | {tables} | {signatures} |")
        
        # Statistics
        md.append("\n## Overall Statistics\n")
        md.append(f"- **Total Form Fields Extracted:** {total_forms}")
        md.append(f"- **Total Tables Extracted:** {total_tables}")
        md.append(f"- **Total Signatures Detected:** {total_signatures}")
        
        # Individual document reports
        md.append("\n## Individual Document Reports\n")
        for filename, _ in all_results:
            report_name = f"{Path(filename).stem}_report.md"
            md.append(f"- [{filename}](./{report_name})")
        
        return "\n".join(md)