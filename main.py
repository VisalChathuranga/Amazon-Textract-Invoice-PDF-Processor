"""
Main driver script for the Textract invoice processor.
Configures and initiates the batch processing pipeline.
"""

import boto3
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from textract_modules.textract_client import TextractPDFProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.FileHandler(f'textract_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    
    """
    Main execution function:
    - Sets configuration parameters (paths, bucket names, region)
    - Defines custom queries for Textract
    - Validates input folder and files
    - Initializes TextractPDFProcessor
    - Starts batch folder processing
    - Handles errors and provides user feedback
    """
    
    # Configuration
    FOLDER_PATH = "invoices" 
    S3_BUCKET_NAME = "visal-invoice-processing-bucket-2"  
    S3_PREFIX = "invoices/"  
    OUTPUT_DIR = "textract_output"  
    AWS_REGION = "us-east-1"  
    
    # Custom queries (optional) - these will be applied to all documents
    custom_queries = [
        "What is the total amount?",
        "What is the invoice date?",
        "What is the invoice number?",
        "Who is the vendor or supplier?",
        "What is the due date?",
        "What is the payment terms?",
        "What is the tax amount?",
        "What is the customer name?"
    ]
    
    # Create invoices folder if it doesn't exist
    if not os.path.exists(FOLDER_PATH):
        os.makedirs(FOLDER_PATH)
        logger.info(f"Created folder: {FOLDER_PATH}")
        logger.warning("Please add PDF files to this folder and run again.")
        return
    
    # Check if folder has PDF files
    pdf_count = len(list(Path(FOLDER_PATH).glob("*.pdf")))
    if pdf_count == 0:
        logger.warning(f"No PDF files found in '{FOLDER_PATH}' folder!")
        logger.warning("Please add PDF files and run again.")
        return
    
    logger.info(f"\nStarting batch processing of {pdf_count} PDF files...")
    logger.info(f"Input folder: {FOLDER_PATH}")
    logger.info(f"S3 bucket: {S3_BUCKET_NAME}")
    logger.info(f"Output folder: {OUTPUT_DIR}")
    logger.info("-" * 50)
    
    try:
        # Initialize processor
        processor = TextractPDFProcessor(region_name=AWS_REGION)
        
        # Process entire folder
        processor.process_folder(
            folder_path=FOLDER_PATH,
            bucket_name=S3_BUCKET_NAME,
            s3_prefix=S3_PREFIX,
            output_dir=OUTPUT_DIR,
            enable_layout=True,
            enable_forms=True,
            enable_tables=True,
            enable_queries=True,
            enable_signatures=True,
            custom_queries=custom_queries,
            max_parallel=3  # Process up to 3 documents in parallel
        )
        
        logger.info("\nBatch processing completed successfully!")
        logger.info(f"Check '{OUTPUT_DIR}' folder for detailed reports.")
        
    except Exception as e:
        logger.error(f"\nError: {str(e)}")
        logger.error("Please check the logs for more details.")

if __name__ == "__main__":
    main()