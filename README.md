# Amazon Textract Invoice Processor

A Python-based invoice processing system that uses AWS Textract to extract structured data from PDF invoices, including invoice numbers, dates, line items, and totals.

## Features

- **Automated PDF Processing**: Processes PDF invoices from a local directory
- **AWS Textract Integration**: Uses advanced OCR and document analysis capabilities
- **Smart Data Extraction**: Extracts key invoice fields including:
  - Invoice numbers
  - Invoice dates
  - Line items with descriptions, quantities, unit prices, and amounts
  - Invoice totals with currency detection
  - Payment terms
- **Duplicate Detection**: Prevents reprocessing of unchanged files using MD5 hashing
- **Multi-Currency Support**: Handles various currency symbols (€, $, £, ¥, ₹)
- **Enhanced Line Item Detection**: Improved algorithm to capture single-line and multi-line invoice items
- **JSON Output**: Saves results in structured JSON format for easy integration

## Prerequisites

- Python 3.10 or higher
- AWS Account with Textract service enabled
- AWS CLI configured with appropriate permissions
- PDF invoices to process

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/amazon-textract-invoice-processor.git
cd amazon-textract-invoice-processor
```

### 2. Create Virtual Environment
```bash
conda create -n invoice-textract-processor python=3.10
conda activate invoice-textract-processor
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure AWS Credentials
```bash
aws configure
```

You'll need to provide:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name (e.g., `us-east-1`)
- Default output format (e.g., `json`)

## Configuration

Before running the processor, update the configuration variables in `invoice_processor.py`:

```python
# ===== CONFIGURATION =====
AWS_REGION = "us-east-1"                    # Your AWS region
BUCKET_NAME = "your-invoice-bucket-name"    # Your S3 bucket name
INVOICE_DIR = "invoices"                    # Local directory containing PDF invoices
RESULTS_DIR = "results"                     # Output directory for JSON results
```

## Required AWS Permissions

Your AWS user/role needs the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "textract:StartDocumentAnalysis",
                "textract:GetDocumentAnalysis",
                "s3:PutObject",
                "s3:GetObject",
                "s3:HeadObject"
            ],
            "Resource": "*"
        }
    ]
}
```

## Directory Structure

```
amazon-textract-invoice-processor/
├── invoice_processor.py     # Main processing script
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── invoices/               # Input directory (place PDF files here)
└── results/                # Output directory (JSON files created here)
```

## Usage

### 1. Prepare Invoice Files
Place your PDF invoice files in the `invoices/` directory:
```bash
mkdir invoices
# Copy your PDF invoices to the invoices/ directory
```

### 2. Run the Processor
```bash
python invoice_processor.py
```

The script will:
1. Upload new/changed PDFs to your S3 bucket
2. Process each PDF with AWS Textract
3. Extract structured data
4. Save results as JSON files in the `results/` directory
5. Display a summary of extracted information

### 3. Review Results
Check the `results/` directory for JSON files containing extracted invoice data:

```json
{
    "InvoiceNumber": "INV-3337",
    "InvoiceDate": "January 31, 2016",
    "InvoiceTotal": "93.50 $",
    "LineItems": [
        {
            "Description": "Web Design",
            "Quantity": 1.0,
            "UnitPrice": {
                "value": 85.0,
                "currency": "$",
                "formatted": "85.00 $"
            },
            "Amount": {
                "value": 85.0,
                "currency": "$",
                "formatted": "85.00 $"
            }
        }
    ],
    "PaymentTerms": null
}
```

## Dependencies

Create a `requirements.txt` file with the following content:

```txt
boto3>=1.26.0
botocore>=1.29.0
```

## Troubleshooting

### Common Issues

1. **AWS Credentials Error**
   - Ensure `aws configure` was run successfully
   - Verify your AWS credentials have the required permissions

2. **S3 Bucket Access Error**
   - Make sure the S3 bucket exists
   - Verify bucket permissions allow PutObject and GetObject

3. **Empty Line Items**
   - Check if your invoice has a table structure
   - Review the debug output for table detection issues
   - Ensure column headers match expected patterns

4. **Textract Job Failed**
   - Check if the PDF is readable and not corrupted
   - Verify the file size is within Textract limits
   - Ensure the PDF contains text (not just images)

### Debug Mode

The script includes debug output that shows:
- Table detection results
- Header matching process
- Line item extraction details

Monitor the console output for debugging information.

## Supported Invoice Formats

The processor works best with:
- **Tabular invoices** with clear column headers
- **Standard fields** like Description, Quantity, Rate/Price, Amount
- **PDF text** (not scanned images, though Textract handles those too)
- **Common currencies** (USD, EUR, GBP, JPY, INR)

## Limitations

- Requires AWS Textract service (paid service)
- Processing time depends on document complexity
- Best results with structured, tabular invoice layouts
- Some complex invoice layouts may require manual review

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with various invoice formats
5. Submit a pull request

