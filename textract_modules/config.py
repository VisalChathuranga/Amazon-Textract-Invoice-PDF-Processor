"""
Configuration constants and patterns for Textract invoice processing.
Contains regex patterns, keyword lists, and field mappings used throughout the application.
"""

INVOICE_NUMBER_PATTERNS = [
    r'invoice\s*(?:number|no|#)[\s:]*([A-Z0-9\-]+)',
    r'inv[\s\-]*(?:number|no|#)[\s:]*([A-Z0-9\-]+)',
    r'invoice[\s:]+([A-Z0-9\-]{3,})',
    r'#([A-Z0-9\-]{3,})'
]

CURRENCY_SYMBOLS = ["€", "$", "£", "¥", "₹"]

HEADER_PATTERNS = {
    "description": [
        "description", "service", "item", "details", "work performed",
        "service description", "product", "article", "task", "work", "desc"
    ],
    "quantity": [
        "qty", "quantity", "hours", "units", "hrs/qty", "hrs",
        "units", "amount", "number", "count", "count", "hrs", "hours", "hrs qty", "hrs/qty"
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

SUMMARY_INDICATORS = [
    "subtotal", "sub total", "sub-total", "sub_total", "total", "grand total",
    "final total", "net total", "gross total", "vat", "tax", "vat 19%", "sales tax",
    "discount", "adjustment", "fee discount", "balance", "amount due", "total due",
    "fees and disbursements", "gross amount", "net amount", "incl. vat", "including vat",
    "excl. vat", "adjusted fees", "total adjusted"
]

TOTAL_KEYWORDS = [
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
    ("total incl", 13),
    ("incl. vat", 14),
    ("including vat", 14),
    ("total fees and disbursements", 15),
    ("balance due", 8),
    ("amount payable", 7)
]

DATE_KEYWORDS = ["invoice date", "date", "bill date", "issued"]

PAYMENT_TERMS_KEYWORDS = ["payment", "terms", "due"]