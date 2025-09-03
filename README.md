# Amazon-Textract-invoice-processor-

conda create -n invoice-textract-processor python=3.10
conda activate invoice-textract-processor
pip install -r requirements.txt

aws configure

python invoice_processor.py
