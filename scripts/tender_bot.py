import requests
import pandas as pd
from datetime import datetime, timedelta
import io
import zipfile
from docx import Document
import PyPDF2
from bs4 import BeautifulSoup
import pytesseract
from PIL import Image
import re
import unicodedata
import requests
import json
import time
import os

# ------------------------------
# Settings
# ------------------------------
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# OCR configuration
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
OCR_LANGS = 'fra+ara+eng'

# ------------------------------
# Helper Functions
# ------------------------------
def clean_text(text):
    if not isinstance(text, str):
        return ""
    # Normalize unicode
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    # Remove control characters
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', ' ', text)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text)
    # Keep readable characters
    text = re.sub(r'[^a-zA-Z0-9À-ž.,;:?!\'"()\-/%€$@#\s]', '', text)
    return text.strip()

def extract_text_from_file(file_bytes, file_name):
    file_name = file_name.lower()
    try:
        if file_name.endswith('.zip'):
            text_data = ""
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                for fname in z.namelist():
                    with z.open(fname) as f:
                        text_data += extract_text_from_file(f.read(), fname) + "\n"
            return clean_text(text_data)

        elif file_name.endswith('.docx'):
            doc = Document(io.BytesIO(file_bytes))
            return clean_text("\n".join([p.text for p in doc.paragraphs]))

        elif file_name.endswith('.pdf'):
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
            text = "\n".join(
                [page.extract_text() for page in pdf_reader.pages if page.extract_text()]
            )
            return clean_text(text)

        elif file_name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_bytes))
            return clean_text(df.to_string(index=False))

        elif file_name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(file_bytes))
            return clean_text(df.to_string(index=False))

        elif file_name.endswith(('.html', '.htm')):
            html_text = file_bytes.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_text, 'html.parser')
            return clean_text(soup.get_text(separator='\n', strip=True))

        elif file_name.endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff', '.webp')):
            image = Image.open(io.BytesIO(file_bytes))
            text = pytesseract.image_to_string(image, lang=OCR_LANGS)
            return clean_text(text)

        else:
            return f"[Binary file: {file_name}, cannot extract text]"

    except Exception as e:
        return f"[Error extracting from {file_name}: {e}]"

# ------------------------------
# Fetch Tender Data
# ------------------------------
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

BASE_URL = os.getenv("URL")
url = f"{BASE_URL}{yesterday}" if BASE_URL.endswith("date=") else BASE_URL
response = requests.get(url)
data = response.json()



# Prepare DataFrame
rows = []
for tender in data.get('data', []):
    row = {
        'tot_id': tender.get('tot_id', ''),
        'country_iso': tender.get('country_iso', ''),
        'tender_notice_no': tender.get('tender_notice_no', ''),
        'title': tender.get('title', ''),
        'description': tender.get('description', ''),
        'cpv': tender.get('cpv', ''),
        'posting_date': tender.get('posting_date', ''),
        'closing_date': tender.get('closing_date', ''),
        'document_type': tender.get('document_type', ''),
        'bidding_type': tender.get('bidding_type', ''),
        'purchaser_name': tender.get('purchaser_name', ''),
        'purchaser_country': tender.get('purchaser_country', ''),
        'purchaser_address': tender.get('purchaser_address', ''),
        'purchaser_email': tender.get('purchaser_email', ''),
        'purchaser_website': tender.get('purchaser_website', ''),
        'tender_value': tender.get('tender_value', ''),
        'currency': tender.get('currency', ''),
        'financier': tender.get('financier', ''),
        'notice_document': tender.get('notice_document', ''),
        'additional_documents': ', '.join(tender.get('additional_documents', [])) if tender.get('additional_documents') else ''
    }
    rows.append(row)

df = pd.DataFrame(rows)

# Initialize columns for extracted text
df['notice_text'] = ''
df['additional_text_all'] = ''

# ------------------------------
# Extract Text from Documents
# ------------------------------
for idx, row in df.iterrows():
    # Notice document
    notice_url = row['notice_document']
    try:
        r = requests.get(notice_url)
        if r.status_code == 200:
            notice_file_name = notice_url.split('/')[-1]
            df.at[idx, 'notice_text'] = extract_text_from_file(r.content, notice_file_name)
        else:
            df.at[idx, 'notice_text'] = f"Failed to download: {r.status_code}"
    except Exception as e:
        df.at[idx, 'notice_text'] = f"Error: {str(e)}"

    # Additional documents
    add_docs_urls = row['additional_documents'].split(', ') if row['additional_documents'] else []
    doc_type_text = {'PDF': [], 'DOC': [], 'EXCEL': [], 'CSV': [], 'IMAGE': []}

    for url in add_docs_urls:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                file_name = url.split('/')[-1].lower()
                text = extract_text_from_file(r.content, file_name)
                if file_name.endswith('.pdf'):
                    doc_type_text['PDF'].append(f"{file_name}:\n{text}")
                elif file_name.endswith(('.doc', '.docx')):
                    doc_type_text['DOC'].append(f"{file_name}:\n{text}")
                elif file_name.endswith(('.xls', '.xlsx')):
                    doc_type_text['EXCEL'].append(f"{file_name}:\n{text}")
                elif file_name.endswith('.csv'):
                    doc_type_text['CSV'].append(f"{file_name}:\n{text}")
                elif file_name.endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff', '.webp')):
                    doc_type_text['IMAGE'].append(f"{file_name}:\n{text}")
                else:
                    doc_type_text['DOC'].append(f"[Other: {file_name}]\n{text}")
            else:
                print(f"⚠️ Failed to download {url}: {r.status_code}")
        except Exception as e:
            print(f"⚠️ Error processing {url}: {e}")

    # Combine all additional text
    final_text_list = []
    for doc_type, texts in doc_type_text.items():
        if texts:
            final_text_list.append(f"Name of the documents {doc_type}:\n" + "\n---\n".join(texts))

    df.at[idx, 'additional_text_all'] = "\n\n".join(final_text_list)


WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

for idx, row in df.iterrows():
    payload = row.to_dict()
    print(f"\n🚀 Sending row {idx+1}/{len(df)} to n8n...")

    try:
        # This will wait until n8n responds
        response = requests.post(WEBHOOK_URL, json=payload, timeout=120)  # 120s timeout in case workflow is slow

        # Parse response
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            response_json = response.text

        if response.status_code == 200:
            print(f"✅ Row {idx+1} processed. Workflow finished.")
            print(json.dumps(response_json, indent=4, ensure_ascii=False))
        else:
            print(f"❌ Row {idx+1} failed with status code: {response.status_code}")
            print(response.text)

        # Optional: small delay between requests to avoid flooding n8n
        time.sleep(1)

    except Exception as e:
        print(f"❌ Error sending row {idx+1}: {e}")




