import os
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
import json
import time

# ------------------------------
# Settings
# ------------------------------
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

OCR_LANGS = 'fra+ara+eng'
DOWNLOAD_DIR = "/home/runner/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ------------------------------
# Helper Functions
# ------------------------------
def log_step(msg):
    print(f"\n🔹 {msg}\n")

def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
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
log_step("Fetching tender data...")

today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
url = f"https://tmproject.tendersontime.org/tmpApi/tender-pull-json-targetup.php?username=targetup&key=vfsyqvdkmfgp5bk34&date={yesterday}"

response = requests.get(url)
data = response.json()

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
df['notice_text'] = ''
df['additional_text_all'] = ''

# ------------------------------
# Extract Text from Documents
# ------------------------------
log_step("Extracting text from documents...")

for idx, row in df.iterrows():
    log_step(f"Processing tender {idx+1}/{len(df)}: {row['tender_notice_no']}")

    # Notice document
    notice_url = row['notice_document']
    try:
        r = requests.get(notice_url)
        if r.status_code == 200:
            notice_file_name = os.path.join(DOWNLOAD_DIR, notice_url.split('/')[-1])
            with open(notice_file_name, 'wb') as f:
                f.write(r.content)
            df.at[idx, 'notice_text'] = extract_text_from_file(r.content, notice_file_name)
            print(f"✅ Notice document processed: {notice_file_name}")
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
                file_name = os.path.join(DOWNLOAD_DIR, url.split('/')[-1].lower())
                with open(file_name, 'wb') as f:
                    f.write(r.content)
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
                print(f"✅ Additional document processed: {file_name}")
            else:
                print(f"⚠️ Failed to download {url}: {r.status_code}")
        except Exception as e:
            print(f"⚠️ Error processing {url}: {e}")

    final_text_list = []
    for doc_type, texts in doc_type_text.items():
        if texts:
            final_text_list.append(f"Name of the documents {doc_type}:\n" + "\n---\n".join(texts))

    df.at[idx, 'additional_text_all'] = "\n\n".join(final_text_list)

# ------------------------------
# Send to n8n Webhook
# ------------------------------
WEBHOOK_URL = "https://targetup.app.n8n.cloud/webhook/985e2b92-e43f-4551-9e2c-871a2209995a"

for idx, row in df.iterrows():
    payload = row.to_dict()
    log_step(f"Sending row {idx+1}/{len(df)} to n8n...")
    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=120)
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            response_json = response.text

        if response.status_code == 200:
            print(f"✅ Row {idx+1} processed. Workflow finished.")
        else:
            print(f"❌ Row {idx+1} failed with status code: {response.status_code}")
            print(response.text)
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error sending row {idx+1}: {e}")

log_step("All rows processed.")
