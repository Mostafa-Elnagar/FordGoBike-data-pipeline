import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import shutil
import zipfile

BASE_URL = "https://s3.amazonaws.com/fordgobike-data/"
RAW_DIR = "data/raw"
EXTRACTED_DIR = "data/extracted"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(EXTRACTED_DIR, exist_ok=True)

def get_links():
    resp = requests.get(BASE_URL)
    soup = BeautifulSoup(resp.content, 'xml')
    keys = soup.find_all('Key')
    links = []
    for key in keys:
        filename = key.text
        if filename.endswith(('.zip', '.csv')):
            links.append(BASE_URL + filename)
    return links

def download_and_extract():
    links = get_links()
    print(f"Found {len(links)} files to download")
    
    for url in tqdm(links, desc="Processing files"):
        filename = url.split('/')[-1]
        raw_path = os.path.join(RAW_DIR, filename)
        
        # Download file if it doesn't exist
        if not os.path.exists(raw_path):
            print(f"Downloading {filename}...")
            r = requests.get(url)
            with open(raw_path, 'wb') as f:
                f.write(r.content)
        
        # Handle CSV files
        if filename.endswith(".csv"):
            target_path = os.path.join(EXTRACTED_DIR, filename)
            if not os.path.exists(target_path):
                shutil.copyfile(raw_path, target_path)
                print(f"Copied {filename} to extracted directory")
        
        # Handle ZIP files
        elif filename.endswith(".zip"):
            extract_path = os.path.join(EXTRACTED_DIR, filename.replace('.zip', ''))
            if not os.path.exists(extract_path):
                print(f"Extracting {filename}...")
                with zipfile.ZipFile(raw_path, 'r') as zip_ref:
                    zip_ref.extractall(EXTRACTED_DIR)

if __name__ == "__main__":
    download_and_extract()
