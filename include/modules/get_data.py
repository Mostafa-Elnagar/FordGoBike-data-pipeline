import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import shutil
import zipfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


S3_BUCKET_URL = os.getenv('S3_BUCKET_URL')
if not S3_BUCKET_URL:
    raise ValueError("S3_BUCKET_URL environment variable is not set.")
BASE_URL = f"{S3_BUCKET_URL}"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("include/modules","")
print(f"Project root directory: {PROJECT_ROOT}")
RAW_DIR = os.path.join(PROJECT_ROOT, "include/data/raw")
EXTRACTED_DIR = os.path.join(PROJECT_ROOT, "include/data/extracted")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "include/data/archive")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

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
        arch_path = os.path.join(ARCHIVE_DIR, filename)
        
        # Download file if it doesn't exist in archive
        if not os.path.exists(arch_path):
            print(f"Downloading {filename}...")
            try:
                r = requests.get(url)
                r.raise_for_status()  # Raise an exception for bad status codes
                with open(raw_path, 'wb') as f:
                    f.write(r.content)
                print(f"Downloaded {filename}")
            except Exception as e:
                print(f"Failed to download {filename}: {e}")
                continue
        
        # Handle CSV files
        if filename.endswith(".csv"):
            target_path = os.path.join(EXTRACTED_DIR, filename)
            if not os.path.exists(target_path):
                try:
                    shutil.copyfile(raw_path, target_path)
                    print(f"Copied {filename} to extracted directory")
                except Exception as e:
                    print(f"Failed to copy {filename}: {e}")
        
        # Handle ZIP files
        elif filename.endswith(".zip"):
            extract_path = os.path.join(EXTRACTED_DIR, filename.replace('.zip', ''))
            if not os.path.exists(extract_path):
                print(f"Extracting {filename}...")
                try:
                    with zipfile.ZipFile(raw_path, 'r') as zip_ref:
                        zip_ref.extractall(EXTRACTED_DIR)
                    print(f"Extracted {filename}")
                except Exception as e:
                    print(f"Failed to extract {filename}: {e}")
        
        # Move to archive after processing
        try:
            if os.path.exists(raw_path):
                shutil.move(raw_path, arch_path)
                print(f"Moved {filename} to archive")
        except Exception as e:
            print(f"Failed to move {filename} to archive: {e}")

def main():
    download_and_extract()

if __name__ == "__main__":
    main()
