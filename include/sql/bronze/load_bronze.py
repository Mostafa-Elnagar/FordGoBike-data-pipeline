import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import csv
from io import StringIO
import time
import sys
import warnings

warnings.filterwarnings('ignore')
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("include/sql/bronze","")

current_dir = os.path.dirname(os.path.abspath(__file__))
modules_path = os.path.abspath(os.path.join(current_dir, "../../modules"))
if modules_path not in sys.path:
    sys.path.append(modules_path)

try:
    from get_locations import enrich_locations
except ImportError:
    from modules.get_locations import enrich_locations


load_dotenv()

RAW_DIR = os.path.join(PROJECT_ROOT, "include/data/raw")
# Get EXTRACTED_DIR from command-line argument if provided, else use default
if len(sys.argv) > 1:
    EXTRACTED_DIR = Path(sys.argv[1])
else:
    EXTRACTED_DIR = Path(os.path.join(PROJECT_ROOT, "include/data/extracted"))
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "include/data/archive")


TABLE_NAME = "bronze.bike_trips"

def get_connection():
    connection = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST') ,
        port=os.getenv('POSTGRES_PORT') ,
        database=os.getenv('DATABASE_NAME'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    return connection

def add_period_column(file_path, period_value):
    buffer = StringIO()
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        writer = csv.writer(buffer)

        headers = next(reader)
        headers.append("period")
        writer.writerow(headers)

        for row in reader:
            row.append(period_value)
            writer.writerow(row)

    buffer.seek(0)
    return buffer

def load_csv(file_path):
    print(f"Loading {file_path.name}")
    conn = get_connection()
    cur = conn.cursor()

    # Extract period from filename
    period = file_path.name.split("-")[0]

    # Modify file in-memory
    buffered_csv = add_period_column(file_path, period)

    # Read and sanitize header
    header_line = buffered_csv.readline().strip()
    column_list = ', '.join([col.strip() for col in header_line.split(',')])

    buffered_csv.seek(0)
    copy_sql = f"""
        COPY {TABLE_NAME} (
            {column_list}
        )
        FROM STDIN WITH CSV HEADER DELIMITER ',';
    """
    cur.copy_expert(sql=copy_sql, file=buffered_csv)

    conn.commit()
    cur.close()
    conn.close()

    # Move file to archive
    os.remove(str(file_path))
    print(f"{file_path.name} loaded and deleted.\n")

def load_locations():
    conn = get_connection()
    try:
        enrich_locations(conn)
        print("Location data enrichment completed.")
    except Exception as e:
        print(f"Error enriching location data: {e}")
    finally:
        conn.close()

def load_trips():
    # Ensure EXTRACTED_DIR is a Path object
    files = sorted(EXTRACTED_DIR.glob("*.csv"))
    if not files:
        print("No new CSV files to load.")
        return
    for file in files:
        try:
            start = time.time()
            load_csv(file)
        except Exception as e:
            print(f"Error loading {file.name}: {e}")
        finally:
            time_spent = time.time() - start
            print(f"Time Spent For Process {time_spent}")
            print("-"*20)

    

if __name__ == "__main__":
    load_trips()
    load_locations()
