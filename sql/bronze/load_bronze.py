import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import csv
from io import StringIO
import time
from typing import Dict
import sys
import warnings

warnings.filterwarnings('ignore')


# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from modules.get_locations import enrich_locations


load_dotenv()

RAW_DIR = Path("data/raw")
ARCHIVE_DIR = Path("data/archive")
EXTRACTED_DIR = Path("data/extracted")

TABLE_NAME = "bronze.bike_trips"

def get_connection():
    connection = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database='fordgobike',
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
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

def main():
    load_trips()
    load_locations()

if __name__ == "__main__":
    main()
