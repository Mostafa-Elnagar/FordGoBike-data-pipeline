from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("dags","")

EXTRACTED_DIR = os.path.join(PROJECT_ROOT, "include/data/extracted")

with DAG(
    dag_id="bronze_dag",
    description="""Bronze DAG for data extraction and loading into the database.
                This DAG extracts data from a source, initializes the database, and loads the extracted data into the bronze layer.""",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": 60,
    },
    start_date=datetime(2025, 6, 21, 10, 0),
    end_date=datetime(2025, 7, 23, 10, 30),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command=f"python {os.path.join(PROJECT_ROOT, 'include/modules/get_data.py')} {EXTRACTED_DIR}",
    )

    init_database = BashOperator(
        task_id="initialize_database",
        bash_command=f"python {os.path.join(PROJECT_ROOT, 'include/sql/bronze/init_db.py')}",
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command=f"python {os.path.join(PROJECT_ROOT, 'include/sql/bronze/load_bronze.py')} {EXTRACTED_DIR}",
    )

    extract_task >> init_database >> load_task