from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("dags","")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

EXTRACTED_DIR = os.path.join(PROJECT_ROOT, "include/data/extracted")

from include.modules.email_sender.sender import task_failure_alert

with DAG(
    dag_id="bronze_dag",
    description="""Bronze DAG for data extraction and loading into the database.
                This DAG extracts data from a source, initializes the database, and loads the extracted data into the bronze layer.""",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    start_date=None,
    end_date=None,
    schedule="@daily",
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command=f"python {os.path.join(PROJECT_ROOT, 'include/modules/get_data.py')} {EXTRACTED_DIR}",
        on_failure_callback=task_failure_alert,
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command=f"python {os.path.join(PROJECT_ROOT, 'include/sql/bronze/load_bronze.py')} {EXTRACTED_DIR}",
        on_failure_callback=task_failure_alert,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_dag',
        trigger_dag_id='silver_dag',  
        wait_for_completion=False,    
        reset_dag_run=True,           
    )

    extract_task >> load_task >> trigger_silver