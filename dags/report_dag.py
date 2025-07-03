from airflow import DAG
from airflow.operators.python import PythonOperator

import os

from datetime import timedelta
import sys 

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("dags","")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from include.modules.email_sender.sender import email_dags_overview


with DAG(
    dag_id="report_dag",
    description="Email report DAG",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(seconds=2),
    },
    start_date=None,
    end_date=None,
    schedule=None,
    catchup=False,
) as dag:
    
    send_email_dags_overview = PythonOperator(
        task_id="send_email_dags_overview",
        python_callable=email_dags_overview,
    )

    send_email_dags_overview