from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import sys

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("dags","")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

EXTRACTED_DIR = os.path.join(PROJECT_ROOT, "include/data/extracted")

from include.modules.email_sender.sender import task_failure_alert

with DAG(
    dag_id="ddl_dag",
    description="""Initializas the fordgobike database and creates bronze, silver, and gold schemas""",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": 1,
    },
    start_date=datetime.now(),
    end_date=None,
    schedule="@once",
    catchup=False,
    template_searchpath=[os.path.join(PROJECT_ROOT, "include/sql")]
) as dag:


    init_database = BashOperator(
        task_id="initialize_database",
        bash_command=f"python {os.path.join(PROJECT_ROOT, 'include/sql/bronze/init_db.py')}",
        on_failure_callback=task_failure_alert,
    )

    silver_ddl = SQLExecuteQueryOperator(
        task_id="create_silver",
        sql="silver/silver_ddl.sql",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )

    gold_ddl = SQLExecuteQueryOperator(
        task_id="create_gold",
        sql="gold/gold_ddl.sql",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )

    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_dag',
        trigger_dag_id='bronze_dag',  
        wait_for_completion=False,    
        reset_dag_run=True,  
    )

    init_database >> [silver_ddl, gold_ddl] >> trigger_bronze