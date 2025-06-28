from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import os
from pathlib import Path
from datetime import timedelta
import sys 

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("dags","")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from include.modules.email_sender.sender import task_failure_alert


with DAG(
    dag_id="silver_dag",
    description="Silver ETL DAG",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=1),
    },
    start_date=None,
    end_date=None,
    schedule=None,
    catchup=False,
    tags=["silver"],
    template_searchpath=[os.path.join(PROJECT_ROOT, "include/sql/silver")],
) as dag:

    load_silver = SQLExecuteQueryOperator(
        task_id="load_silver",
        sql="silver_load.sql",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_dag',
        trigger_dag_id='gold_dag',  
        wait_for_completion=False,    
        reset_dag_run=True,          
    )

    load_silver >> trigger_gold
