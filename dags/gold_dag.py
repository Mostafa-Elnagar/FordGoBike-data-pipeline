from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

import os

from datetime import timedelta
import sys 

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("dags","")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from include.modules.email_sender.sender import task_failure_alert, email_dags_overview


with DAG(
    dag_id="gold_dag",
    description="Gold ETL DAG",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=1),
    },
    start_date=None,
    end_date=None,
    schedule=None,
    catchup=False,
    tags=["gold"],
    template_searchpath=[os.path.join(PROJECT_ROOT, "include/sql/gold")],
) as dag:
    
    refresh_daily_dm = SQLExecuteQueryOperator(
        task_id="refresh_daily_dm",
        sql="CALL gold.refresh_dm_daily_trip_summary()",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )
    refresh_stations_dm = SQLExecuteQueryOperator(
        task_id="refresh_stations_dm",
        sql="CALL gold.refresh_dm_station_popularity()",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )

    refresh_routes_dm = SQLExecuteQueryOperator(
        task_id="refresh_routes_dm",
        sql="CALL gold.refresh_dm_popular_routes()",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )

    refresh_users_dm = SQLExecuteQueryOperator(
        task_id="refresh_users_dm",
        sql="CALL gold.refresh_dm_user_behavior_summary()",
        conn_id="postgres_conn_id",
        autocommit=True,
        on_failure_callback=task_failure_alert,
    )
    send_email_dags_overview = PythonOperator(
        task_id="send_email_dags_overview",
        python_callable=email_dags_overview,
    )

    [refresh_daily_dm, refresh_stations_dm, refresh_routes_dm, refresh_users_dm] >> send_email_dags_overview