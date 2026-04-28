from datetime import datetime
import os
import sys
import pendulum
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from src.main import run_etl

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="oracle_excel_etl",
    start_date=datetime(2026, 4, 20, tzinfo=local_tz),
    schedule="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["oracle", "etl", "excel"],
) as dag:
    run_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
    run_etl