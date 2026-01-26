from datetime import datetime, timedelta
import time,json,re
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'Youssef',
    'depends_on_past': False,
    'email': ['yossalaa.99@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='transactions_json_to_mysql_etl',
    default_args=default_args,
    description='Extract JSON semi-structured, process , load into MySQL',
    schedule=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    end_task = EmptyOperator(task_id="end_task")
    

    start_task >> end_task