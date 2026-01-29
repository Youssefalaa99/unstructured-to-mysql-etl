from datetime import datetime, timedelta
import time,json,os
import pandas as pd
# from sqlalchemy import create_engine
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configs
INPUT_FILES_DIR = '/opt/airflow/incoming'
ARCHIVE_DIR = '/opt/airflow/processed'
CONN_ID = 'mysql_default'

default_args = {
    'owner': 'Youssef',
    'depends_on_past': False,
    'email': ['yossalaa.99@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def extract_and_stage():
    mysql_hook = MySqlHook(mysql_conn_id=CONN_ID)
    engine = mysql_hook.get_sqlalchemy_engine()

    files = [f for f in os.listdir(INPUT_FILES_DIR) if f.endswith('.json')]
    records = []
    for file_name in files:
        with open(os.path.join(INPUT_FILES_DIR,file_name),'r') as f:
            data = json.load(f)
            # print(data)
            # print(json.dumps(data, indent=4))
            records.extend(data)

        # print(records)
        df = pd.json_normalize(records)
        # print(df)
        
        # Flatten logic 
        df_products = pd.json_normalize(records, record_path=['line_items'])[['product_code', 'sku', 'category', 'unit_price']].drop_duplicates()
        print(f"products:{df_products}")
        
        df_transactions = pd.json_normalize(records)[['transaction_code', 'timestamp', 'store.store_code', 'customer.customer_code', 'payment.method', 'payment.total_amount']]
        df_transactions.columns = ['transaction_code', 'created_at', 'store_code', 'customer_code', 'payment_method', 'total_amount']
        df_transactions['created_at'] = pd.to_datetime(df_transactions['created_at'])
        print(f"transactions:{df_transactions}")

        df_stores = pd.json_normalize(records)[['store.store_code','store.location','store.region']]
        df_stores.columns = ['store_code','location','region']
        print(f"stores:{df_stores}")
        
        df_customers = pd.json_normalize(records)[['customer.customer_code','customer.personal_info.full_name','customer.personal_info.email','customer.loyalty_member']]
        df_customers.columns = ['customer_code','full_name','email','loyalty_member']
        # Add first and last name
        df_customers['first_name'] = df_customers['full_name'].str.split().str[0]
        df_customers['last_name']  = df_customers['full_name'].str.split().str[1:].str.join(' ')
        df_customers = df_customers[['customer_code','first_name','last_name','email','loyalty_member']]
        print(f"customers: {df_customers}")

        df_transaction_items = pd.json_normalize(records, record_path=['line_items'],meta=['transaction_code'])[['product_code','transaction_code','quantity']]
        print(f"transaction_items:{df_transaction_items}")

        # Load to staging
        print("Inserting into stg products...")
        df_products.to_sql('stg_products', engine, if_exists='replace', schema="staging", index=False, method="multi", chunksize=1000)
        print("Inserting into stg transactions...")
        df_transactions.to_sql('stg_transactions', engine, if_exists='replace', schema="staging", index=False, method="multi", chunksize=1000)
        print("Inserting into stg stores...")
        df_stores.to_sql('stg_stores', engine, if_exists='replace', schema="staging", index=False, method="multi", chunksize=1000)
        print("Inserting into stg customers...")
        df_customers.to_sql('stg_customers', engine, if_exists='replace', schema="staging", index=False, method="multi", chunksize=1000)
        print("Inserting into stg transaction items...")
        df_transaction_items.to_sql('stg_transaction_items', engine, if_exists='replace', schema="staging", index=False, method="multi", chunksize=1000)


def load_into_target():
    pass



with DAG(
    dag_id='transactions_json_to_mysql_etl',
    default_args=default_args,
    description='Extract JSON semi-structured, process , load into MySQL',
    # schedule=timedelta(minutes=1),
    schedule='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    start_task = EmptyOperator(task_id="start_task")

    wait_for_file_task = FileSensor(
        task_id="wait_for_json_files",
        filepath="/opt/airflow/incoming/*.json",
        fs_conn_id="fs_default",
        poke_interval=5,   # check every 5s
        timeout=60*60,    # fail after 1 hour
        mode="poke"
    )

    process_files_task = PythonOperator(
        task_id='process_files', 
        python_callable=extract_and_stage
    )
    
    # load_data_task = PythonOperator(
    #     task_id='load_data', 
    #     python_callable=merge_to_production
    # )

    end_task = EmptyOperator(task_id="end_task")
    

    start_task >> wait_for_file_task >> process_files_task >> end_task