from datetime import datetime, timedelta
import time,json,os,shutil
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
MYSQL_HOOK = MySqlHook(mysql_conn_id=CONN_ID)
ENGINE = MYSQL_HOOK.get_sqlalchemy_engine()
STG_DB="staging"
TRGT_DB="mydb"


default_args = {
    'owner': 'Youssef',
    'depends_on_past': False,
    'email': ['yossalaa.99@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def extract_and_stage():
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
        
        df_transactions = pd.json_normalize(records)[['transaction_code', 'timestamp', 'store.store_code', 'customer.customer_code', 'payment.method', 'payment.total_amount','payment.tax']]
        df_transactions.columns = ['transaction_code', 'created_at', 'store_code', 'customer_code', 'payment_method', 'total_amount','tax']
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
        df_products.to_sql('stg_products', ENGINE, if_exists='replace', schema=STG_DB, index=False, method="multi", chunksize=1000)
        print("Inserting into stg transactions...")
        df_transactions.to_sql('stg_transactions', ENGINE, if_exists='replace', schema=STG_DB, index=False, method="multi", chunksize=1000)
        print("Inserting into stg stores...")
        df_stores.to_sql('stg_stores', ENGINE, if_exists='replace', schema=STG_DB, index=False, method="multi", chunksize=1000)
        print("Inserting into stg customers...")
        df_customers.to_sql('stg_customers', ENGINE, if_exists='replace', schema=STG_DB, index=False, method="multi", chunksize=1000)
        print("Inserting into stg transaction items...")
        df_transaction_items.to_sql('stg_transaction_items', ENGINE, if_exists='replace', schema=STG_DB, index=False, method="multi", chunksize=1000)


def load_tables():
    # Products
    upsert_products = f"""
        INSERT INTO {TRGT_DB}.products (product_code, sku, category, price)
        SELECT product_code, sku, category, unit_price FROM {STG_DB}.stg_products
        ON DUPLICATE KEY UPDATE 
            sku = VALUES(sku),
            category = VALUES(category), 
            price = unit_price;
    """

    # Customers
    upsert_customers = f"""
        INSERT INTO {TRGT_DB}.customers (customer_code,first_name,last_name,email,loyalty_member)
        SELECT customer_code,first_name,last_name,email,loyalty_member FROM {STG_DB}.stg_customers
        ON DUPLICATE KEY UPDATE 
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            email = VALUES(email),
            loyalty_member = VALUES(loyalty_member);
    """

    # Stores
    upsert_stores = f"""
        INSERT INTO {TRGT_DB}.stores (store_code,location,region)
        SELECT store_code,location,region FROM {STG_DB}.stg_stores
        ON DUPLICATE KEY UPDATE 
            location = VALUES(location),
            region = VALUES(region);
        """
    
    # Transactions (Insert Ignore - we don't update historical facts)
    insert_transactions = f"""
        INSERT IGNORE INTO {TRGT_DB}.transactions (transaction_code, created_at, store_id, customer_id, payment_method, total_amount, tax)
        SELECT st.transaction_code, st.created_at, s.store_id, c.customer_id, st.payment_method, st.total_amount, st.tax 
        FROM {STG_DB}.stg_transactions st
        JOIN {TRGT_DB}.stores s on st.store_code=s.store_code
        JOIN {TRGT_DB}.customers c on st.customer_code=c.customer_code;
    """

    # Transction Items
    insert_transaction_items = f"""
        INSERT IGNORE INTO {TRGT_DB}.transaction_items (transaction_id, product_id, quantity)
        SELECT t.transaction_id, p.product_id, sti.quantity
        FROM {STG_DB}.stg_transaction_items sti
        JOIN {TRGT_DB}.transactions t ON sti.transaction_code=t.transaction_code
        JOIN {TRGT_DB}.products p ON sti.product_code=p.product_code;
    """

    # Order matter (transactions, transaction_items to be last)
    sql_queries = {"customers":upsert_customers, "products":upsert_products, "stores":upsert_stores, "transactions":insert_transactions, "transaction_items":insert_transaction_items}

    with MYSQL_HOOK.get_conn() as conn:
        with conn.cursor() as cur:
            for table,query in sql_queries.items():
                print(f"Inserting into trgt {table}..")
                cur.execute(query)
                conn.commit()
                print("Done!")

    # Move files to archive
    for file_name in os.listdir(INPUT_FILES_DIR):
        if file_name.endswith('.json'):
            # os.rename(os.path.join(INPUT_FILES_DIR, file_name), os.path.join(ARCHIVE_DIR, file_name))
            src = os.path.join(INPUT_FILES_DIR, file_name)
            dst = os.path.join(ARCHIVE_DIR, file_name)
            shutil.move(src, dst)



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

    load_tables_task = PythonOperator(
        task_id='load_tables', 
        python_callable=load_tables
    )

    end_task = EmptyOperator(task_id="end_task")
    

    start_task >> wait_for_file_task >> process_files_task >> load_tables_task >> end_task