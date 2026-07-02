import pandas as pd
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
from pipeline_datasets import bronze_cdc_dataset

def extract_load_postgres_to_bq():
    key_path = "/opt/airflow/config/gcp-key.json"
    project_id = "logistics-500519"
    dataset_id = "logistics_raw"
    
    # 1. Connect to GCP
    credentials = service_account.Credentials.from_service_account_file(key_path)
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    
    # 2. Create the dataset if it doesn't exist 
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"Created new dataset: {dataset_id}")

    # 3. Connect to Local Postgres
    pg_conn = psycopg2.connect(
        host="postgres-airflow", database="airflow", user="airflow", password="airflow"
    ) 

    # 4. Move the tables
    for table in ["shipments", "hubs", "drivers"]:
        df = pd.read_sql_query(f"SELECT * FROM {table}", pg_conn)
        table_dest = f"{project_id}.{dataset_id}.{table}"
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = bq_client.load_table_from_dataframe(df, table_dest, job_config=job_config)
        job.result()
        print(f"Pushed {len(df)} rows from {table} to BigQuery.")

    pg_conn.close()

default_args = {'owner': 'airflow', 'start_date': datetime(2026, 6, 1)}

with DAG('logistics_postgres_to_bq', default_args=default_args, schedule=[bronze_cdc_dataset], catchup=False) as dag:
    PythonOperator(
        task_id='push_to_bigquery',
        python_callable=extract_load_postgres_to_bq
    )