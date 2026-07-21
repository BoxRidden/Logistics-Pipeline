import os
import psycopg2
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the bridge dataset to prevent the dbt race condition
from pipeline_datasets import bronze_cdc_dataset, bronze_gcs_cdc_dataset

def extract_postgres_to_bronze_gcs():
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    
    # Connect to the local Postgres container
    pg_conn = psycopg2.connect(
        host="postgres-airflow", database="airflow", user="airflow", password="airflow"
    ) 

    # Loop through the operational tables 
    for table_name in ["shipments", "hubs", "drivers"]:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, pg_conn) 
        
        # Ensure timestamp compatibility for Parquet (Microsecond precision for Spark)
        for col in ["created_at", "updated_at", "valid_from", "valid_to"]:
            if col in df.columns:
                # Cast the Pandas datetime to prevent Spark crash
                df[col] = pd.to_datetime(df[col], errors='coerce').astype('datetime64[us]')

        # Push directly to GCS Bronze path as a Parquet file
        destination = f"gs://{gcs_bucket}/bronze/cdc/{table_name}/{table_name}.parquet"
        df.to_parquet(destination, index=False)

    pg_conn.close()

default_args = {'owner': 'data_engineering', 'start_date': datetime(2026, 6, 1)}

with DAG(
    'postgres_to_bronze_cdc', 
    default_args=default_args, 
    schedule=[bronze_cdc_dataset],  # Runs right after the simulator finishes
    catchup=False,
    tags=['ingestion', 'gcs', 'bronze']
) as dag:
    
    PythonOperator(
        task_id='extract_cdc_to_gcs',
        python_callable=extract_postgres_to_bronze_gcs,
        # Emits the signal to safely start PySpark
        outlets=[bronze_gcs_cdc_dataset] 
    )