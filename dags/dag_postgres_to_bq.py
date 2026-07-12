"""
DAG: logistics_postgres_to_bq
Description: Extracts operational CDC data from PostgreSQL and pushes to BigQuery.
             Implements explicit BigQuery schema definitions to guarantee type safety
             for Slowly Changing Dimension (SCD) timestamp columns.
"""

import psycopg2
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery

from pipeline_datasets import bronze_cdc_dataset, silver_cdc_dataset, silver_weather_dataset

# =============================================================================
# EXPLICIT SCHEMA DEFINITIONS
# Reviewer Note: Hardcoding the schemas prevents BigQuery from inferring 
# completely NULL columns (like valid_to) as INT64/FLOAT64.
# =============================================================================
BQ_SCHEMAS = {
    "hubs": [
        bigquery.SchemaField("hub_id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lon", "FLOAT"),
        bigquery.SchemaField("valid_from", "TIMESTAMP"),
        bigquery.SchemaField("valid_to", "TIMESTAMP"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
    ],
    "drivers": [
        bigquery.SchemaField("driver_id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("vehicle_type", "STRING"),
        bigquery.SchemaField("valid_from", "TIMESTAMP"),
        bigquery.SchemaField("valid_to", "TIMESTAMP"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
    ],
    "shipments": [
        bigquery.SchemaField("shipment_id", "INTEGER"),
        bigquery.SchemaField("tracking_code", "STRING"),
        bigquery.SchemaField("hub_id", "INTEGER"),
        bigquery.SchemaField("driver_id", "INTEGER"),
        bigquery.SchemaField("customer_city", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("revenue", "FLOAT"),
        bigquery.SchemaField("item_quantity", "INTEGER"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("order_type", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]
}

def extract_load_postgres_to_bq():
    key_path = "/opt/airflow/config/gcp-key.json"
    project_id = "logistics-500519"
    dataset_id = "logistics_raw"
    
    credentials = service_account.Credentials.from_service_account_file(key_path)
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    pg_conn = psycopg2.connect(
        host="postgres-airflow", database="airflow", user="airflow", password="airflow"
    ) 

    for table_name in ["shipments", "hubs", "drivers"]:
        table_dest = f"{project_id}.{dataset_id}.{table_name}"
        
        # Drop table to purge the corrupted INT64 schema completely
        bq_client.delete_table(table_dest, not_found_ok=True)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=BQ_SCHEMAS[table_name]  # Enforce the strict schema mapping
        )
        
        query = f"SELECT * FROM {table_name}"
        
        for i, chunk in enumerate(pd.read_sql_query(query, pg_conn, chunksize=10000)):
            # Convert temporal columns to proper datetime objects for the client
            for col in ["created_at", "updated_at", "valid_from", "valid_to"]:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

            if i > 0:
                job_config.write_disposition = "WRITE_APPEND"
                
            job = bq_client.load_table_from_dataframe(chunk, table_dest, job_config=job_config)
            job.result()

    pg_conn.close()

default_args = {
    'owner': 'data_engineering', 
    'start_date': datetime(2026, 6, 1)
}

with DAG(
    'logistics_postgres_to_bq', 
    default_args=default_args, 
    schedule=[bronze_cdc_dataset], 
    catchup=False,
    tags=['ingestion', 'bigquery', 'pandas']
) as dag:
    
    PythonOperator(
        task_id='push_to_bigquery',
        python_callable=extract_load_postgres_to_bq,
        outlets=[silver_cdc_dataset, silver_weather_dataset] 
    )