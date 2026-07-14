"""
DAG: logistics_postgres_to_bq
Description: Extracts operational CDC data from PostgreSQL and pushes to BigQuery.
             Implements explicit BigQuery schema definitions and a Staging-to-MERGE 
             Upsert architecture to protect downstream Materialized Views.
"""

import psycopg2
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from pipeline_datasets import bronze_cdc_dataset, silver_cdc_dataset, silver_weather_dataset


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

# Define Primary Keys for the MERGE logic
# SCD tables use a composite key (ID + valid_from) to prevent duplicate matches
PK_MAP = {
    "shipments": ["shipment_id"],
    "hubs": ["hub_id", "valid_from"],
    "drivers": ["driver_id", "valid_from"]
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
    except NotFound:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    pg_conn = psycopg2.connect(
        host="postgres-airflow", database="airflow", user="airflow", password="airflow"
    ) 

    for table_name in ["shipments", "hubs", "drivers"]:
        main_table_id = f"{project_id}.{dataset_id}.{table_name}"
        staging_table_id = f"{project_id}.{dataset_id}.{table_name}_staging"
        
        # Ensure main Table exists 
        try:
            bq_client.get_table(main_table_id)
        except NotFound:
            table = bigquery.Table(main_table_id, schema=BQ_SCHEMAS[table_name])
            bq_client.create_table(table)
        
        # Load Postgres Data into Staging Table
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # Always overwrite staging table safely
            schema=BQ_SCHEMAS[table_name]  
        )
        
        query = f"SELECT * FROM {table_name}"
        
        for i, chunk in enumerate(pd.read_sql_query(query, pg_conn, chunksize=10000)):
            for col in ["created_at", "updated_at", "valid_from", "valid_to"]:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

            if i > 0:
                job_config.write_disposition = "WRITE_APPEND"
                
            job = bq_client.load_table_from_dataframe(chunk, staging_table_id, job_config=job_config)
            job.result()

        # Dynamic MERGE (Upsert) from Staging to Main
        columns = [field.name for field in BQ_SCHEMAS[table_name]]
        pks = PK_MAP[table_name]
        
        join_conditions = " AND ".join([f"T.{pk} = S.{pk}" for pk in pks])
        update_set = ", ".join([f"T.{col} = S.{col}" for col in columns if col not in pks])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"S.{col}" for col in columns])
        
        merge_query = f"""
        MERGE `{main_table_id}` T
        USING `{staging_table_id}` S
        ON {join_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """
        
        merge_job = bq_client.query(merge_query)
        merge_job.result()
        
        # Clean up Staging Table
        bq_client.delete_table(staging_table_id, not_found_ok=True)

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