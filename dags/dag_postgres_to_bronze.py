import os
import psycopg2
import pandas as pd
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import datasets for Data-Aware Scheduling
from pipeline_datasets import bronze_cdc_dataset, bronze_gcs_cdc_dataset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def extract_postgres_to_bronze_gcs(**context):
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    
    # Environment-driven database credentials
    db_host = os.environ.get("SOURCE_DB_HOST", "postgres-airflow")
    db_name = os.environ.get("SOURCE_DB_NAME", "airflow")
    db_user = os.environ.get("SOURCE_DB_USER", "airflow")
    db_pass = os.environ.get("SOURCE_DB_PASSWORD", "airflow")

    # 1. BATCH INCREMENTAL LOGIC (Watermarking)
    last_run = context.get('data_interval_start')
    if not last_run:
        watermark = '1970-01-01 00:00:00'
    else:
        watermark = last_run.strftime('%Y-%m-%d %H:%M:%S')

    logger.info(f"Starting Incremental CDC Extraction. Watermark threshold: > {watermark}")

    pg_conn = None
    try:
        pg_conn = psycopg2.connect(
            host=db_host, database=db_name, user=db_user, password=db_pass
        ) 

        for table_name in ["shipments", "hubs", "drivers"]:
            # 2. INCREMENTAL SQL QUERY
            if table_name == "shipments":
                query = f"SELECT * FROM {table_name} WHERE updated_at >= '{watermark}'"
            else:
                # Hubs and Drivers use valid_from for SCD Type 2 tracking
                query = f"SELECT * FROM {table_name} WHERE valid_from >= '{watermark}'"
                
            df = pd.read_sql_query(query, pg_conn) 

            # Ensure timestamp compatibility for Parquet and Apache Iceberg
            for col in ["created_at", "updated_at", "valid_from", "valid_to"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').astype('datetime64[us]')

            # 3. OPERATION TYPE INFERENCE & SCHEMA UNIFORMITY
            if table_name == "shipments":
                if not df.empty:
                    df['op_type'] = df.apply(
                        lambda row: 'I' if row['created_at'] == row['updated_at'] else 'U', 
                        axis=1
                    )
                    # Soft Delete Classification
                    df.loc[(df['op_type'] == 'U') & (df['status'] == 'Cancelled'), 'op_type'] = 'D'
                else:
                    df['op_type'] = pd.Series(dtype='str')
            else:
                df['op_type'] = 'U' if not df.empty else pd.Series(dtype='str')

            destination = f"gs://{gcs_bucket}/bronze/cdc/{table_name}/{table_name}.parquet"
            
            # Write latest delta batch (even if 0 rows) to overwrite previous delta files
            df.to_parquet(destination, index=False)
            
            if df.empty:
                logger.info(f"No new CDC events for {table_name} since watermark. Wrote empty delta file to {destination}")
            else:
                logger.info(f"Extracted {len(df)} incremental records for {table_name}. Uploaded CDC payload to {destination}")

    except Exception as e:
        logger.error(f"Incremental CDC extraction failed: {e}")
        raise e
    finally:
        # Guarantee connection cleanup
        if pg_conn:
            pg_conn.close()
            logger.info("PostgreSQL connection closed successfully.")

default_args = {'owner': 'data_engineering', 'start_date': datetime(2026, 6, 1)}

with DAG(
    'postgres_to_bronze_cdc', 
    default_args=default_args, 
    schedule=[bronze_cdc_dataset], 
    catchup=False,
    tags=['ingestion', 'gcs', 'bronze', 'incremental']
) as dag:
    
    PythonOperator(
        task_id='extract_cdc_to_gcs',
        python_callable=extract_postgres_to_bronze_gcs,
        outlets=[bronze_gcs_cdc_dataset] 
    )