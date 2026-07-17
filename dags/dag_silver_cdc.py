import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. FIX: Listen for the bridge signal, output the final silver signal
from pipeline_datasets import bronze_gcs_cdc_dataset, silver_cdc_dataset

gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
gcp_project = "logistics-500519" 

default_args = {'owner': 'data_engineer', 'start_date': datetime(2026, 6, 1)}

with DAG(
    'silver_cdc_dag',
    default_args=default_args,
    
    # 2. FIX: Will not run until the Pandas extraction DAG finishes 
    schedule=[bronze_gcs_cdc_dataset], 
    
    catchup=False,
    tags=['silver', 'spark', 'iceberg']
) as dag:

    tables = ["shipments", "hubs", "drivers"]

    for table in tables:
        spark_task = BashOperator(
            task_id=f'spark_cdc_{table}_to_iceberg',
            bash_command=(
                f'export JAVA_HOME=/usr/lib/jvm/default-java && '
                f'python /opt/airflow/dags/spark/silver_cdc.py '
                f'"gs://{gcs_bucket}/bronze/cdc/{table}/{table}.parquet" "{table}"'
            )
        )

        sync_task = BashOperator(
            task_id=f'bq_sync_{table}',
            bash_command=( 
                f'python /opt/airflow/dags/spark/bq_sync.py '
                f'{gcp_project} logistics_raw {table} '
                f'"gs://{gcs_bucket}/iceberg/silver/{table}/metadata"'
            ),
            # Only trigger dbt once the primary shipments table is safely synced to BQ
            outlets=[silver_cdc_dataset] if table == "shipments" else []
        )

        spark_task >> sync_task