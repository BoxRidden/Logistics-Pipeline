import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_datasets import bronze_gcs_cdc_dataset, silver_cdc_dataset

gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
gcp_project = "logistics-500519" 

default_args = {'owner': 'data_engineer', 'start_date': datetime(2026, 6, 1)}

with DAG(
    'silver_cdc_dag',
    default_args=default_args,
    schedule=[bronze_gcs_cdc_dataset], 
    catchup=False,
    tags=['silver', 'spark', 'iceberg']
) as dag:

    # FIX 1: Moved 'shipments' to the end. dbt will only trigger when this list finishes.
    tables = ["hubs", "drivers", "shipments"]
    
    # Variable to help us chain the tasks sequentially 
    previous_sync = None

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
            # Only trigger dbt once the final shipments table is synced
            outlets=[silver_cdc_dataset] if table == "shipments" else []
        )

        # 1. Ensure the sync happens after its respective Spark job
        spark_task >> sync_task
        
        # FIX 2: Chain the tables sequentially (Hubs -> Drivers -> Shipments) to prevent memory crashes
        if previous_sync:
            previous_sync >> spark_task
            
        previous_sync = sync_task