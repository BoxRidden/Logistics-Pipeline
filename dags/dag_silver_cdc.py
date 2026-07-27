import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_datasets import bronze_gcs_cdc_dataset, silver_cdc_dataset

# Environment-driven configurations
gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
gcp_project = os.environ.get("GCP_PROJECT", "logistics-500519") 
gcp_key_path = os.environ.get("GCP_SA_KEYFILE", "/opt/airflow/config/gcp-key.json")

default_args = {
    'owner': 'data_engineer', 
    'start_date': datetime(2026, 6, 1)
}

with DAG(
    'silver_cdc_dag',
    default_args=default_args,
    schedule=[bronze_gcs_cdc_dataset], 
    catchup=False,
    tags=['silver', 'spark', 'iceberg', 'incremental']
) as dag:

    tables = ["hubs", "drivers", "shipments"]
    
    # Variable to help us chain the tasks sequentially 
    previous_sync = None

    for table in tables:
        
        # UPGRADE: Explicitly pass GCP credentials to the Spark execution environment
        spark_task = BashOperator(
            task_id=f'spark_cdc_{table}_to_iceberg',
            bash_command=(
                f'export JAVA_HOME=/usr/lib/jvm/default-java && '
                f'python /opt/airflow/dags/spark/silver_cdc.py '
                f'"gs://{gcs_bucket}/bronze/cdc/{table}/{table}.parquet" "{table}"'
            ),
            env={
                "GOOGLE_APPLICATION_CREDENTIALS": gcp_key_path,
                "GCP_SA_KEYFILE": gcp_key_path
            },
            append_env=True
        )

        sync_task = BashOperator(
            task_id=f'bq_sync_{table}',
            bash_command=( 
                f'python /opt/airflow/dags/spark/bq_sync.py '
                f'{gcp_project} logistics_raw {table} '
                f'"gs://{gcs_bucket}/iceberg/silver/{table}/metadata"'
            ),
            env={"GOOGLE_APPLICATION_CREDENTIALS": gcp_key_path},
            append_env=True,
            # Only trigger dbt once the final shipments table is synced
            outlets=[silver_cdc_dataset] if table == "shipments" else []
        )

        # Ensure the sync happens after its respective Spark job
        spark_task >> sync_task
        
        # Chain the tables sequentially (Hubs -> Drivers -> Shipments) to prevent memory crashes
        if previous_sync:
            previous_sync >> spark_task
            
        previous_sync = sync_task 