import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_datasets import silver_weather_dataset, silver_cdc_dataset

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# UPGRADE: Changed schedule to a list [dataset1, dataset2] which creates an AND condition.
# dbt will now wait for BOTH Silver pipelines to finish before building the Gold layer.
with DAG(
    'gold_dbt_dag',
    default_args=default_args,
    schedule=[silver_weather_dataset, silver_cdc_dataset], 
    catchup=False,
    tags=['transform', 'dbt', 'gold', 'bigquery', 'event-driven']
) as dag:

    # Explicitly grab the service account key path for dbt authentication
    gcp_key_path = os.environ.get("GCP_SA_KEYFILE", "/opt/airflow/config/gcp-key.json")

    run_dbt_pipeline = BashOperator(
        task_id='trigger_dbt_build',
        bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir . --target dev',
        env={"GOOGLE_APPLICATION_CREDENTIALS": gcp_key_path},
        append_env=True
    )