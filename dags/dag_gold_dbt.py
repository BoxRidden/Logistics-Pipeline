from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pipeline_datasets import silver_weather_dataset, silver_cdc_dataset

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

#Schedule parameter.   
with DAG(
    'gold_dbt_dag',
    default_args=default_args,
    schedule=(silver_weather_dataset | silver_cdc_dataset), #OR condition
    catchup=False,
    tags=['transform', 'dbt', 'gold', 'bigquery']
) as dag:

    run_dbt_pipeline = BashOperator(
        task_id='trigger_dbt_build',
        bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir . --target dev'
    )
