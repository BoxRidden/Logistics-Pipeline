# dags/dag_gold_dbt.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'dbt_gold_transformation',
    default_args=default_args,
    schedule_interval='@daily',  # Runs automatically every day, or can be triggered manually
    catchup=False,
    tags=['transform', 'dbt', 'gold', 'bigquery']
) as dag:

    # Executes dbt build using the profiles.yml file sitting inside the same directory
    run_dbt_pipeline = BashOperator(
        task_id='trigger_dbt_build',
        bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir . --target dev'
    )
