import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# UPDATE 1: Import ALL triggers (Bronze) and the emitter (Silver)
from pipeline_datasets import (
    bronze_weather_tomorrow,
    bronze_weather_openmeteo,
    bronze_weather_openweather,
    silver_weather_dataset
)

gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'silver_weather_dag', 
    default_args=default_args,
    
    # UPDATE 2: Use the pipe (|) for an OR condition. 
    # This DAG will run if ANY of the three APIs successfully finish downloading.
    schedule=(bronze_weather_tomorrow | bronze_weather_openmeteo | bronze_weather_openweather), 
    
    catchup=False,
    tags=['silver', 'spark', 'weather', 'event-driven']
) as dag:
    
    BashOperator(
        task_id='spark_weather_to_iceberg',
        bash_command=(
            f'export JAVA_HOME=/usr/lib/jvm/default-java && '
            f'python /opt/airflow/dags/spark/silver_weather.py '
            # UPDATE 3: Use a wildcard (*/*.parquet) to grab from tomorrowio, openmeteo, and openweather
            f'"gs://{gcs_bucket}/bronze/weather/*/*.parquet" "weather_iceberg"'
        ),
        outlets=[silver_weather_dataset] # Broadcasts to the Gold (dbt) layer
    )