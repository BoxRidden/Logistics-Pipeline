import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Import BOTH the trigger (Bronze) and the emitter (Silver)
from pipeline_datasets import bronze_weather_tomorrow, silver_weather_dataset

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
    
    # Replaced schedule_interval='@hourly' with the Dataset!
    # This DAG only runs when Tomorrow.io successfully finishes downloading.
    schedule=[bronze_weather_tomorrow], 
    
    catchup=False,
    tags=['silver', 'spark', 'weather', 'event-driven']
) as dag:
    
    # Pass the dynamic GCS bucket path directly into the PySpark script
    BashOperator(
        task_id='spark_weather_to_iceberg',
        bash_command=(
            f'export JAVA_HOME=/usr/lib/jvm/default-java && '
            f'python /opt/airflow/dags/spark/silver_weather.py '
            f'"gs://{gcs_bucket}/bronze/weather/tomorrowio/*.parquet" "weather_iceberg"'
        ),
        outlets=[silver_weather_dataset] # Broadcasts to the Gold (dbt) layer
    )
