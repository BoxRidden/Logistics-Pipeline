import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from weather.fetcher import TomorrowIOFetcher
from weather.loader import ParquetLoader
from pipeline_datasets import bronze_weather_tomorrow

def fetch_and_store_bronze(**kwargs):
    # 1. Securely grab credentials from the Docker environment
    api_key = os.environ.get("TOMORROWIO_API_KEY")
    if not api_key:
        raise ValueError("TOMORROWIO_API_KEY is missing from environment variables.")
        
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    
    # 2. Build the production GCS path (Hive partitioned by source)
    destination = f"gs://{gcs_bucket}/bronze/weather/tomorrowio/"

    # 3. Hit the live REST API
    fetcher = TomorrowIOFetcher(api_key=api_key)
    raw_data = fetcher.get_hourly_forecast("Hanoi") 
    
    # 4. Stream directly to Google Cloud Storage
    loader = ParquetLoader(destination_path=destination)
    loader.save_as_parquet(raw_data, "weather_forecast")

# 5. Added retries and delays for network resilience 
default_args = {
    'owner': 'data_engineer', 
    'start_date': datetime(2026, 6, 1), 
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'weather_tomorrowio_pipeline', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['weather', 'live-api', 'bronze']
) as dag:
    
    ingest_bronze_task = PythonOperator(
        task_id='fetch_weather_api',
        python_callable=fetch_and_store_bronze,
        outlets=[bronze_weather_tomorrow] 
    )