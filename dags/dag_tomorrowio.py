import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather.fetcher import TomorrowIOFetcher
from weather.loader import ParquetLoader
from pipeline_datasets import bronze_weather_tomorrow
from logistics.profiles import CITIES  # Dynamically import your hubs

def fetch_and_store_bronze(**kwargs):
    api_key = os.environ.get("TOMORROWIO_API_KEY")
    if not api_key:
        raise ValueError("TOMORROWIO_API_KEY is missing from environment variables.")
        
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    destination = f"gs://{gcs_bucket}/bronze/weather/tomorrowio/" 

    fetcher = TomorrowIOFetcher(api_key=api_key)
    all_weather_data = []

    # Iterate dynamically through every hub city in your network
    for city in CITIES:
        try:
            print(f"Fetching data for {city}...")
            city_data = fetcher.get_hourly_forecast(city)
            all_weather_data.extend(city_data)
        except Exception as e:
            print(f"Failed to fetch weather for {city}: {e}")
            # Continue the loop even if one city's API call drops
            
    # Save the combined payload to a single Bronze Parquet file
    loader = ParquetLoader(destination_path=destination)
    loader.save_as_parquet(all_weather_data, "weather_forecast")

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