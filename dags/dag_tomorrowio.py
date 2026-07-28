import os
import json
import logging
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather.fetcher import TomorrowIOFetcher
from weather.loader import ParquetLoader
from pipeline_datasets import bronze_weather_tomorrowio
from logistics.profiles import HUBS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def fetch_tomorrowio_pipeline():
    api_key = os.environ.get("TOMORROWIO_API_KEY")
    if not api_key or api_key == "your_key_here":
        logger.error("Valid TOMORROWIO_API_KEY is missing from environment variables.")
        raise ValueError("Valid TOMORROWIO_API_KEY is missing from environment variables.")
        
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    destination = f"gs://{gcs_bucket}/bronze/weather/tomorrowio/"
    
    fetcher = TomorrowIOFetcher(api_key=api_key)
    all_weather_data = []

    for hub_id, name, city, lat, lon in HUBS:
        logger.info(f"Fetching Tomorrow.io weather data for {city}...")
        location_query = f"{lat},{lon}"
        try:
            payload = fetcher.get_hourly_forecast(location_query)
            for item in payload:
                item["hub_city"] = city
            all_weather_data.extend(payload)
        except Exception as e:
            logger.error(f"Failed to fetch Tomorrow.io data for {city}: {e}")
            raise e

    loader = ParquetLoader(destination_path=destination)
    loader.save_as_parquet(all_weather_data, "tomorrowio_forecast")

default_args = {
    'owner': 'data_engineer', 
    'start_date': datetime(2026, 6, 1), 
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True
}

with DAG(
    'weather_tomorrowio_pipeline', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['weather', 'live-api', 'bronze']
) as dag:
    
    PythonOperator(
        task_id='fetch_tomorrowio_api',
        python_callable=fetch_tomorrowio_pipeline,
        outlets=[bronze_weather_tomorrowio] 
    )