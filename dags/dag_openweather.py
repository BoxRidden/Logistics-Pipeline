import os
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather.loader import ParquetLoader
from pipeline_datasets import bronze_weather_openweather
from logistics.profiles import HUBS

logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def fetch_openweather():
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key or api_key == "your_key_here":
        logger.error("Valid OPENWEATHER_API_KEY is missing from environment variables.")
        raise ValueError("Valid OPENWEATHER_API_KEY is missing from environment variables.")
        
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    destination = f"gs://{gcs_bucket}/bronze/weather/openweather/"
    
    all_weather_data = [] 
    
    for hub_id, name, city, lat, lon in HUBS:
        logger.info(f"Fetching OpenWeather data for {city}...")
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # OpenWeather returns rain volume in a nested dictionary (example 'rain': {'1h': 2.5})
            precipitation = data.get("rain", {}).get("1h", 0.0)
            
            # Format the live data while preserving the raw API response for Bronze standards
            all_weather_data.append({
                "hub_city": city,
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "temperature_2m": data.get("main", {}).get("temp", 0.0),
                "precipitation": precipitation,
                "weather_code": data.get("weather", [{}])[0].get("id", 0),
                "raw_json": json.dumps(data, ensure_ascii=False)
            })
            logger.info(f"Successfully fetched weather payload for {city}.")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {city}: {e}")
            raise e
        
    loader = ParquetLoader(destination_path=destination)
    loader.save_as_parquet(all_weather_data, "openweather_forecast")

default_args = {
    'owner': 'data_engineer', 
    'start_date': datetime(2026, 6, 1), 
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True
}

with DAG(
    'weather_openweather_pipeline', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['weather', 'live-api', 'bronze']
) as dag:
    
    PythonOperator(
        task_id='fetch_openweather_api',
        python_callable=fetch_openweather,
        outlets=[bronze_weather_openweather] 
    )