import os
import requests
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather.loader import ParquetLoader
from pipeline_datasets import bronze_weather_openmeteo
from logistics.profiles import HUBS

def fetch_openmeteo():
    gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    destination = f"gs://{gcs_bucket}/bronze/weather/openmeteo/"
    
    all_weather_data = []
    
    for hub_id, name, city, lat, lon in HUBS:
        print(f"Fetching Open-Meteo data for {city}...")
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,precipitation,weather_code"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        all_weather_data.append({
            "hub_city": city,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "temperature_2m": data["current"]["temperature_2m"],
            "precipitation": data["current"]["precipitation"],
            "weather_code": data["current"]["weather_code"]
        })
        
    loader = ParquetLoader(destination_path=destination)
    loader.save_as_parquet(all_weather_data, "openmeteo_forecast")

default_args = {
    'owner': 'data_engineer', 
    'start_date': datetime(2026, 6, 1), 
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'weather_openmeteo_pipeline', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['weather', 'live-api', 'bronze']
) as dag:
    
    PythonOperator(
        task_id='fetch_openmeteo_api',
        python_callable=fetch_openmeteo,
        outlets=[bronze_weather_openmeteo] 
    )