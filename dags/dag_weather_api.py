# dags/dag_weather_api.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

# Coordinates for our 3 Hubs (Matches our database)
HUBS = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Da_Nang": {"lat": 16.0471, "lon": 108.2068},
    "HCM": {"lat": 10.8231, "lon": 106.6297}
}

# We will save the raw data here so you can see it on your local machine
RAW_DATA_DIR = "/opt/airflow/dags/data/weather/raw"

def fetch_weather_data(**kwargs):
    """Fetches current weather for all hubs and saves it as a JSON file."""
    # Create the folder if it doesn't exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    # Airflow gives us the exact time the DAG is scheduled to run
    execution_date = kwargs['ds'] 
    
    weather_records = []

    for city, coords in HUBS.items():
        # Open-Meteo is a great free API for data engineering projects
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current=temperature_2m,precipitation,rain,showers,snowfall,weather_code&timezone=Asia%2FBangkok"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Attach our city name and timestamp to the payload
            data['hub_city'] = city
            data['captured_at'] = execution_date
            weather_records.append(data)
            print(f"Successfully fetched weather for {city}")
        else:
            print(f"Failed to fetch weather for {city}. Status: {response.status_code}")

    # Save to our "Bronze" Data Lake layer (Raw JSON files)
    file_path = f"{RAW_DATA_DIR}/weather_{execution_date}.json"
    with open(file_path, "w") as f:
        json.dump(weather_records, f, indent=4)
    
    print(f"Saved raw weather data to {file_path}")

# Define the Airflow DAG
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'weather_api_ingestion',
    default_args=default_args,
    schedule_interval='@daily', # We run it daily for testing, but in production this would be hourly
    catchup=False,
    tags=['ingestion', 'api', 'bronze']
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_open_meteo',
        python_callable=fetch_weather_data,
        provide_context=True
    )