import pandas as pd
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
from pipeline_datasets import weather_dataset

def fetch_and_push_weather():
    # 1. Setup BigQuery Connection
    key_path = "/opt/airflow/config/gcp-key.json"
    project_id = "logistics-500519"
    dataset_id = "logistics_raw"
    table_id = "weather_api_raw"
    
    credentials = service_account.Credentials.from_service_account_file(key_path)
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    
    # 2. Fetch Live Weather from Open-Meteo
    cities = {
        "Hanoi": {"lat": 21.0285, "lon": 105.8542},
        "Da Nang": {"lat": 16.0471, "lon": 108.2068},
        "Ho Chi Minh City": {"lat": 10.8231, "lon": 106.6297}
    }
    
    weather_data = []
    for city, coords in cities.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        response = requests.get(url).json()
        
        weather_data.append({
            "hub_city": city,
            "captured_at": datetime.now().isoformat(),
            "temperature_2m": response["current_weather"]["temperature"],
            "precipitation": 0.0, # Using 0.0 as default if current_weather lacks it
            "weather_code": response["current_weather"]["weathercode"]
        })
        
    df = pd.DataFrame(weather_data)
    
    # 3. Push to BigQuery
    table_dest = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, table_dest, job_config=job_config)
    job.result()
    print(f"Successfully pushed live weather for {len(df)} cities to BigQuery.")

# --- THE AIRFLOW DAG ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 6, 1),
}

with DAG(
    'weather_api_to_bq', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['ingestion', 'api', 'weather']
) as dag:
    
    PythonOperator(
        task_id='fetch_push_weather',
        python_callable=fetch_and_push_weather,
        outlets=[weather_dataset] # This signals dbt to run after the weather arrives!
    )