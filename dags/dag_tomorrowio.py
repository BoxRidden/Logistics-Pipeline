from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from weather.fetcher import TomorrowIOFetcher
from weather.loader import ParquetLoader
from pipeline_datasets import bronze_weather_tomorrow # IMPORT THE SIGNAL

def fetch_and_store_bronze(**kwargs):
    fetcher = TomorrowIOFetcher(api_key="YOUR_TOMORROW_IO_KEY")
    raw_data = fetcher.get_hourly_forecast("Hanoi") 
    
    loader = ParquetLoader(destination_path="gs://your-lakehouse-bucket/bronze/weather/")
    loader.save_as_parquet(raw_data, "weather_forecast")
    # Removed XCom push; Spark will naturally read the destination folder.

default_args = {'owner': 'data_engineer', 'start_date': datetime(2026, 6, 1), 'retries': 1}

with DAG('weather_tomorrowio_pipeline', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    
    ingest_bronze_task = PythonOperator(
        task_id='fetch_weather_api',
        python_callable=fetch_and_store_bronze,
        outlets=[bronze_weather_tomorrow] # ANNOUNCES: Bronze is ready!
    )

