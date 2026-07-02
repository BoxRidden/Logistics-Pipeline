from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from pipeline_datasets import bronze_weather_tomorrow, silver_weather_dataset

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 1)  
}

with DAG(
    'silver_weather_dag', 
    default_args=default_args,
    schedule=[bronze_weather_tomorrow], 
    catchup=False,
    tags=['silver', 'spark', 'weather']
) as dag:
    
    BashOperator(
        task_id='spark_weather_to_iceberg',
        bash_command='export JAVA_HOME=/usr/lib/jvm/default-java && python /opt/airflow/dags/spark/silver_weather.py "gs://your-lakehouse-bucket/bronze/weather/weather_forecast_latest.parquet" "weather_iceberg"',
        outlets=[silver_weather_dataset] 
    )
