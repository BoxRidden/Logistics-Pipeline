import os
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from pipeline_datasets import (
    bronze_weather_openmeteo, 
    bronze_weather_openweather, 
    bronze_weather_tomorrowio,
    silver_weather_dataset
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

gcs_bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
gcp_project = os.environ.get("GCP_PROJECT", "logistics-500519")
gcp_key_path = os.environ.get("GCP_SA_KEYFILE", "/opt/airflow/config/gcp-key.json")

def validate_bronze_weather_data(**context):
    """
    Data Quality Check: Validates schema integrity, null constraints, and range 
    bounds on raw Bronze weather files before running PySpark processing.
    """
    import gcsfs
    logger.info("Executing Data Quality validation on Bronze Weather payloads...")
    
    bronze_sources = ["openmeteo", "openweather", "tomorrowio"]
    fs = gcsfs.GCSFileSystem()

    for source in bronze_sources:
        path_pattern = f"gs://{gcs_bucket}/bronze/weather/{source}/*/*/*/*/*.parquet"
        files = fs.glob(path_pattern.replace("gs://", ""))
        if not files:
            error_msg = f"No files found for validation in source '{source}'. Failing task to prevent empty downstream processing."
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        latest_file = f"gs://{max(files, key=lambda f: fs.info(f)['timeCreated'])}"
        logger.info(f"Validating latest Bronze payload for {source}: {latest_file}")
        
        df = pd.read_parquet(latest_file)

        # Data Quality Constraints
        assert "hub_city" in df.columns, f"Validation Error: 'hub_city' missing in {source}"
        assert "captured_at" in df.columns, f"Validation Error: 'captured_at' missing in {source}"
        assert df["temperature_2m"].isnull().sum() == 0, f"Validation Error: Null temperature found in {source}"
        assert df["temperature_2m"].between(-60, 60).all(), f"Validation Error: Out-of-bounds temperature in {source}"

        logger.info(f"Data Quality Check PASSED for {source} ({len(df)} records verified).")

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 1)
}

with DAG(
    'silver_weather_dag',
    default_args=default_args,
    # Triggers if ANY of the three weather source datasets update
    schedule=(bronze_weather_openmeteo | bronze_weather_openweather | bronze_weather_tomorrowio),
    catchup=False,
    tags=['silver', 'spark', 'iceberg', 'weather', 'data-quality']
) as dag:

    # Task 1: Data Quality Check Gate
    quality_check_task = PythonOperator(
        task_id='validate_bronze_weather_quality',
        python_callable=validate_bronze_weather_data
    )

    # Task 2: PySpark Silver Transformation & Iceberg Write
    spark_weather_task = BashOperator(
        task_id='spark_weather_to_iceberg',
        bash_command=(
            f'export JAVA_HOME=/usr/lib/jvm/default-java && '
            f'python /opt/airflow/dags/spark/silver_weather.py '
            f'"gs://{gcs_bucket}/bronze/weather/*/*/*/*/*.parquet" "weather_events" '
            f'--watermark "{{{{ (data_interval_start - macros.timedelta(minutes=5)).strftime(\'%Y-%m-%d %H:%M:%S\') }}}}"'
        ),
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": gcp_key_path,
            "GCP_SA_KEYFILE": gcp_key_path
        },
        append_env=True
    )

    # Task 3: BigQuery External Metadata Pointer Sync
    sync_bq_task = BashOperator(
        task_id='bq_sync_weather_events',
        bash_command=(
            f'python /opt/airflow/dags/spark/bq_sync.py '
            f'{gcp_project} logistics_raw weather_events '
            f'"gs://{gcs_bucket}/iceberg/silver/weather_events/metadata"' 
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": gcp_key_path},
        append_env=True,
        outlets=[silver_weather_dataset]
    )

    quality_check_task >> spark_weather_task >> sync_bq_task
    