from airflow.datasets import Dataset
import os

GCS_BUCKET = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")

# BRONZE LAYER (Raw ingestion signals)
bronze_cdc_dataset = Dataset(f"gs://{GCS_BUCKET}/bronze/cdc/shipments")
bronze_gcs_cdc_dataset = Dataset(f"gs://{GCS_BUCKET}/bronze/cdc/gcs_parquet") 
bronze_weather_tomorrow = Dataset(f"gs://{GCS_BUCKET}/bronze/weather/tomorrowio")
bronze_weather_openmeteo = Dataset(f"gs://{GCS_BUCKET}/bronze/weather/openmeteo")
bronze_weather_openweather = Dataset(f"gs://{GCS_BUCKET}/bronze/weather/openweather")

# SILVER LAYER (Iceberg processing signals)
silver_cdc_dataset = Dataset(f"gs://{GCS_BUCKET}/silver/cdc")
silver_weather_dataset = Dataset(f"gs://{GCS_BUCKET}/silver/weather")

