from airflow.datasets import Dataset

# ==========================================
# BRONZE LAYER (Raw Ingestion Signals)
# ==========================================
# These are triggered when your extraction DAGs finish pulling raw data.

# 1. Logistics / CDC Data
bronze_cdc_dataset = Dataset("ds://bronze/cdc/shipments")

# 2. Weather APIs
bronze_weather_tomorrow = Dataset("ds://bronze/weather/tomorrowio")
bronze_weather_openmeteo = Dataset("ds://bronze/weather/openmeteo")
bronze_weather_openweather = Dataset("ds://bronze/weather/openweather")


# ==========================================
# SILVER LAYER (Iceberg Processing Signals)
# ==========================================
# These are triggered when PySpark finishes merging Bronze data into Iceberg.

silver_cdc_dataset = Dataset("ds://silver/cdc")
silver_weather_dataset = Dataset("ds://silver/weather")