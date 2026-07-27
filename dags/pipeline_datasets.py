from airflow.datasets import Dataset

# We use a custom 'logistics://' scheme to bypass strict Airflow provider parsers
# while maintaining perfect Data-Aware Scheduling triggers.

# Operational CDC Datasets
bronze_cdc_dataset = Dataset("logistics://postgres/cdc_extract")
bronze_gcs_cdc_dataset = Dataset("logistics://gcs/bronze_cdc")
silver_cdc_dataset = Dataset("logistics://iceberg/silver_cdc")

# Weather API Bronze Datasets
bronze_weather_openmeteo = Dataset("logistics://gcs/bronze_weather_openmeteo")
bronze_weather_openweather = Dataset("logistics://gcs/bronze_weather_openweather")
bronze_weather_tomorrowio = Dataset("logistics://gcs/bronze_weather_tomorrowio")

# Silver Weather Dataset
silver_weather_dataset = Dataset("logistics://iceberg/silver_weather")