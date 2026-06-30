from airflow.datasets import Dataset

# These act as signals. When one DAG updates these, it triggers the next DAG automatically.
weather_dataset = Dataset("file://weather_api_raw.json")
postgres_dataset = Dataset("database://postgres/logistics_db")