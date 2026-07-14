from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from pipeline_datasets import bronze_cdc_dataset, silver_cdc_dataset

default_args = {'owner': 'data_engineer', 'start_date': datetime(2026, 6, 1)}

# Schedule parameter
with DAG(
    'silver_cdc_dag',
    default_args=default_args,
    schedule=[bronze_cdc_dataset], 
    catchup=False,
    tags=['silver', 'spark', 'iceberg']
) as dag:

    process_cdc_silver = BashOperator(
        task_id='spark_cdc_to_iceberg',
        bash_command='export JAVA_HOME=/usr/lib/jvm/default-java && python /opt/airflow/dags/spark/silver_cdc.py "gs://your-lakehouse-bucket/bronze/cdc/shipments/" "shipments_iceberg"',
        outlets=[silver_cdc_dataset]
    )