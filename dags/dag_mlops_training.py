from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from pipeline_datasets import gold_dbt_dataset

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 7, 26),
}

with DAG(
    'mlops_model_training',
    default_args=default_args,
    schedule=[gold_dbt_dataset], 
    catchup=False,
    tags=['mlops', 'mlflow']
) as dag:

    train_delay = BashOperator(
        task_id='train_delay_prediction',
        bash_command='python /opt/airflow/dags/mlops/train_delay_model.py'
    )

    train_anomaly = BashOperator(
        task_id='train_anomaly_detection',
        bash_command='python /opt/airflow/dags/mlops/train_anomaly_model.py'
    )

    run_batch_scoring = BashOperator(
        task_id='run_batch_scoring',
        # Note: If you saved batch_scoring.py in the mlops folder, 
        # change this path to /opt/airflow/dags/mlops/batch_scoring.py
        bash_command='python /opt/airflow/dags/batch_scoring.py' 
    )

    # This line tells Airflow: "Run the two training tasks first, and when both succeed, run the scoring task"
    [train_delay, train_anomaly] >> run_batch_scoring
