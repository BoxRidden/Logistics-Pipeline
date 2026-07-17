import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline_datasets import bronze_cdc_dataset

# Import our new modular classes from the logistics folder
from logistics.simulator import ShipmentSimulator
from logistics.repository import PostgresRepository
from logistics.profiles import HUBS, DRIVERS

def generate_modular_data():
    # Connect to the local database
    repo = PostgresRepository(
        host="postgres-airflow",
        database="airflow",
        user="airflow",
        password="airflow" 
    )

    # Build tables and seed the dimensions (Hubs and Drivers)
    repo.initialize_schema(HUBS, DRIVERS)
 
    # Generate the random shipment payloads
    simulator = ShipmentSimulator()
    random_batch_size = random.randint(5, 10)
    shipments_payload = simulator.generate_payload(random_batch_size)

    # Insert the simulated shipments into Postgres
    repo.insert_shipments(shipments_payload)

    # Safely close the connection
    repo.close()
    print(f"Successfully generated and inserted {len(shipments_payload)} shipments using modular architecture.")


# --- THE AIRFLOW DAG ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 6, 1)
}

with DAG(
    'dag_shipment_sim', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['simulator', 'postgres', 'logistics']
) as dag:
    
    PythonOperator(
        task_id='generate_data',
        python_callable=generate_modular_data,
        outlets=[bronze_cdc_dataset] # Signals the ingestion DAG to run next
    )
    
   