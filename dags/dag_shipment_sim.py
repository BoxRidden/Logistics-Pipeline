import os
import random
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline_datasets import bronze_cdc_dataset

# Import new modular classes from the logistics folder
from logistics.simulator import ShipmentSimulator
from logistics.repository import PostgresRepository
from logistics.profiles import HUBS, DRIVERS
from logistics.kafka_producer import LogisticsKafkaProducer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def generate_modular_data():
    # Environment-driven credentials
    db_host = os.environ.get("SOURCE_DB_HOST", "postgres-airflow")
    db_name = os.environ.get("SOURCE_DB_NAME", "airflow")
    db_user = os.environ.get("SOURCE_DB_USER", "airflow")
    db_pass = os.environ.get("SOURCE_DB_PASSWORD", "airflow")
    
    # Airflow runs inside Docker, so it uses the internal Kafka port
    kafka_broker = os.environ.get("KAFKA_BROKER_URL", "kafka:29092") 

    repo = None
    try:
        # Connect to the local database 
        repo = PostgresRepository(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_pass 
        )

        # Build tables and seed the dimensions (Hubs and Drivers)
        repo.initialize_schema(HUBS, DRIVERS)
     
        # Generate the random shipment payloads
        # Extract just the IDs from the tuples to pass dynamically
        hub_ids = [h[0] for h in HUBS]
        driver_ids = [d[0] for d in DRIVERS]
        
        simulator = ShipmentSimulator(hubs=hub_ids, drivers=driver_ids)
        random_batch_size = random.randint(5, 10)
        shipments_payload = simulator.generate_payload(random_batch_size)

        # Insert the simulated shipments into Postgres 
        repo.insert_shipments(shipments_payload)
        repo.advance_shipment_status()
        logger.info(f"Successfully generated and inserted {len(shipments_payload)} shipments into PostgreSQL.")

        # Real time Kafka streaming
        logger.info(f"Initializing Kafka Producer at {kafka_broker}...")
        kafka_client = LogisticsKafkaProducer(broker_url=kafka_broker)
        
        logger.info("Streaming shipment events to Kafka topic 'logistics_shipments'...")
        for shipment in shipments_payload:
            kafka_client.publish_shipment_event(
                topic='logistics_shipments', 
                shipment_data=shipment
            )
        
        # Ensure all messages leave the Python buffer and hit the Kafka server
        kafka_client.flush()
        logger.info("Kafka streaming complete.")

    except Exception as e:
        logger.error(f"Simulator DAG failed during execution: {e}")
        raise e
    finally:
        # Safely close the connection to prevent pooling exhaustion
        if repo:
            repo.close()
            logger.info("PostgreSQL connection closed.")


# --- THE AIRFLOW DAG --- 
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 1)
}

with DAG(
    'dag_shipment_sim', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False,
    tags=['simulator', 'postgres', 'logistics', 'kafka', 'streaming']
) as dag:
    
    PythonOperator(
        task_id='generate_data', 
        python_callable=generate_modular_data,
        outlets=[bronze_cdc_dataset] # Signals the ingestion DAG to run next
    )