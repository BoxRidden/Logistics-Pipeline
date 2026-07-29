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
     
        # Extract just the IDs from the tuples to pass dynamically
        hub_ids = [h[0] for h in HUBS]
        driver_ids = [d[0] for d in DRIVERS]
        
        simulator = ShipmentSimulator(hubs=hub_ids, drivers=driver_ids)
        
        # --- 1. GENERATE NEW ORDERS ---
        random_batch_size = random.randint(5, 10)
        new_shipments = simulator.generate_new_orders(random_batch_size)

        # Insert the NEW simulated shipments into Postgres 
        repo.insert_shipments(new_shipments)
        logger.info(f"Successfully inserted {len(new_shipments)} NEW shipments into PostgreSQL.")

        # --- 2. TRANSITION EXISTING ORDERS ---
        # Fetch active orders directly using the repo's database connection
        cursor = repo.conn.cursor()
        cursor.execute("SELECT * FROM shipments WHERE status IN ('Pending', 'In Transit', 'Delayed')")
        
        # Map the returned SQL tuples into Python dictionaries
        columns = [col[0] for col in cursor.description]
        active_orders = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Pass dictionaries through our Python State Machine
        updated_shipments = simulator.transition_existing_orders(active_orders)
        
        # Update the transitioned orders back in the database
        for order in updated_shipments:
            cursor.execute(
                "UPDATE shipments SET status = %s, updated_at = NOW() WHERE tracking_code = %s", # <--- Add updated_at = NOW()
                (order['status'], order['tracking_code'])
            )
        repo.conn.commit()
        cursor.close()
        
        logger.info(f"Successfully transitioned and updated {len(updated_shipments)} EXISTING shipments.")

        # --- 3. KAFKA STREAMING (CDC) ---
        # Combine both new and updated records so Kafka gets the full picture
        all_cdc_events = new_shipments + updated_shipments
        
        if all_cdc_events:
            logger.info(f"Initializing Kafka Producer at {kafka_broker}...")
            kafka_client = LogisticsKafkaProducer(broker_url=kafka_broker)
            
            logger.info(f"Streaming {len(all_cdc_events)} total events to Kafka topic 'logistics_shipments'...")
            for shipment in all_cdc_events:
                kafka_client.publish_shipment_event(
                    topic='logistics_shipments', 
                    shipment_data=shipment
                )
            
            # Ensure all messages leave the Python buffer and hit the Kafka server
            kafka_client.flush()
            logger.info("Kafka streaming complete.")
        else:
            logger.info("No events to stream to Kafka this cycle.")

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

