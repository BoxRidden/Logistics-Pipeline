# dags/dag_shipment_sim.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random
import uuid

# Database connection details (Matches our docker-compose.yaml)
DB_CONFIG = {
    "host": "postgres-source",
    "port": 5432,
    "user": "logistics_admin",
    "password": "supersecret",
    "dbname": "logistics_db"
}

def simulate_shipments():
    """Connects to Postgres to create new orders and update old ones."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 1. Update existing shipments (Move them through the delivery pipeline)
    cursor.execute("""
        UPDATE shipments 
        SET status = CASE 
            WHEN status = 'Pending' THEN 'In Transit'
            WHEN status = 'In Transit' THEN 'Delivered'
            ELSE status END,
            updated_at = CURRENT_TIMESTAMP
        WHERE status IN ('Pending', 'In Transit')
    """)
    updated_count = cursor.rowcount

    # 2. Generate 5 to 15 NEW shipments
    new_shipments_count = random.randint(5, 15)
    for _ in range(new_shipments_count):
        tracking_code = f"VN-{uuid.uuid4().hex[:8].upper()}"
        hub_id = random.choice([1, 2, 3]) # Hanoi, Da Nang, HCM
        driver_id = random.choice([1, 2, 3])
        city = random.choice(["Hanoi", "Hai Phong", "Da Nang", "Nha Trang", "Ho Chi Minh City", "Can Tho"])
        
        cursor.execute("""
            INSERT INTO shipments (tracking_code, hub_id, driver_id, customer_city, status)
            VALUES (%s, %s, %s, %s, 'Pending')
        """, (tracking_code, hub_id, driver_id, city))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Updated {updated_count} old shipments. Created {new_shipments_count} new shipments.")

# Define the Airflow DAG
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'logistics_shipment_simulator',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2), # Runs every 2 minutes
    catchup=False,
    tags=['simulation', 'source_db']
) as dag:

    run_simulation = PythonOperator(
        task_id='generate_and_update_shipments',
        python_callable=simulate_shipments
    )