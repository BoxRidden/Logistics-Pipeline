import random
from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline_datasets import postgres_dataset

def generate_fake_data():
    # Connect to local Postgres inside Docker
    conn = psycopg2.connect(
        host="postgres-airflow",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    # 1. Build the database tables if they don't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hubs (
        hub_id SERIAL PRIMARY KEY, name VARCHAR(50), city VARCHAR(50), lat FLOAT, lon FLOAT
    );
    CREATE TABLE IF NOT EXISTS drivers (
        driver_id SERIAL PRIMARY KEY, name VARCHAR(50), vehicle_type VARCHAR(50), created_at TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS shipments (
        shipment_id SERIAL PRIMARY KEY, tracking_code VARCHAR(50), hub_id INT,
        driver_id INT, customer_city VARCHAR(50), status VARCHAR(20),
        created_at TIMESTAMP, updated_at TIMESTAMP
    );
    """)

    # 2. Seed the basic dimensions if empty
    cursor.execute("SELECT COUNT(*) FROM hubs;")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO hubs (name, city, lat, lon) VALUES ('Hanoi Central', 'Hanoi', 21.0285, 105.8542), ('Da Nang Hub', 'Da Nang', 16.0471, 108.2068), ('HCM City Base', 'Ho Chi Minh City', 10.8231, 106.6297);")
        cursor.execute("INSERT INTO drivers (name, vehicle_type, created_at) VALUES ('John Doe', 'Truck', NOW()), ('Jane Smith', 'Motorcycle', NOW()), ('Nguyen Van A', 'Van', NOW());")

    # 3. Generate a burst of 5-10 random shipments
    cities = ['Hanoi', 'Da Nang', 'Ho Chi Minh City']
    statuses = ['Pending', 'In Transit', 'Delivered']
    
    for _ in range(random.randint(5, 10)):
        cursor.execute(f"""
        INSERT INTO shipments (tracking_code, hub_id, driver_id, customer_city, status, created_at, updated_at)
        VALUES ('TRK-{random.randint(10000,99999)}', {random.randint(1, 3)}, {random.randint(1, 3)}, '{random.choice(cities)}', '{random.choice(statuses)}', NOW(), NOW());
        """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Successfully generated new shipments in local database.")

default_args = {'owner': 'airflow', 'start_date': datetime(2026, 6, 1)}

with DAG('dag_shipment_sim', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    PythonOperator(
        task_id='generate_data', 
        python_callable=generate_fake_data,
        outlets=[postgres_dataset] # This signals that the Postgres database has fresh data!
    )