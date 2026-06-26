#!/bin/bash
set -e

echo "Starting Logistics Lakehouse Setup..."

mkdir -p ./dags ./logs ./plugins ./dbt ./dashboard ./data-init ./scripts

echo -e "AIRFLOW_UID=$(id -u)\n" > .env
cat .env.example >> .env
echo "Environment variables configured."

echo "Building custom Airflow image."
docker compose build

echo "Initializing Airflow Database"
docker compose up airflow-init

echo "Starting all services"
docker compose up -d

echo "================================================="
echo "The Logistics Lakehouse is running."
echo "Airflow UI: http://localhost:8080 (User: airflow / Pass: airflow)"
echo "Source DB is running on localhost:5432"
echo "================================================="
