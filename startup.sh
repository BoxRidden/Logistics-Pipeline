#!/bin/bash
set -e

echo "🚚 Starting Logistics Lakehouse Setup..."

# 1. Create necessary folders
mkdir -p ./dags ./logs ./plugins ./dbt ./dashboard ./data-init ./scripts

# 2. Set up the .env file with the correct User ID for permissions
echo -e "AIRFLOW_UID=$(id -u)\n" > .env
cat .env.example >> .env
echo "✅ Environment variables configured."

# 3. Build custom Airflow image and initialize the database
echo "⏳ Building custom Airflow image (this might take a minute)..."
docker compose build

echo "⚙️ Initializing Airflow Database..."
docker compose up airflow-init

# 4. Start everything
echo "🚀 Starting all services..."
docker compose up -d

echo "================================================="
echo "🎉 SUCCESS! The Logistics Lakehouse is running."
echo "🕸️  Airflow UI: http://localhost:8080 (User: airflow / Pass: airflow)"
echo "🐘 Source DB is running on localhost:5432"
echo "================================================="