# Basic logistic pipeline project
This project was built primarily as a hands-on practice exercise to explore modern data engineering concepts. While this was created as a personal learning environment, it successfully implements several production-grade patterns—such as idempotent Upsert (MERGE) architecture, live API integration, and BigQuery Materialized Views—to simulate a real-world, end-to-end analytics infrastructure.

## Table of contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Flow](#pipeline-flow)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Layer Schema](#layer-schema)
- [Setup Guide](#setup-guide)
- [Running the Pipeline](#run-pipeline)
- [DAG Reference](#dag-reference)
- [Dashboard](#dashboard)
  
## Overview

## Architecture 


## Pipeline Flow

## Tech Stack

## Project Structure
```text
├── dags/
│   ├── dag_shipment_sim.py         # Generates synthetic order data
│   ├── dag_postgres_to_bq.py       # ELT ingestion (Postgres -> Staging -> MERGE)
│   ├── dag_gold_dbt.py             # dbt execution and transformation
│   ├── dag_silver_cdc.py           # Spark CDC to Iceberg processing
│   ├── dag_tomorrowio.py           # Weather API ingestion
│   └── logistics/                  # Core Python modules for simulation
├── dbt/                            # dbt project folder (models, profiles.yml)
├── data-init/                      # SQL scripts for DB initialization & BQ Views
├── docker-compose.yaml             # Airflow & Postgres container configurations
├── requirements.txt                # Python dependencies
└── README.md
```

| Requirement | Notes |
|---|---|
| Docker Desktop ≥ 4.x | Allocate **≥ 8 GB RAM** in Docker settings (required for PySpark & Airflow) |
| GCP Project | With BigQuery and Cloud Storage APIs enabled |
| GCP Service Account | Roles: `BigQuery Admin`, `Storage Admin` |
| API Keys | Tomorrow.io API key (Open-Meteo is open-source/free) |

### 1. Clone the repository

```bash
git clone [https://github.com/](https://github.com/)<your-username>/highlands-lakehouse.git
cd highlands-lakehouse
```

### 2. Configure environment variables
Copy the example environment file and fill in your specific values:

```
cp .env.example .env
```
### 3. Place your GCP Service Account key
Save your JSON key file directly into the config directory:

```
config/gcp-key.json
```
### 4. Build and start the Infrastructure

```
chmod +x startup.sh
./startup.sh
```
### 5. Configure BigQuery (Materialized View)
The raw datasets (logistics_raw) and tables will be auto-created by the Airflow ingestion DAG. However, the high-performance Materialized View for the Looker Studio dashboard must be created manually in the BigQuery Console once the base shipments table exists.
After running the `logistics_postgres_to_bq` DAG for the first time, 
copy the contents of `data-init/gold_bq_mv.sql` and run it in your BigQuery SQL Workspace.

### 6. Configure dbt
Update the dbt/profiles.yml file to match your specific GCP Project ID:
```
logistics_profile:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "your-gcp-project-id" # <--- UPDATE THIS
      dataset: logistics_mart
      threads: 4
      keyfile: /opt/airflow/config/gcp-key.json
```
Verify the connection is working from inside the Airflow container (optional):
```
docker exec -it $(docker ps -qf "name=airflow-webserver") bash -c "cd /opt/airflow/dbt && dbt debug"
```
## Layer Schema


## Setup Guide



## Running the Pipeline


## DAG Reference

## Dashboard
