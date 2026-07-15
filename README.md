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
logistics-lakehouse/
├── docker-compose.yaml             # Airflow & Postgres container infrastructure
├── Dockerfile                      # Custom Airflow image (Java + PySpark + dbt)
├── startup.sh                      # Environment initialization and build script
├── requirements.txt                # Python dependencies (Pandas, BigQuery, PySpark)
├── .env.example                    # Template for environment variables and API keys
│
├── dags/
│   ├── pipeline_datasets.py        # Centralized Airflow Dataset declarations (Data-aware scheduling)
│   ├── dag_shipment_sim.py         # Generates synthetic order data into PostgreSQL
│   ├── dag_postgres_to_bq.py       # Extracts Postgres CDC → BigQuery (Staging/MERGE)
│   ├── dag_tomorrowio.py           # Fetches Tomorrow.io API → GCS Bronze Parquet
│   ├── dag_silver_cdc.py           # Spark job: CDC → Silver Iceberg tables
│   ├── dag_silver_weather.py       # Spark job: Weather Parquet → Silver Iceberg tables
│   ├── dag_gold_dbt.py             # Triggers dbt build for Gold Layer
│   │
│   ├── logistics/                  # Logistics Domain Logic
│   │   ├── simulator.py            # Generates orders based on weather profiles
│   │   ├── profiles.py             # Maps weather conditions to business impact profiles
│   │   └── repository.py           # Postgres connection and initialization logic
│   │
│   ├── spark/                      # PySpark ETL Scripts
│   │   ├── common.py               # SparkSession builder with GCS/Iceberg configurations
│   │   ├── silver_cdc.py           # Iceberg processing logic for CDC data
│   │   ├── silver_weather.py       # Iceberg processing logic for Weather data
│   │   └── bq_sync.py              # BigQuery External Table metadata sync helper
│   │
│   └── weather/                    # Weather API Domain Logic
│       ├── fetcher.py              # Tomorrow.io API client
│       ├── loader.py               # Serializes API responses 
│       └── repository.py           # API fetch logging for Postgres
│
├── dbt/                            # Data Build Tool (Transformation Layer)
│   ├── dbt_project.yml             # dbt project configuration
│   ├── profiles.yml                # BigQuery connection configuration
│   └── models/
│       ├── staging/                # Standardizes names, casts types (stg_shipments, stg_weather)
│       └── mart/                   # Business logic and aggregations (fact_shipment_weather)
│
├── data-init/                      # Database Initialization Scripts
│   ├── logistics_schema.sql        # PostgreSQL DDL (Hubs, Drivers, Shipments)
│   ├── seed_data.sql               # Inserts dimensional baseline data
│   └── gold_bq_mv.sql              # BigQuery Materialized View DDL (realtime_order_stats)
│
└── dashboard/
    └── dashboard_queries.md        # Reference queries for Looker Studio configuration
```


## Layer Schema
The data warehouse follows a Medallion-style architecture, orchestrating data through progressively refined states:

### 1. Bronze (Raw Ingestion)
Raw data is ingested as-is. CDC streams land as Avro, while API fetches land as Parquet.
* **Logistics CDC:** Extracted from PostgreSQL via `dag_postgres_to_bq` (Using a Staging → MERGE Upsert pattern for BigQuery) and `dag_silver_cdc` (PySpark to Iceberg).
* **Weather APIs:** Fetched hourly via `dag_tomorrowio` and saved locally as Parquet files using `weather/loader.py`.

### 2. Silver (Cleaned & Integrated)
Data is strictly typed, deduplicated, and stored in high-performance formats (Iceberg/Parquet/BigQuery Native).
* **Logistics Core:** The Upserted (`MERGE`) main tables where schema definitions are strictly enforced to handle NULL SCD timestamp columns safely.
* **Iceberg Processing:** `dag_silver_cdc` processes raw Avro into Iceberg tables using `spark/silver_cdc.py`.
* **Weather Integration:** `dag_silver_weather` processes the raw Parquet fetches into partitioned Iceberg tables using `spark/silver_weather.py`.

### 3. Gold (Business / Serving)
Highly aggregated Data Marts built for low-latency BI querying.
* **dbt Transformations:** `dag_gold_dbt` executes models like `fact_shipment_weather` to join the Silver CDC data with the latest weather consensus.
* **Materialized Views:** The final `realtime_order_stats` table is cached as a BigQuery Materialized View to ensure Looker Studio loads instantly without recomputing the heavy aggregations. 

## Setup Guide

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


## Running the Pipeline

### Enable DAGs

In the Airflow UI, unpause the DAGs in this order to ensure downstream datasets are ready to catch triggers:

1. `weather_tomorrowio_pipeline`
2. `silver_weather_dag`
3. `silver_cdc_dag`
4. `logistics_postgres_to_bq`
5. `gold_dbt_dag`
6. `dag_shipment_sim`

### Manual trigger (first run)

```
# 1. Trigger the weather pipeline to fetch live API data and populate Bronze
airflow dags trigger weather_tomorrowio_pipeline

# 2. Trigger the simulator to generate today's operational data in Postgres
airflow dags trigger dag_shipment_sim

# Because this architecture uses Airflow Datasets (Data-Aware Scheduling), triggering the simulator will automatically trigger the downstream CDC, BigQuery Ingestion, and dbt transformation DAGs
```
## DAG Reference

## Dashboard
