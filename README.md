# Basic logistic pipeline project
This project was built primarily as a hands-on practice exercise to explore modern data engineering concepts. While this was created as a personal learning environment, it successfully implements several production-grade patterns—such as idempotent Upsert architecture, dynamic schema resolution, Data-Aware orchestration, and MLOps integration—to simulate a real-world, end-to-end analytics infrastructure.

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

Logistics Lakehouse simulates an end-to-end data platform for a regional delivery network operating across major Vietnamese cities. The pipeline:

- **Ingests** operational data (shipments, drivers, hubs) through a Python simulator acting as a CDC stream into PostgreSQL, featuring synthetic anomaly and delay injection.
- **Enriches** logistics tracking data with **real-time weather consensus modeling** by aggregating live data from Tomorrow.io, OpenWeather, and Open-Meteo.
- **Validates** incoming raw data via strict PySpark Data Quality Gates using cloud-native prefix scanning on a GCS Bronze Layer.
- **Processes** the raw data into the **Apache Iceberg** open-table format via PySpark, dynamically mounting the active metadata (`version-hint.text`) into Google BigQuery as External Tables.
- **Orchestrates** dependencies using **Airflow Datasets** (Data-Aware Scheduling), ensuring the Silver processing layer only triggers upon strict consensus (AND logic) from all three weather APIs.
- **Transforms** data through Bronze → Silver → Gold layers entirely managed by **dbt-bigquery**.
- **Predicts** supply chain friction using **MLflow**. An *Isolation Forest* model flags anomalous/fraudulent orders, while a *Random Forest* model predicts weather-driven delays. A batch scoring pipeline writes these unified predictions back to BigQuery.
- **Visualizes** operations through a Looker Studio Command Center showing dual-currency revenue (USD/VND), anomaly alerts, and weather-driven supply chain impacts.

The core hypothesis being modeled: *severe weather (thunderstorms/rain/dusty) → higher delay probabilities and longer transit times; clear weather → optimal delivery rates and faster fulfillment.*

## Architecture 
```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
│                                                                         │
│  PostgreSQL (Local Simulator)       Weather APIs                        │
│  ├── shipments (w/ anomalies)       ├── OpenWeather                     │
│  ├── hubs                           ├── Open-Meteo                      │
│  └── drivers                        └── Tomorrow.io                     │
└──────────────┬──────────────────────────────┬───────────────────────────┘
               │ Pandas / Python              │ Python / REST API
               │ (CDC Extract)                │ (Hourly Fetch)
               ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER — GCS Raw Parquet                       │
│  gs://{bucket}/bronze/cdc/{table}/                                      │
│  gs://{bucket}/bronze/weather/{api_name}/                               │
│                                                                         │
│  Format: Parquet (Microsecond-precision)   No transformation applied    │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ Airflow Dataset Triggered (Strict AND consensus logic)
               │ + Native GCS Prefix Scanning (Data Quality Gates)
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER — Apache Iceberg on GCS                   │
│  gs://{bucket}/iceberg/silver/                                          │
│  ├── shipments  (Overwrite — accumulating order history)                │
│  ├── hubs       (Overwrite — latest 1-to-1 dimensional state)           │
│  ├── drivers    (Overwrite — latest 1-to-1 dimensional state)           │
│  └── weather    (Append — deduplicated batch from all 3 APIs)           │
│                                                                         │
│  ACID · Version-hint tracking · Schema evolution · Storage-backed       │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ bq_sync.py (Auto-updates BQ pointer via v-metadata.json)
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER — BigQuery Native (dbt)                   │
│  Dataset: logistics_mart                                                │
│  ├── stg_... (Staging: Clean & cast)                                    │
│  ├── int_weather_consensus (Intermediate: 3-API hourly voting)          │
│  ├── fact_shipment_weather (Mart: Shipment grain, enriched)             │
│  └── realtime_order_stats  (Mart: Dual-currency KPIs & counts)          │
└─────────────────────────────────────────────────────────────────────────┘
               │                                      
               ▼                                      
┌─────────────────────────────────────────┐           
│           MLOps & AI LAYER              │           
│  MLflow (Model Registry & Tracking)     │           
│  ├── Isolation Forest (Anomalies)       │           
│  └── Random Forest (Delay Prediction)   │           
│                                         │           
│  Outputs to: ml_predictions (BigQuery)  │           
└─────────────────────────────────────────┘           
                                                   ▼

```

```mermaid
graph TD
    %% Styling classes to differentiate DAGs and Datasets
    classDef dag fill:#f8f9fa,stroke:#dee2e6,stroke-width:1px,color:#212529;
    classDef dataset fill:#ffffff,stroke:#ced4da,stroke-width:1px,color:#495057;

    %% CDC Pipeline Nodes
    sim(dag_shipment_sim):::dag
    ds_cdc_ext[("logistics://postgres/cdc_extract")]:::dataset
    p2b(postgres_to_bronze_cdc):::dag
    ds_cdc_pq[("logistics://gcs/bronze_cdc")]:::dataset
    scdc(silver_cdc_dag):::dag
    ds_silv_cdc[("logistics://iceberg/silver_cdc")]:::dataset

    %% Weather Pipeline Nodes
    tom(weather_tomorrowio_pipeline):::dag
    ds_tom[("logistics://gcs/bronze_weather_tomorrowio")]:::dataset
    
    om(weather_openmeteo_pipeline):::dag
    ds_om[("logistics://gcs/bronze_weather_openmeteo")]:::dataset
    
    ow(weather_openweather_pipeline):::dag
    ds_ow[("logistics://gcs/bronze_weather_openweather")]:::dataset
    
    sw(silver_weather_dag):::dag
    ds_silv_w[("logistics://iceberg/silver_weather")]:::dataset

    %% Gold & MLOps Target Nodes
    dbt(gold_dbt_dag):::dag
    ds_gold[("logistics://bigquery/gold_mart")]:::dataset
    mlops(mlops_model_training):::dag

    %% CDC Pipeline Flow
    sim --> ds_cdc_ext
    ds_cdc_ext --> p2b
    p2b --> ds_cdc_pq
    ds_cdc_pq --> scdc
    scdc --> ds_silv_cdc

    %% Weather Pipeline Flow
    tom --> ds_tom
    om --> ds_om
    ow --> ds_ow
    
    ds_tom --> sw
    ds_om --> sw
    ds_ow --> sw
    
    sw --> ds_silv_w

    %% Gold Trigger Convergence
    ds_silv_cdc --> dbt
    ds_silv_w --> dbt

    %% MLOps Trigger Convergence
    dbt --> ds_gold
    ds_gold --> mlops
```
![AirflowDatasets](docs/dags/AirflowDatasets.png)



## Pipeline Flow

The Logistics Lakehouse operates on a Medallion Architecture (Bronze, Silver, Gold), heavily orchestrated by Airflow's Data-Aware Scheduling (Datasets) to create a reactive, event-driven pipeline.

### 1. Ingestion (Data Sources → Bronze Layer)
*   **Operational CDC Extraction:** A custom Python simulator generates live logistics data (shipments, driver updates, hub statuses) into a local **PostgreSQL** database. The simulator intelligently injects synthetic anomalies (massive fraudulent orders) and logical transit delays to train downstream machine learning models. Airflow extracts this data, casts timestamps to microsecond-precision, and loads it into Google Cloud Storage (GCS) as raw **Parquet** files.
*   **Weather API Ensemble:** Airflow simultaneously triggers three separate API fetches (**Tomorrow.io, OpenWeather, Open-Meteo**). The JSON responses are parsed and serialized directly into GCS Bronze Parquet files. 

### 2. Processing (Bronze Layer → Silver Layer)
*   **Data Quality Gates:** Before processing, Airflow validates the Bronze payloads using native GCP prefix scanning, ensuring data integrity (no nulls, schema validation) and preventing pipeline failure from empty API responses.
*   **Event-Driven PySpark:** As soon as the Bronze Datasets are updated (requiring strict `AND` consensus from all APIs), Airflow triggers distributed **PySpark** jobs to process the raw Parquet files. 
*   **Apache Iceberg:** The data is written into **Apache Iceberg** tables hosted on GCS. PySpark handles CDC deduplication and overwrites the dimension and fact tables, while safely appending the new weather payloads using Airflow's execution time macros to prevent duplicate processing.
*   **BigQuery Pointer Sync:** Because Iceberg continuously generates new metadata snapshots, a custom `bq_sync.py` script reads the `version-hint.text` file and dynamically updates **Google BigQuery External Tables** to point to the newest active snapshot, auto-resolving any schema evolution conflicts.

### 3. Transformation (Silver Layer → Gold Layer)
*   Once the BigQuery external tables are synced, Airflow triggers **dbt (Data Build Tool)** to execute the final transformation models:
    *   **Staging:** Cleans, standardizes, and applies business logic (converting USD to VND currency).
    *   **Weather Consensus:** A custom dbt view aggregates the three distinct weather APIs, using an hourly voting mechanism to determine the most accurate weather code for a given city.
    *   **Data Marts:** Builds the final `fact_shipment_weather` table and overwrites the `realtime_order_stats` table for high-performance dashboarding and ML training.

### 4. Predictive Analytics (Gold Layer → MLOps)
*   **Model Training:** Airflow triggers Python scripts that fetch the transformed Gold data from BigQuery to train models tracked and registered via **MLflow**.
*   **Anomaly Detection:** An *Isolation Forest* model isolates and flags fraudulent or mathematically impossible orders (e.g., $15,000 revenue for standard delivery).
*   **Delay Prediction:** A *Random Forest Classifier* evaluates weather severity against order volume to predict if a shipment is at risk of delay.
*   **Batch Scoring:** A unified scoring script evaluates live orders against the "champion" models and writes an `ml_predictions` table back to BigQuery.

### 5. Serving & Visualization (Gold Layer & ML → Looker Studio)
*   **Looker Studio** connects directly to the dbt-managed BigQuery data marts and the ML predictions table.
*   The dashboard functions as a Command Center, surfacing the correlation between severe weather events and supply chain delays, dual-currency operational scorecards (Total Revenue, Active Orders), real-time anomaly alerts, and an early-warning system for predicted logistics failures. 


## Tech Stack

| Component | Technology | Role / Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.9 (Dockerized) with Data-Aware Scheduling |
| Compute & Processing | PySpark & Pandas | Data chunking, deduping, timestamp standardizing, and ETL |
| Table Format | Apache Iceberg | Cloud-native Hadoop Catalog |
| Storage Environment | Google Cloud Storage | Bronze Parquet & Silver Iceberg Metadata |
| Source Database | PostgreSQL | 13 (Simulated CDC Source with Anomaly Injection) |
| Data Warehouse | Google Cloud BigQuery | Cloud Analytics Engine & Iceberg External Table Host |
| Transformation | dbt-bigquery | Gold Layer / Data Mart Modeling & Dual-Currency Logic |
| MLOps & AI | MLflow & Scikit-Learn | Isolation Forest (Anomalies) & Random Forest (Delays) |
| External APIs | Tomorrow.io, OpenWeather, Open-Meteo | Real-time weather data ensemble |
| BI / Visualization | Looker Studio | Interactive Command Center UI |
| CI / CD | GitHub Actions | Automated DAG parsing, dbt compilation, & dependency resolution |
| Containerization | Docker & Docker Compose | Infrastructure deployment |
| Runtime | Python 3.12 & Java (default-jre) | Core execution environments |

## Project Structure

```text
logistics-lakehouse/
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions CI/CD (DAG integrity, dbt parsing, dependency checks)
│
├── docker-compose.yaml             # Airflow, Postgres, & MLflow container infrastructure
├── Dockerfile                      # Custom Airflow image (PySpark, dbt, MLflow, Scikit-Learn)
├── startup.sh                      # Environment initialization and build script
├── requirements.txt                # Python dependencies (strictly pinned to prevent pip backtracking loops)
├── .env.example                    # Template for environment variables and API keys
│
├── dags/
│   ├── pipeline_datasets.py        # Centralized Airflow Dataset declarations (Data-aware scheduling)
│   ├── dag_shipment_sim.py         # Generates synthetic order data into PostgreSQL
│   ├── dag_postgres_cdc.py         # Extracts Postgres CDC → GCS Bronze Parquet
│   ├── dag_tomorrowio.py           # Fetches Tomorrow.io API → GCS Bronze Parquet
│   ├── dag_openmeteo.py            # Fetches Open-Meteo API → GCS Bronze Parquet
│   ├── dag_openweather.py          # Fetches OpenWeather API → GCS Bronze Parquet
│   ├── dag_silver_cdc.py           # Spark job: CDC → Silver Iceberg tables
│   ├── dag_silver_weather.py       # Spark job: Bronze Weather → Silver Iceberg (w/ Data Quality Gates)
│   ├── dag_gold_dbt.py             # Triggers dbt build for Gold Layer in BigQuery
│   ├── dag_mlops_training.py       # Orchestrates MLflow model training and batch scoring
│   │
│   ├── logistics/                  # Logistics Domain Logic
│   │   ├── simulator.py            # Generates orders and dynamically injects anomalies & delays
│   │   ├── profiles.py             # Maps weather conditions to business impact profiles
│   │   └── repository.py           # Postgres connection and initialization logic
│   │
│   ├── mlops/                      # Machine Learning Scripts
│   │   ├── train_anomaly_model.py  # Trains Isolation Forest on BigQuery data to detect fraud
│   │   ├── train_delay_model.py    # Trains Random Forest to predict weather-driven delays
│   │   └── batch_scoring.py        # Scores live orders against champion models → BigQuery ml_predictions
│   │
│   ├── spark/                      # PySpark ETL Scripts
│   │   ├── common.py               # SparkSession builder with GCS/Iceberg configurations
│   │   ├── silver_cdc.py           # Iceberg processing logic for CDC data
│   │   ├── silver_weather.py       # Iceberg processing logic for Weather data
│   │   └── bq_sync.py              # BigQuery External Table dynamic schema & metadata auto-sync
│   │
│   └── weather/                    # Weather API Domain Logic
│       ├── fetcher.py              # API client for weather endpoints
│       ├── loader.py               # Serializes API responses to GCS Parquet
│       └── repository.py           # API fetch logging for Postgres
│
├── dbt/                            # Data Build Tool (Transformation Layer)
│   ├── dbt_project.yml             # dbt project configuration
│   ├── profiles.yml                # BigQuery connection configuration
│   └── models/
│       ├── staging/                # Standardizes names, casts types (stg_shipments, stg_weather)
│       ├── intermediate/           # Joins and applies business logic (int_weather_consensus)
│       └── mart/                   # Business logic, aggregations, and consensus views (fact_shipment_weather)
│
├── data-init/                      # Database Initialization Scripts
│   ├── logistics_schema.sql        # PostgreSQL DDL (Hubs, Drivers, Shipments)
│   ├── seed_data.sql               # Inserts dimensional baseline data
│   └── gold_bq_mv.sql              # BigQuery Materialized View DDL (realtime_order_stats)
│
└── dashboard/
    └── dashboard_queries.md        # Reference queries for Looker Studio configuration (including ML predictions)
```

## Layer Schema

### Source — PostgreSQL (Local Simulator)

| Table | Description |
|---|---|
| `hubs` | hub_id, name, city, lat, lon, valid_from, is_current |
| `drivers` | driver_id, name, vehicle_type, valid_from, is_current |
| `shipments` | shipment_id, tracking_code, hub_id, driver_id, customer_city, status, revenue, item_quantity, product_category, order_type |

```mermaid
erDiagram
    %% Source Layer - PostgreSQL Relational Database
    hubs ||--o{ shipments : "processes"
    drivers ||--o{ shipments : "delivers"

    %% PostgreSQL Tables
    shipments {
        serial shipment_id PK
        varchar tracking_code
        int hub_id FK
        int driver_id FK
        varchar customer_city
        varchar status
        float revenue
        int item_quantity
        varchar product_category
        varchar order_type
        timestamp created_at
        timestamp updated_at
    }

    hubs {
        int hub_id PK
        varchar name
        varchar city
        decimal lat
        decimal lon
        timestamp valid_from PK
        timestamp valid_to
        boolean is_current
    }

    drivers {
        int driver_id PK
        varchar name
        varchar vehicle_type
        timestamp valid_from PK
        timestamp valid_to
        boolean is_current
    }
```


### Bronze — Raw Ingestion (GCS Parquet)

| Path / Target | Source |
|---|---|
| `gs://{bucket}/bronze/cdc/shipments/` | PostgreSQL Extract (via Pandas) |
| `gs://{bucket}/bronze/cdc/hubs/` | PostgreSQL Extract (via Pandas) |
| `gs://{bucket}/bronze/cdc/drivers/` | PostgreSQL Extract (via Pandas) |
| `gs://{bucket}/bronze/weather/{api_name}/` | Tomorrow.io, Open-Meteo, OpenWeather |

![bronze](docs/gcs/bronze.png)

### Silver — Cleaned & Integrated (Apache Iceberg) 

| Table | Grain | Strategy | Output |
|---|---|---|---|
| `iceberg.silver.shipments` | 1 row per shipment | Overwrite | Mounted to BQ as `logistics_raw.shipments` |
| `iceberg.silver.hubs` | Latest row per version | Overwrite | Mounted to BQ as `logistics_raw.hubs` |
| `iceberg.silver.drivers` | Latest row per version | Overwrite | Mounted to BQ as `logistics_raw.drivers` |
| `iceberg.silver.weather` | 1 row per city per API | Append | Mounted to BQ as `logistics_raw.weather` |

```mermaid 
erDiagram
    %% Relationships
    hubs ||--o{ shipments : "processes"
    drivers ||--o{ shipments : "delivers"

    %% Silver Iceberg Tables
    shipments {
        int shipment_id PK
        varchar tracking_code
        int hub_id FK
        int driver_id FK
        varchar customer_city
        varchar status
        float revenue
        int item_quantity
        varchar product_category
        varchar order_type
        timestamp created_at
        timestamp updated_at
    }

    hubs {
        int hub_id PK
        timestamp valid_from PK
        varchar name
        varchar city
        decimal lat
        decimal lon
        timestamp valid_to
        boolean is_current
    }

    drivers {
        int driver_id PK
        timestamp valid_from PK
        varchar name
        varchar vehicle_type
        timestamp valid_to
        boolean is_current
    }

    weather {
        varchar hub_city PK
        timestamp captured_at PK
        float temperature_2m
        float precipitation
        int weather_code
        date date_partition
    }
```

### Gold — BigQuery Data Marts (dbt)

| Table / View | Type | Description |
|---|---|---|
| `logistics_mart.stg_weather_consensus` | dbt view | Hourly aggregation and voting consensus of all 3 weather API fetches |
| `logistics_mart.fact_shipment_weather` | dbt table | Main analytics fact joining shipments, weather conditions, and 1-to-1 dimensional data |
| `logistics_mart.realtime_order_stats` | dbt view | Near-real-time operational counts and dual-currency (USD/VND) revenue scorecards |

```mermaid 
erDiagram
    %% Relationships
    HUBS ||--o{ SHIPMENTS : "processes"
    DRIVERS ||--o{ SHIPMENTS : "delivers"
    HUBS ||--o{ WEATHER_CONSENSUS : "experiences (joined on city)"

    %% Tables & Columns 
    SHIPMENTS {
        int shipment_id PK
        varchar tracking_code
        int hub_id FK
        int driver_id FK
        varchar customer_city
        varchar status
        float revenue
        int item_quantity
        varchar product_category
        varchar order_type
        timestamp created_at
        timestamp updated_at
    }

    HUBS {
        int hub_id PK
        timestamp valid_from PK
        varchar name
        varchar city
        decimal lat
        decimal lon
        timestamp valid_to
        boolean is_current
    }

    DRIVERS {
        int driver_id PK
        timestamp valid_from PK
        varchar name
        varchar vehicle_type
        timestamp valid_to
        boolean is_current
    }

    WEATHER_CONSENSUS {
        varchar hub_city PK
        timestamp weather_captured_at PK
        float temperature_celsius
        float precipitation_mm
        int weather_code
        int api_response_count
    }
```


## Setup Guide

| Requirement | Notes |
|---|---|
| Docker Desktop ≥ 4.x | Allocate **≥ 8 GB RAM** in Docker settings (required for PySpark & Airflow) |
| GCP Project | With BigQuery and Cloud Storage APIs enabled |
| GCP Service Account | Roles: `BigQuery Admin`, `Storage Admin` |
| API Keys | Tomorrow.io and OpenWeather API keys (Open-Meteo is open-source/free) |

### 1. Clone the repository

```bash
git clone [https://github.com/](https://github.com/)<your-username>/logistics-lakehouse.git
cd logistics-lakehouse
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
### 4. Configure dbt
Update the dbt/profiles.yml file to match your specific GCP Project ID
```
logistics_profile:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "your-gcp-project-id" # <--- UPDATE THIS
      
```
Verify the connection is working from inside the Airflow container (optional):
```
docker exec -it $(docker ps -qf "name=airflow-webserver") bash -c "cd /opt/airflow/dbt && dbt debug"
```      


### 5. Configure BigQuery (Materialized View)
The raw datasets (logistics_raw) and tables will be auto-created by the Airflow ingestion DAG. However, the high-performance Materialized View for the Looker Studio dashboard must be created manually in the BigQuery Console once the base shipments table exists.
After running the `logistics_postgres_to_bq` DAG for the first time, 
copy the contents of `data-init/gold_bq_mv.sql` and run it in your BigQuery SQL Workspace.

### 6. Build and start the infrastructure
Initialize the environment and spin up the Docker containers
```
chmod +x startup.sh
./startup.sh
```
## Local Services & URLs

Once the Docker Compose infrastructure is running (`./startup.sh`), you can access the core platforms via the following local URLs:

| Service | URL | Default Credentials | Description |
| :--- | :--- | :--- | :--- |
| **Apache Airflow UI** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` | DAG orchestration, execution logs, and Data-Aware Dataset graphs. |
| **MLflow UI** | [http://localhost:5000](http://localhost:5000) | *None* | Machine learning model registry, training metrics, and champion tracking. |
| **Looker Studio** | [Link to your Dashboard] | *Google Account* | Live Command Center dashboard (requires manual BQ connection). |

*(Note: If you deploy this to a cloud VM, replace `localhost` with your server's public IP address and ensure the respective ports are open in your firewall rules).*

## Running the Pipeline

### Enable DAGs

In the Airflow UI, unpause the DAGs in this order to ensure downstream datasets are ready to catch triggers:

1. `dag_shipment_sim`
2. `weather_tomorrowio_pipeline`
3. `weather_openweather_pipeline`
4. `weather_openmeteo_pipeline`
5. `silver_weather_dag`
6. `postgres_to_bronze_cdc`
7. `silver_cdc_dag`
8. `gold_dbt_dag`
9. `mlops_model_training`

### Manual trigger

```
# 1. Trigger the weather pipelines to fetch live API data and populate the Bronze layer
airflow dags trigger weather_tomorrowio_pipeline
airflow dags trigger weather_openweather_pipeline
airflow dags trigger weather_openmeteo_pipeline

# 2. Trigger the simulator to generate today's operational data in Postgres
airflow dags trigger dag_shipment_sim

# Because this architecture uses Airflow Datasets (Data-Aware Scheduling), triggering the simulator will automatically trigger the downstream Bronze extraction, Silver PySpark processing, BigQuery external table syncing, and Gold dbt transformations.
```
### Check pipeline status 
```
Airflow UI → DAGs → Graph view
```
After a complete cycle, verify the final materialized data in BigQuery:

```
SELECT * FROM `your-gcp-project-id.logistics_mart.fact_shipment_weather` LIMIT 10;
SELECT * FROM `your-gcp-project-id.logistics_mart.realtime_order_stats` LIMIT 10;
```


## DAG Reference

| DAG ID | Schedule / Trigger | Emits Dataset (Outlets) | Description |
| :--- | :--- | :--- | :--- |
| **`weather_tomorrowio_pipeline`** | `@hourly` (Cron) | `bronze_weather_tomorrow` | Fetches real-time weather from the Tomorrow.io API and saves it locally as Bronze Parquet files. |
| **`weather_openweather_pipeline`** | `@hourly` (Cron) | `bronze_weather_openweather` | Fetches real-time weather from the OpenWeather API and saves it locally as Bronze Parquet files. |
| **`weather_openmeteo_pipeline`** | `@hourly` (Cron) | `bronze_weather_openmeteo`| Fetches real-time weather from the Open-Meteo API and saves it locally as Bronze Parquet files. |
| **`dag_shipment_sim`** | `@hourly` (Cron) | `bronze_cdc_dataset` | Generates synthetic logistics shipments and driver updates, inserting them into the local PostgreSQL database to simulate live CDC data. |
| **`postgres_to_bronze_cdc`** | `Dataset` (`bronze_cdc_dataset`) | `bronze_gcs_cdc_dataset` | Extracts the raw Postgres CDC chunks and loads them into GCS as microsecond-precision Parquet files. |
| **`silver_weather_dag`** | `Dataset` (OR condition) | `silver_weather_dataset` | Data-aware PySpark job. Triggered when ANY of the three weather APIs successfully finish downloading. Processes wildcard Bronze Parquet files into Silver Iceberg tables. |
| **`silver_cdc_dag`** | `Dataset` (`bronze_gcs_cdc_dataset`) | `silver_cdc_dataset` | Data-aware PySpark job. Sequentially processes raw CDC data into Silver Iceberg tables and updates BigQuery pointer metadata. |
| **`gold_dbt_dag`** | `Dataset` (OR condition) | — | Triggered automatically when *either* the silver weather or silver CDC datasets are updated. Executes `dbt build` to transform the data and update BigQuery data marts. |
| **`mlops_model_training`** | `Dataset` (`gold_dbt_dataset`) | — | Triggered when dbt finishes. Trains Isolation Forest (anomalies) and Random Forest (delays) models on live BigQuery data via MLflow, then writes batch predictions back to BigQuery. |


## Dashboard

https://datastudio.google.com/s/hQv62WW3Hq8
The Looker Studio Command Center connects directly to the dbt-managed data marts (`logistics_mart.fact_shipment_weather`, `logistics_mart.realtime_order_stats`) and the MLOps inference table (`logistics_mart.ml_predictions`) to visualize:

![LookerReport1](docs/dashboard/LookerReport1.png)
![LookerReport2](docs/dashboard/LookerReport3.png)

- **AI Anomaly Alerts** — filtering and flagging highly anomalous, mathematically impossible orders (e.g., massive $15,000 revenue spikes or 500-item quantities) identified by the Isolation Forest model.
- **Predicted Delay Early-Warnings** — highlighting active orders at high risk of transit delays based on weather severity and operational volume, powered by the Random Forest model.
- **Shipment Status by Weather Condition** — analyzing the direct correlation between severe weather (e.g., Rain, Thunderstorms, Dusty) and the volume of "Delayed" or "Cancelled" orders.
- **Real-Time Logistics Scorecards** — top-level KPIs for Dual-Currency Revenue (VND/USD), Order Count, and Active Shipments (powered by BigQuery Materialized Views for low-latency BI).
- **Order Type Performance** — tracking how "Express" and "Next-Day" deliveries hold up under adverse conditions compared to "Standard" shipping.
- **Hourly Shipment Volume & Hub Distribution** — tracking volume across regional fulfillment centers (Hanoi, Da Nang, Ho Chi Minh City).
- **Revenue by Product Category** — identifying which shipment types (Electronics, Groceries, Clothing) drive volume during specific weather events.