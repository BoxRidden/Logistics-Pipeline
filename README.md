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


## Layer Schema

## Setup Guide



## Running the Pipeline


## DAG Reference

## Dashboard
