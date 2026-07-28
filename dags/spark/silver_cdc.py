import argparse
import logging
import os
from pyspark.sql.functions import col
from common import build_spark_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def process_cdc_to_iceberg(spark, bronze_path, silver_table):
    logger.info(f"Reading Incremental CDC Bronze data from GCS: {bronze_path}")
    
    try:
        df_cdc = spark.read.parquet(bronze_path)
    except Exception as e:
        logger.warning(f"Could not read {bronze_path} (it may not exist if no updates occurred). Skipping.")
        return

    if df_cdc.isEmpty():
        logger.info("No records to process in this CDC batch.")
        return

    # Primary Key mapping based on the table name 
    if silver_table == "shipments":
        pk = "shipment_id"
        sort_col = "updated_at"
    elif silver_table == "hubs":
        pk = "hub_id"
        sort_col = "valid_from"
    elif silver_table == "drivers":
        pk = "driver_id"
        sort_col = "valid_from"
    else:
        pk = "id"
        sort_col = "updated_at"

    # Deduplicate incremental batch (keep the latest event per PK to prevent MERGE crashes)
    if pk in df_cdc.columns and sort_col in df_cdc.columns:
        logger.info(f"Deduplicating events by {pk} ordering by {sort_col} DESC...")
        df_cdc = df_cdc.orderBy(col(sort_col).desc()).dropDuplicates([pk])
    
    # Drop CDC metadata
    columns_to_drop = ["op_type"] 
    for col_name in columns_to_drop:
        if col_name in df_cdc.columns:
            df_cdc = df_cdc.drop(col_name)

    logger.info("Ensuring Iceberg namespace 'iceberg.silver' exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    table_id = f"iceberg.silver.{silver_table}"

    # CDC UPSERT LOGIC
    if not spark.catalog.tableExists(table_id):
        logger.info(f"Table {table_id} does not exist. Creating and inserting initial data...")
        df_cdc.writeTo(table_id).using("iceberg").create()
        logger.info(f"SUCCESS: Initial data loaded into {table_id}")
    else:
        logger.info(f"Table {table_id} exists. Executing MERGE INTO (Upsert)...")
        df_cdc.createOrReplaceTempView("incremental_updates")
        
        # Dynamically generate the UPDATE SET string for all columns except the Primary Key
        columns = df_cdc.columns
        update_set = ", ".join([f"t.{c} = s.{c}" for c in columns if c != pk])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])
        
        # Execute the Apache Iceberg ACID transaction 
        merge_sql = f"""
            MERGE INTO {table_id} t
            USING incremental_updates s
            ON t.{pk} = s.{pk}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        spark.sql(merge_sql)
        logger.info(f"SUCCESS: CDC Upsert completed for {table_id}")

def main():
    # Argument Parsing
    parser = argparse.ArgumentParser(description="Process Incremental CDC data into Silver Iceberg Tables.")
    parser.add_argument("bronze_path", help="GCS URI to Bronze Parquet CDC file")
    parser.add_argument("table", help="Target Iceberg table name")
    
    args = parser.parse_args()
    
    bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    logger.info("Initializing PySpark Session...")
    spark = build_spark_session(f"Silver_CDC_{args.table}", bucket)
    
    # Catalog caching remains disabled to prevent UUID mismatch ghost errors
    spark.conf.set("spark.sql.catalog.iceberg.cache-enabled", "false")
    
    try:
        process_cdc_to_iceberg(spark, args.bronze_path, args.table)
    except Exception as e:
        logger.error(f"PySpark CDC processing failed: {e}")
        raise e
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()