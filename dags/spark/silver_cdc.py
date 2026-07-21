import sys
import os
from pyspark.sql.functions import col
from common import build_spark_session

def process_cdc_to_iceberg(bronze_path, silver_table):
    bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    spark = build_spark_session("Silver_CDC_Processor", bucket)
    
    # Catalog caching remains disabled to prevent UUID mismatch ghost errors
    spark.conf.set("spark.sql.catalog.iceberg.cache-enabled", "false")

    print(f"Reading real CDC Bronze data from GCS: {bronze_path}")
    df_cdc = spark.read.parquet(bronze_path)
    
    # Deduplicate CDC events
    if "shipment_id" in df_cdc.columns:
        df_cdc = df_cdc.orderBy(col("updated_at").desc()).dropDuplicates(["shipment_id"])

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    # (REMOVED the DROP TABLE command) Iceberg will now naturally evolve versions.

    # Write data using overwrite mode to create incremental snapshots
    print("Writing CDC data to Iceberg table...")
    df_cdc.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"iceberg.silver.{silver_table}")
        
    print(f"SUCCESS: CDC Data saved to Iceberg table iceberg.silver.{silver_table}")
    spark.stop()

if __name__ == "__main__":
    bronze_parquet_path = sys.argv[1] 
    table_name = sys.argv[2]       
    process_cdc_to_iceberg(bronze_parquet_path, table_name)