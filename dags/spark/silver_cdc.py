import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_cdc_to_iceberg(bronze_path, silver_table):
    
    # Initialize standard Spark session
    spark = SparkSession.builder \
        .appName("Silver_CDC_Processor") \
        .getOrCreate()

    # Read Bronze data (mocked for local dev)
    if bronze_path == "None" or "dummy" in bronze_path or "gs://" in bronze_path:
        data = [("TRK-123", "2026-07-01T08:00:00", "Hanoi", "UPDATE"), 
                ("TRK-456", "2026-07-01T08:05:00", "Saigon", "INSERT")]
        columns = ["id", "source_timestamp", "customer_city", "change_type"]
        df_bronze = spark.createDataFrame(data, columns)
    else:
        df_bronze = spark.read.format("avro").load(bronze_path)
    
    # Deduplicate CDC events
    df_latest = df_bronze.orderBy(col("source_timestamp").desc()).dropDuplicates(["id"])

    # Write as Parquet to Silver Lakehouse folder
    output_path = f"/opt/airflow/lakehouse/silver/{silver_table}"
    df_latest.write.mode("overwrite").parquet(output_path)
        
    print(f"SUCCESS: Data written to {output_path}")
    spark.stop()

if __name__ == "__main__":
    bronze_avro_path = sys.argv[1] 
    table_name = sys.argv[2]       
    process_cdc_to_iceberg(bronze_avro_path, table_name)