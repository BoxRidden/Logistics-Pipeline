import sys
import os
from pyspark.sql.functions import to_date, col
from common import build_spark_session

def process_weather_to_iceberg(bronze_path, silver_table):
    
    # Grab the bucket name from the environment
    bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    
    # Initialize our new cloud-connected Spark session
    spark = build_spark_session("Silver_Weather_Processor", bucket)

    print(f"Reading real Bronze data from GCS: {bronze_path}")
    
    # Read the real Parquet files from Google Cloud Storage
    df_weather = spark.read.parquet(bronze_path)
    
    # Transform the data
    df_weather = df_weather.withColumn("date_partition", to_date(col("captured_at")))

    # Ensure the Iceberg namespace exists in GCS
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    # Write back to GCS in Apache Iceberg format
    print("Writing data to Iceberg table...")
    df_weather.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(f"iceberg.silver.{silver_table}")
        
    print(f"SUCCESS: Data appended to Iceberg table iceberg.silver.{silver_table}")
    spark.stop()

if __name__ == "__main__":
    bronze_parquet_path = sys.argv[1]
    table_name = sys.argv[2]
    process_weather_to_iceberg(bronze_parquet_path, table_name)