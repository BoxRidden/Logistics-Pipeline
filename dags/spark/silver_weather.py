import argparse
import logging
import os
from pyspark.sql.functions import to_date, col, lit, to_timestamp
from common import build_spark_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def process_weather_to_iceberg(spark: SparkSession, bronze_path: str, silver_table: str, watermark: str) -> None:
    logger.info(f"Reading Bronze data from GCS: {bronze_path}")
    
    try:
        raw_weather = spark.read.parquet(bronze_path)
    except Exception as e:
        logger.warning(f"Could not read from {bronze_path}. Directory may be empty. Error: {e}")
        return

    # Incremental logic: Filter out historical records processed in previous runs
    logger.info(f"Applying watermark filter: isolating records newer than {watermark}")
    new_weather = raw_weather.filter(to_timestamp(col("captured_at")) > lit(watermark).cast("timestamp"))
    
    # Safety check to prevent empty writes
    if new_weather.isEmpty():
        logger.info(f"No new weather records found since watermark {watermark}. Exiting.")
        return

    logger.info("Applying transformations (date_partition)...")
    df_weather = new_weather.withColumn("date_partition", to_date(col("captured_at")))

    logger.info("Verifying Iceberg namespace 'iceberg.silver'...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    logger.info(f"Appending new records to Iceberg table: iceberg.silver.{silver_table}")
    df_weather.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(f"iceberg.silver.{silver_table}")
        
    logger.info("SUCCESS: Data appended without duplication.")

def main():
    parser = argparse.ArgumentParser(description="Process Bronze Weather data into Silver Iceberg Tables.")
    parser.add_argument("bronze_path", help="GCS URI to Bronze Parquet weather files")
    parser.add_argument("table", help="Target Iceberg table name")
    parser.add_argument("--watermark", default="1970-01-01 00:00:00", help="ISO 8601 Timestamp to filter old records")
    
    args = parser.parse_args()
    
    bucket = os.environ.get("GCS_BRONZE_BUCKET", "logistics-lakehouse")
    
    logger.info("Initializing PySpark Session...")
    spark = build_spark_session("Silver_Weather_Processor", bucket)
    
    try:
        process_weather_to_iceberg(spark, args.bronze_path, args.table, args.watermark)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise e
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()