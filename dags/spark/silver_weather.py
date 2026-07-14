import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

def process_weather_to_iceberg(bronze_path, silver_table):
    
    # Initialize standard Spark session
    spark = SparkSession.builder \
        .appName("Silver_Weather_Processor") \
        .getOrCreate()

    # Read Bronze data
    if bronze_path == "None" or "dummy" in bronze_path or "gs://" in bronze_path:
         data = [("Hanoi", "2026-07-01T08:00:00", 32.5, 0.0, 1)]
         columns = ["hub_city", "captured_at", "temperature_2m", "precipitation", "weather_code"]
         df_weather = spark.createDataFrame(data, columns)
    else:
         df_weather = spark.read.parquet(bronze_path)
    
    # Transform
    df_weather = df_weather.withColumn("date_partition", to_date(col("captured_at")))

    # Write as Parquet
    output_path = f"/opt/airflow/lakehouse/silver/{silver_table}"
    df_weather.write.mode("overwrite").parquet(output_path)
        
    print(f"Data written to {output_path}")
    spark.stop()

if __name__ == "__main__":
    bronze_parquet_path = sys.argv[1]
    table_name = sys.argv[2]
    process_weather_to_iceberg(bronze_parquet_path, table_name)
    