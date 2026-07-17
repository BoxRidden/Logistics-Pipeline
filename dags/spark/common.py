import os
from pyspark.sql import SparkSession

def build_spark_session(app_name: str, bucket: str) -> SparkSession:
    # Use the env var if it exists, otherwise use this default path
    keyfile = os.environ.get("GCS_SA_KEYFILE", "/opt/airflow/config/gcp-key.json")
    
    return (
        SparkSession.builder
        .appName(app_name)
        # Packages
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22")
        # Iceberg Catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", f"gs://{bucket}/iceberg")
        # GCS Auth
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile)
        # Buffer Overrides (Force as strings to bypass the "m" check)
        .config("spark.hadoop.fs.gs.block.size", "67108864")
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "67108864")
        .config("spark.hadoop.fs.gs.outputstream.upload.buffer.size", "8388608")
        .config("spark.hadoop.fs.gs.inputstream.inplace.seek.limit", "8388608")
        .config("spark.hadoop.fs.gs.inputstream.buffer.size", "2097152")
        .getOrCreate()
    )
