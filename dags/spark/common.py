import os
from pyspark.sql import SparkSession

def build_spark_session(app_name: str, bucket: str) -> SparkSession:
    keyfile = os.environ.get("GCP_SA_KEYFILE", "/opt/airflow/config/gcp-key.json")
    
    return (
        SparkSession.builder
        .appName(app_name)
        
        # 1. Point directly to our hardcoded JARs instead of using spark.jars.packages
        .config("spark.jars", "/opt/airflow/jars/gcs-connector-hadoop3-shaded.jar,/opt/airflow/jars/iceberg-spark-runtime.jar")
        
        # 2. Iceberg Catalog Configuration 
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", f"gs://{bucket}/iceberg")
        
        # 3. GCS Authentication
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile)
        
        .getOrCreate()
    )
