# common.py - Verified SparkSession configuration for native GCS access
from pyspark.sql import SparkSession

def build_spark_session(app_name: str) -> SparkSession:
    """
    Initializes a SparkSession with Hadoop configurations required for GCS authentication.
    Service account key path must align with the container volume mount.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # GCS Connector implementations
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # Authentication using explicit Service Account JSON
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/config/gcp-key.json")
        .getOrCreate()
    )