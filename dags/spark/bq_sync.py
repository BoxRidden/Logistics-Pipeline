import argparse
import logging
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def sync_iceberg_to_bigquery(project_id: str, dataset_id: str, table_name: str, gcs_metadata_dir: str) -> None:
    """
    Syncs the latest Apache Iceberg metadata from GCS to a BigQuery External Table.
    """
    logger.info(f"Starting BigQuery sync for {project_id}.{dataset_id}.{table_name}")
    
    # Extract bucket and prefix
    path_parts = gcs_metadata_dir.replace("gs://", "").split("/")
    bucket_name = path_parts[0]
    prefix = "/".join(path_parts[1:])
    
    # Safety check for GCS prefix formatting
    if not prefix.endswith("/"):
        prefix += "/"
        
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # Grab the exact active version directly from Iceberg's version-hint.text 
        hint_blob = bucket.blob(f"{prefix}version-hint.text")
        
        if hint_blob.exists():
            version = hint_blob.download_as_text().strip()
            latest_uri = f"gs://{bucket_name}/{prefix}v{version}.metadata.json"
            logger.info(f"Reading version-hint.text. Active metadata is: {latest_uri}")
        else:
            logger.warning(f"version-hint.text not found in {gcs_metadata_dir}. Falling back to directory scan.")
            
            # Fallback to scanning if the hint file is temporarily missing
            blobs = list(bucket.list_blobs(prefix=prefix))
            json_blobs = [b for b in blobs if b.name.endswith('.metadata.json')]
            if not json_blobs:
                raise FileNotFoundError(f"No Iceberg metadata found in {gcs_metadata_dir}")
            
            latest_blob = max(json_blobs, key=lambda b: b.time_created)
            latest_uri = f"gs://{bucket_name}/{latest_blob.name}"
            logger.info(f"Fallback scan found latest metadata: {latest_uri}")

        # Apply to BigQuery
        bq_client = bigquery.Client(project=project_id)
        dataset_ref = bq_client.dataset(dataset_id) 
        
        # Create dataset if it doesn't exist
        try:
            bq_client.get_dataset(dataset_ref)
        except NotFound:
            logger.info(f"Dataset {dataset_id} not found. Creating it...")
            bq_client.create_dataset(bigquery.Dataset(dataset_ref))

        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = bigquery.Table(table_id)
        
        external_config = bigquery.ExternalConfig("ICEBERG")
        external_config.source_uris = [latest_uri] 
        table.external_data_configuration = external_config

        try:
            # Check if the table exists first to avoid ambiguity
            bq_client.get_table(table_id) 
            
            # If it exists, update pointer
            bq_client.update_table(table, ["external_data_configuration"])
            logger.info(f"Successfully UPDATED Iceberg table in BigQuery: {table_id}")
            
        except NotFound:
            # If it does not exist, create it
            bq_client.create_table(table) 
            logger.info(f"Successfully CREATED Iceberg table in BigQuery: {table_id}")

    except Exception as e:
        logger.error(f"Failed to sync to BigQuery: {e}")
        raise e

def main():
    # Argument Parsing 
    parser = argparse.ArgumentParser(description="Sync Iceberg metadata to BigQuery External Tables.")
    parser.add_argument("project", help="GCP Project ID")
    parser.add_argument("dataset", help="BigQuery Dataset Name")
    parser.add_argument("table", help="BigQuery Table Name")
    parser.add_argument("metadata_dir", help="GCS URI to Iceberg metadata directory (e.g., gs://bucket/path/metadata)")
    
    args = parser.parse_args()
    
    sync_iceberg_to_bigquery(
        project_id=args.project, 
        dataset_id=args.dataset, 
        table_name=args.table, 
        gcs_metadata_dir=args.metadata_dir
    )

if __name__ == "__main__":
    main()