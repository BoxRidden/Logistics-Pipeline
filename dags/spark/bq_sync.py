import sys
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

def sync_iceberg_to_bigquery(project_id, dataset_id, table_name, gcs_metadata_dir):
    # Extract bucket and prefix
    path_parts = gcs_metadata_dir.replace("gs://", "").split("/")
    bucket_name = path_parts[0]
    prefix = "/".join(path_parts[1:]) + "/"
    
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Grab the exact active version directly from Iceberg's version-hint.text 
    hint_blob = bucket.blob(f"{prefix}version-hint.text")
    
    if hint_blob.exists():
        version = hint_blob.download_as_text().strip()
        latest_uri = f"gs://{bucket_name}/{prefix}v{version}.metadata.json"
        print(f"Reading version-hint.text. Active metadata is: {latest_uri}")
    else:
        # Fallback to scanning if the hint file is temporarily missing
        blobs = list(bucket.list_blobs(prefix=prefix))
        json_blobs = [b for b in blobs if b.name.endswith('.metadata.json')]
        if not json_blobs:
            raise FileNotFoundError(f"No Iceberg metadata found in {gcs_metadata_dir}")
        latest_blob = max(json_blobs, key=lambda b: b.time_created)
        latest_uri = f"gs://{bucket_name}/{latest_blob.name}"
        print(f"Fallback scan found: {latest_uri}")

    # Apply to BigQuery
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(dataset_ref))

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id)
    
    external_config = bigquery.ExternalConfig("ICEBERG")
    external_config.source_uris = [latest_uri] 
    table.external_data_configuration = external_config

    try:
        # Check if the table exists first to avoid ambiguity
        client.get_table(table_id) 
        
        # If it exists, update pointer
        client.update_table(table, ["external_data_configuration"])
        print(f"Successfully UPDATED Iceberg table in BigQuery: {table_id}")
    except NotFound:
        # If it does not exist, create it
        client.create_table(table)
        print(f"Successfully CREATED Iceberg table in BigQuery: {table_id}")
    except Exception as e:
        print(f"Failed to sync to BigQuery: {e}")
        raise e

if __name__ == "__main__":
    project = sys.argv[1]
    dataset = sys.argv[2]
    table = sys.argv[3]
    iceberg_uri = sys.argv[4]
    
    sync_iceberg_to_bigquery(project, dataset, table, iceberg_uri)