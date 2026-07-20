import sys
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

def sync_iceberg_to_bigquery(project_id, dataset_id, table_name, gcs_metadata_dir):
    """
    Creates or updates a BigQuery External Table pointing to an Apache Iceberg 
    metadata layer stored in Google Cloud Storage.
    """
    # 1. Extract bucket and prefix from the GCS directory string
    path_parts = gcs_metadata_dir.replace("gs://", "").split("/")
    bucket_name = path_parts[0]
    prefix = "/".join(path_parts[1:]) + "/"
    
    # 2. Scan GCS to find the exact Iceberg metadata JSON file
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    print(f"Scanning {gcs_metadata_dir} for the latest metadata JSON...")
    blobs = list(bucket.list_blobs(prefix=prefix))
    json_blobs = [b for b in blobs if b.name.endswith('.metadata.json')]
    
    if not json_blobs:
        raise FileNotFoundError(f"No Iceberg metadata JSON files found in {gcs_metadata_dir}")
        
    # Sort files by creation time to guarantee we always get the newest version
    latest_blob = max(json_blobs, key=lambda b: b.time_created)
    latest_uri = f"gs://{bucket_name}/{latest_blob.name}"
    print(f"Found latest Iceberg metadata: {latest_uri}")

    # 3. Apply it to BigQuery
    client = bigquery.Client(project=project_id)
    
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"Created BigQuery dataset: {dataset_id}")

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    table = bigquery.Table(table_id)
    external_config = bigquery.ExternalConfig("ICEBERG")
    external_config.source_uris = [latest_uri] 
    table.external_data_configuration = external_config

    # 4. Safely Create or Update (No dropping!)
    try:
        # Attempt to update the existing table in-place
        client.update_table(table, ["external_data_configuration"])
        print(f"Successfully UPDATED Iceberg table in BigQuery: {table_id}")
    except NotFound:
        # If it doesn't exist yet, create it
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