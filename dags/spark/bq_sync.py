import sys
from google.cloud import bigquery

def sync_iceberg_to_bigquery(project_id, dataset_id, table_name, gcs_metadata_uri):
    """
    Creates or updates a BigQuery External Table pointing to an Apache Iceberg 
    metadata layer stored in Google Cloud Storage.
    """
    client = bigquery.Client(project=project_id)
    
    #Ensure the Gold dataset exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"Created Gold dataset: {dataset_id}")

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    #Configure the external table to read Iceberg formats natively
    external_config = bigquery.ExternalConfig("ICEBERG")
    # BigQuery expects the path to the Iceberg metadata directory or specific .metadata.json
    external_config.source_uris = [gcs_metadata_uri]

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    #Apply the table to BigQuery
    # If the table exists, this updates its pointer to the latest Iceberg metadata
    try:
        client.delete_table(table_id, not_found_ok=True) # Drop old pointer
        table = client.create_table(table)
        print(f"Successfully synced Iceberg table to BigQuery: {table_id}")
    except Exception as e:
        print(f"Failed to sync to BigQuery: {e}")

if __name__ == "__main__":
    project = sys.argv[1]
    dataset = sys.argv[2]
    table = sys.argv[3]
    iceberg_uri = sys.argv[4]
    
    sync_iceberg_to_bigquery(project, dataset, table, iceberg_uri)