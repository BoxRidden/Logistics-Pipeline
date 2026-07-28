import pandas as pd
import mlflow.sklearn
from google.cloud import bigquery

# Point to your local MLflow container
mlflow.set_tracking_uri("http://mlflow:5000")

def score_data():
    client = bigquery.Client(project="logistics-500519")
    query = """
        SELECT shipment_id, revenue_usd, item_quantity 
        FROM `logistics-500519.logistics_mart.fact_shipment_weather`
        WHERE revenue_usd IS NOT NULL AND item_quantity IS NOT NULL
    """
    df = client.query(query).to_dataframe()

    # Load the 'champion' anomaly model from the Model Registry
    logged_model = 'models:/logistics_anomaly_detector@champion'
    loaded_model = mlflow.sklearn.load_model(logged_model)

    features = df[['revenue_usd', 'item_quantity']] 
    df['is_anomaly'] = loaded_model.predict(features)

    # Count the anomalies just so we can see it in the Airflow logs
    anomaly_count = len(df[df['is_anomaly'] == -1])
    
    # Write ALL rows to BigQuery so Looker always has a table to read
    df.to_gbq(
        destination_table='logistics_mart.predicted_anomalies',
        project_id='logistics-500519',
        if_exists='replace'
    )
    
    print(f"Successfully wrote all {len(df)} rows to BigQuery. Found {anomaly_count} anomalies today.")

if __name__ == "__main__":
    score_data()