import pandas as pd
import mlflow.sklearn
from google.cloud import bigquery

# Point to your local MLflow container
mlflow.set_tracking_uri("http://mlflow:5000")

def score_data():
    client = bigquery.Client(project="logistics-500519")
    
    # Grab the features needed for BOTH models
    query = """
        SELECT shipment_id, revenue_usd, item_quantity, temperature_celsius 
        FROM `logistics-500519.logistics_mart.fact_shipment_weather`
        WHERE revenue_usd IS NOT NULL 
          AND item_quantity IS NOT NULL 
          AND temperature_celsius IS NOT NULL
    """
    df = client.query(query).to_dataframe()

    if df.empty:
        print("No data available to score.")
        return

    # --- 1. SCORE ANOMALIES ---
    anomaly_model = mlflow.sklearn.load_model('models:/logistics_anomaly_detector@champion')
    anomaly_features = df[['revenue_usd', 'item_quantity']] 
    df['is_anomaly'] = anomaly_model.predict(anomaly_features)

    # --- 2. SCORE DELAYS ---
    # Temporarily map temperature_celsius to temperature_2m so the model recognizes it
    df['temperature_2m'] = df['temperature_celsius']
    delay_model = mlflow.sklearn.load_model('models:/logistics_delay_predictor@champion')
    delay_features = df[['revenue_usd', 'item_quantity', 'temperature_2m']]
    df['predicted_is_delayed'] = delay_model.predict(delay_features)

    # Clean up the temporary column before writing to BigQuery
    df = df.drop(columns=['temperature_2m'])

    # Write ALL predictions back to BigQuery into a unified table 
    df.to_gbq(
        destination_table='logistics_mart.ml_predictions',
        project_id='logistics-500519',
        if_exists='replace'
    )
    
    # Logging for Airflow
    anomaly_count = len(df[df['is_anomaly'] == -1])
    delay_count = len(df[df['predicted_is_delayed'] == 1])
    print(f"Successfully wrote {len(df)} rows to BigQuery.")
    print(f"Detected: {anomaly_count} Anomalies | {delay_count} Predicted Delays.")

if __name__ == "__main__":
    score_data()