import pandas as pd
import mlflow.sklearn
from google.cloud import bigquery
import os

mlflow.set_tracking_uri("http://logisticspipeline-mlflow:5000")

def score_data():
    client = bigquery.Client(project="logistics-500519")
    query = """
        SELECT shipment_id, revenue_usd, order_quantity 
        FROM `logistics-500519.logistics_mart.fact_shipment_weather`
        WHERE revenue_usd IS NOT NULL AND order_quantity IS NOT NULL 
    """
    df = client.query(query).to_dataframe()

    # Load the 'champion' model from the Model Registry
    logged_model = 'models:/logistics_anomaly_detector@champion'
    loaded_model = mlflow.sklearn.load_model(logged_model)

    features = df[['revenue_usd', 'order_quantity']] 
    df['is_anomaly'] = loaded_model.predict(features)

    anomalies_df = df[df['is_anomaly'] == -1]

    if not anomalies_df.empty:
        anomalies_df.to_gbq(
            destination_table='logistics_mart.predicted_anomalies',
            project_id='logistics-500519',
            if_exists='replace'
        )
        print(f"Successfully wrote {len(anomalies_df)} anomalies to BigQuery!")
    else:
        print("No anomalies detected today.")

if __name__ == "__main__":
    score_data()