import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.client import MlflowClient  
from sklearn.ensemble import IsolationForest
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

def train_and_log_anomaly_model():
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("Logistics_Anomaly_Detection")

    logger.info("Fetching Gold data from BigQuery...")
    client = bigquery.Client(project="logistics-500519")
    query = """
        SELECT shipment_id, revenue_usd, item_quantity 
        FROM `logistics-500519.logistics_mart.fact_shipment_weather`
        WHERE revenue_usd IS NOT NULL AND item_quantity IS NOT NULL
    """
    data = client.query(query).to_dataframe()

    if data.empty:
        logger.warning("No data found in BigQuery. Aborting training.")
        return

    X = data[['revenue_usd', 'item_quantity']]

    with mlflow.start_run():
        # Set contamination to 5% to match our new simulator injection rate
        contamination_rate = 0.05 
        
        mlflow.log_param("contamination", contamination_rate)
        mlflow.log_param("algorithm", "IsolationForest")

        logger.info("Training Isolation Forest Anomaly Detector on live data...")
        model = IsolationForest(contamination=contamination_rate, random_state=42)
        model.fit(X) 

        data['is_anomaly'] = model.predict(X)
        anomaly_count = len(data[data['is_anomaly'] == -1])
        mlflow.log_metric("anomalies_detected", anomaly_count)
        
        logger.info(f"Model trained. Found {anomaly_count} anomalies in the live dataset.")

        # Save and register the model
        model_info = mlflow.sklearn.log_model(sk_model=model, artifact_path="isolation_forest_anomaly_model")
        model_version = mlflow.register_model(model_uri=model_info.model_uri, name="logistics_anomaly_detector")
        
        client = MlflowClient()
        client.set_registered_model_alias("logistics_anomaly_detector", "champion", model_version.version)
        logger.info("Successfully registered Anomaly model as 'champion'.")
        
if __name__ == "__main__":
    train_and_log_anomaly_model()

