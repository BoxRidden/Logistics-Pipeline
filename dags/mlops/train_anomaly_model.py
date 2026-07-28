import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.client import MlflowClient  
from sklearn.ensemble import IsolationForest
import logging

logger = logging.getLogger(__name__)

def train_and_log_anomaly_model():
    # Make sure "mlflow" matches your Docker compose service name for the tracking server
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("Logistics_Anomaly_Detection")

    logger.info("Fetching Gold data from BigQuery...")
    # Simulated query from your logistics_mart.stg_shipments
    data = pd.DataFrame({
        'shipment_id': ['S1', 'S2', 'S3', 'S4', 'S5', 'S6'],
        'revenue_usd': [120, 115, 130, 9500, 110, 5], 
        'item_quantity': [5, 4, 6, 2, 5, 500],        
    })

    X = data[['revenue_usd', 'item_quantity']]

    with mlflow.start_run():
        contamination_rate = 0.1 # Expecting roughly 10% of the data to be anomalous
        
        mlflow.log_param("contamination", contamination_rate)
        mlflow.log_param("algorithm", "IsolationForest")

        logger.info("Training Isolation Forest Anomaly Detector...")
        # Isolation Forest isolates observations by randomly selecting a feature and then randomly selecting a split value
        model = IsolationForest(contamination=contamination_rate, random_state=42)
        model.fit(X)

        # Predict (-1 is an anomaly, 1 is normal)
        data['is_anomaly'] = model.predict(X)
        
        # Log how many anomalies found
        anomaly_count = len(data[data['is_anomaly'] == -1])
        mlflow.log_metric("anomalies_detected", anomaly_count)
        
        logger.info(f"Model trained. Found {anomaly_count} anomalies in the training set.")
        logger.info(f"Anomaly details:\n{data[data['is_anomaly'] == -1]}")

        # <-- NEW: Save the model to MLflow and register it simultaneously
        # 1. Save the model to MLflow (Logging)
        model_info = mlflow.sklearn.log_model(
            sk_model=model, 
            artifact_path="isolation_forest_anomaly_model"
        )
        
        # 2. Register the model explicitly to get the version object
        model_version = mlflow.register_model(
            model_uri=model_info.model_uri,
            name="logistics_anomaly_detector"
        )
        
        # 3. Assign the "champion" alias to this specific version
        client = MlflowClient()
        client.set_registered_model_alias(
            name="logistics_anomaly_detector",
            alias="champion",
            version=model_version.version
        )
        
        logger.info(f"Successfully logged and registered Anomaly Detection model version {model_version.version} as 'champion'.")
        
        
if __name__ == "__main__":
    train_and_log_anomaly_model()



