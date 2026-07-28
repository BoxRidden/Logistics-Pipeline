import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.client import MlflowClient  
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

def train_and_log_model():
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("Shipment_Delay_Prediction")

    logger.info("Fetching Gold data from BigQuery...")
    client = bigquery.Client(project="logistics-500519")
    # Label delays based on the actual shipment status
    query = """
        SELECT 
            revenue_usd, 
            item_quantity, 
            temperature_celsius AS temperature_2m,
            CASE WHEN shipment_status = 'Delayed' THEN 1 ELSE 0 END AS is_delayed
        FROM `logistics-500519.logistics_mart.fact_shipment_weather`
        WHERE revenue_usd IS NOT NULL AND temperature_celsius IS NOT NULL
    """
    data = client.query(query).to_dataframe()

    if data.empty or len(data) < 10:
        logger.warning("Insufficient data to train Delay model. Aborting.")
        return

    X = data[['revenue_usd', 'item_quantity', 'temperature_2m']]
    y = data['is_delayed']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        n_estimators = 100
        max_depth = 5
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)

        logger.info("Training Random Forest Classifier on live data...")
        clf = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=42)
        clf.fit(X_train, y_train)

        predictions = clf.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        mlflow.log_metric("accuracy", accuracy)

        # Save and register the model
        model_info = mlflow.sklearn.log_model(sk_model=clf, artifact_path="random_forest_model")
        model_version = mlflow.register_model(model_uri=model_info.model_uri, name="logistics_delay_predictor")
        
        client = MlflowClient()
        client.set_registered_model_alias("logistics_delay_predictor", "champion", model_version.version)
        logger.info("Successfully registered Delay model as 'champion'.")

if __name__ == "__main__":
    train_and_log_model()