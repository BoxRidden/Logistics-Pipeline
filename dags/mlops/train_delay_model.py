import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.client import MlflowClient  
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import logging

logger = logging.getLogger(__name__)

def train_and_log_model():
    # Connect to the MLflow server 
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("Shipment_Delay_Prediction")

    logger.info("Fetching Gold data from BigQuery...")
    # Dummy dataset mimicking the schema:
    data = pd.DataFrame({
        'revenue_usd': [120, 45, 500, 80, 210, 60],
        'item_quantity': [5, 1, 12, 2, 8, 1],
        'temperature_2m': [35.5, 22.0, -5.0, 15.0, 40.0, 18.0],
        'is_delayed': [1, 0, 1, 0, 1, 0] # 1 = Delayed, 0 = On Time
    })

    X = data[['revenue_usd', 'item_quantity', 'temperature_2m']]
    y = data['is_delayed']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Start the MLflow tracking run
    with mlflow.start_run():
        n_estimators = 100
        max_depth = 5
        
        # Hyper-parameters log
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)

        logger.info("Training Random Forest Classifier...")
        clf = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=42)
        clf.fit(X_train, y_train)

        # Evaluate the model
        predictions = clf.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        
        # Log the accuracy metric
        mlflow.log_metric("accuracy", accuracy)
        logger.info(f"Model trained with accuracy: {accuracy}") 

        # 1. Save the model to MLflow (Logging)
        model_info = mlflow.sklearn.log_model(
            sk_model=clf, 
            artifact_path="random_forest_model"
        )
        
        # 2. Register the model explicitly to get the version object
        model_version = mlflow.register_model(
            model_uri=model_info.model_uri,
            name="logistics_delay_predictor"
        )
        
        # 3. Promote this new version to the 'champion' alias
        client = MlflowClient()
        client.set_registered_model_alias(
            name="logistics_delay_predictor",
            alias="champion",
            version=model_version.version
        )
        
        logger.info(f"Successfully logged and registered Delay Prediction model version {model_version.version} as 'champion'.")

if __name__ == "__main__":
    train_and_log_model() 