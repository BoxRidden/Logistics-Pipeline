import logging
import requests
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class TomorrowIOFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        # Using the realtime endpoint for current conditions
        self.base_url = "https://api.tomorrow.io/v4/weather/realtime"

    def get_hourly_forecast(self, location):
        logger.info(f"Fetching live realtime weather for {location}...")
        
        params = {
            "location": location,
            "apikey": self.api_key,
            "units": "metric"
        }
        
        try:
            # 10-second timeout prevents Airflow tasks from hanging forever 
            response = requests.get(self.base_url, params=params, timeout=10)
            
            # Fails the Airflow task immediately if there's a 401 Unauthorized or 500 Server Error
            response.raise_for_status() 
            
            raw_data = response.json()
            values = raw_data.get("data", {}).get("values", {})
            
            # Format the live data to match the downstream Parquet schema,
            # while preserving the raw API response for Medallion Bronze standards.
            payload = [{
                "hub_city": location,
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "temperature_2m": values.get("temperature", 0.0),
                "precipitation": values.get("precipitationIntensity", 0.0),
                "weather_code": values.get("weatherCode", 1000),
                "raw_json": json.dumps(raw_data, ensure_ascii=False)
            }]
            
            logger.info(f"Successfully fetched and serialized weather payload for {location}.")
            return payload
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {location}: {e}")
            raise e