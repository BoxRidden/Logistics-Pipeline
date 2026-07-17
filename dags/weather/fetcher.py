import requests
from datetime import datetime, timezone

class TomorrowIOFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        # Using the realtime endpoint for current conditions
        self.base_url = "https://api.tomorrow.io/v4/weather/realtime"

    def get_hourly_forecast(self, location):
        print(f"Fetching live realtime weather for {location}...")
        
        params = {
            "location": location,
            "apikey": self.api_key,
            "units": "metric"
        }
        
        # 10-second timeout prevents Airflow tasks from hanging forever
        response = requests.get(self.base_url, params=params, timeout=10)
        
        # Fails the Airflow task immediately if we get a 401 Unauthorized or 500 Server Error
        response.raise_for_status() 
        
        raw_data = response.json()
        values = raw_data["data"]["values"]
        
        # Format the live data to match your downstream Parquet schema
        payload = [{
            "hub_city": location,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "temperature_2m": values.get("temperature", 0.0),
            "precipitation": values.get("precipitationIntensity", 0.0),
            "weather_code": values.get("weatherCode", 1000)
        }]
        
        return payload