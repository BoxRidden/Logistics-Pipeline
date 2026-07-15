import requests

class TomorrowIOFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.tomorrow.io/v4/weather/forecast"

    def get_hourly_forecast(self, location):
        """
        Returns a mocked JSON payload structure to bypass the live API 
        and trigger the Parquet loader to build the local Bronze folders.
        """
        print(f"Bypassing live API. Generating mocked weather data for {location}...")
        
        mock_payload = [
            {
                "hub_city": location,
                "captured_at": "2026-07-15T08:00:00",
                "temperature_2m": 32.5,
                "precipitation": 0.0,
                "weather_code": 1
            },
            {
                "hub_city": location,
                "captured_at": "2026-07-15T09:00:00",
                "temperature_2m": 33.1,
                "precipitation": 0.0,
                "weather_code": 1
            }
        ]
        
        return mock_payload