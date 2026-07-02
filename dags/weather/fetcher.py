import requests

class TomorrowIOFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.tomorrow.io/v4/weather/forecast"

    def get_hourly_forecast(self, location):
        # Implementation to hit the REST API and return JSON dictionary
        pass