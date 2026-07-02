import psycopg2
from datetime import datetime

class WeatherMetadataRepo:
    def __init__(self, host, database, user, password):
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        self.cursor = self.conn.cursor()
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_api_logs (
                fetch_id SERIAL PRIMARY KEY, city VARCHAR(50), 
                fetch_hour TIMESTAMP, success BOOLEAN, gcs_path VARCHAR(255)
            );
        """)
        self.conn.commit()

    def log_fetch(self, city, gcs_path):
        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        self.cursor.execute("""
            INSERT INTO weather_api_logs (city, fetch_hour, success, gcs_path)
            VALUES (%s, %s, True, %s)
        """, (city, current_hour, gcs_path))
        self.conn.commit()