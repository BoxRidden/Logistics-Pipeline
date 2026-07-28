import logging
import psycopg2
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class WeatherMetadataRepo:
    def __init__(self, host, database, user, password):
        try:
            self.conn = psycopg2.connect(host=host, database=database, user=user, password=password)
            self.cursor = self.conn.cursor()
            self._ensure_table_exists()
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise e

    def _ensure_table_exists(self):
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_api_logs (
                    fetch_id SERIAL PRIMARY KEY, 
                    city VARCHAR(50), 
                    fetch_hour TIMESTAMP, 
                    success BOOLEAN, 
                    gcs_path VARCHAR(255)
                );
            """) 
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to verify or create weather_api_logs table: {e}")
            raise e

    def log_fetch(self, city, gcs_path):
        # Enforce UTC time to prevent server locale inconsistencies
        current_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        
        try:
            self.cursor.execute("""
                INSERT INTO weather_api_logs (city, fetch_hour, success, gcs_path)
                VALUES (%s, %s, True, %s)
            """, (city, current_hour, gcs_path))
            self.conn.commit()
            logger.info(f"Database audit log recorded for {city} weather fetch.")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to log weather fetch for {city}: {e}")
            raise e

    def close(self):
        """Safely close database resources to prevent connection pool exhaustion."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Weather metadata repository database connection closed.")