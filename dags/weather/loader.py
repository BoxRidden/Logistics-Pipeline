import os
import logging
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class ParquetLoader:
    def __init__(self, destination_path: str):
        """
        Production loader that writes directly to Google Cloud Storage.
        Relies on GOOGLE_APPLICATION_CREDENTIALS being set in the environment.
        """
        self.destination_path = destination_path
        if not self.destination_path.endswith('/'):
            self.destination_path += '/'

    def save_as_parquet(self, data: list, filename_prefix: str) -> str:
        if not data:
            logger.warning("No data provided to ParquetLoader. Skipping.")
            return None
        
        # Convert raw dictionary list to DataFrame
        df = pd.DataFrame(data) 
        
        # Hive Partitioning for performance
        now = datetime.now(timezone.utc)
        hive_partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        
        # Generate unique timestamped filename
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        file_name = f"{filename_prefix}_{timestamp}.parquet"
        
        full_gcs_path = f"{self.destination_path}{hive_partition}{file_name}"
        
        try:
            # Write directly to GCS via Pandas and gcsfs
            logger.info(f"Uploading Parquet file to GCP: {full_gcs_path}")
            df.to_parquet(full_gcs_path, compression='snappy', index=False)
            
            logger.info(f"SUCCESS: Weather payload serialized and uploaded to {full_gcs_path}")
            return full_gcs_path
            
        except Exception as e:
            logger.error(f"FAILED to upload Parquet file to GCS: {e}")
            raise e