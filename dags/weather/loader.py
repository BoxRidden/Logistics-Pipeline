import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os 
from datetime import datetime

class ParquetLoader:
    def __init__(self, destination_path: str):
        """
        Production loader that writes directly to Google Cloud Storage.
        Relies on GOOGLE_APPLICATION_CREDENTIALS being set in the environment.
        """
        self.destination_path = destination_path

    def save_as_parquet(self, data: list, filename_prefix: str) -> str:
        if not data:
            print("[WARN] No data provided to ParquetLoader. Skipping.")
            return None
        
        # Convert raw dictionary list to DataFrame
        df = pd.DataFrame(data) 
        
        # Generate unique timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{filename_prefix}_{timestamp}.parquet"
        
        if not self.destination_path.endswith('/'):
            self.destination_path += '/'
            
        full_gcs_path = f"{self.destination_path}{file_name}"
        
        # Write directly to GCS via Pandas and gcsfs
        print(f"Uploading Parquet file to GCP: {full_gcs_path}")
        df.to_parquet(full_gcs_path, compression='snappy', index=False)
        
        print(f"SUCCESS: Weather payload serialized and uploaded to {full_gcs_path}")
        return full_gcs_path