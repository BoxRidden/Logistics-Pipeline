import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime

class ParquetLoader:
    def __init__(self, destination_path: str):
        """
        Note: The destination_path should point to a local directory 
        during this phase, as native GCS upload requires Google Cloud Storage 
        client libraries which are currently bypassed in the architecture.
        """
        # Fallback to local lakehouse if an external GCS path is passed
        if destination_path.startswith("gs://"):
            self.destination_path = "/opt/airflow/lakehouse/bronze/weather/"
        else:
            self.destination_path = destination_path

    def save_as_parquet(self, data: list, filename_prefix: str) -> str:
        """
        Serializes JSON dictionary arrays into Parquet format with Snappy compression.
        """
        if not data:
            print("[WARN] No data provided to ParquetLoader. Skipping.")
            return None

        # Ensure local directory exists
        os.makedirs(self.destination_path, exist_ok=True)
        
        # Convert raw dictionary list to DataFrame
        df = pd.DataFrame(data)
        
        # Generate unique timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{filename_prefix}_{timestamp}.parquet"
        full_path = os.path.join(self.destination_path, file_name)
        
        # Write out optimized Parquet file
        table = pa.Table.from_pandas(df)
        pq.write_table(table, full_path, compression='snappy')
        
        print(f"Weather payload serialized to {full_path}")
        return full_path