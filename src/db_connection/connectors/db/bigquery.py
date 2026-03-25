# src/db_connection/connectors/bigquery.py
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from ..base import BaseConnector

class BigQueryConnector(BaseConnector):
    def __init__(self, config: dict = None):
        self.config = config
        self.client = self.connect(config)
        # self._ensure_dataset_exists()
    def connect(self, config: dict):
        """
        Logic: 
        1. Check if a path is provided in the YAML config.
        2. If not, check the GOOGLE_APPLICATION_CREDENTIALS env var.
        3. Otherwise, try Default Application Credentials (ADC).
        """
        key_path = config.get("credentials_path") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        project_id = config.get("project_id")

        if key_path and os.path.exists(key_path):
            print(f"Using service account key at: {key_path}")
            return bigquery.Client.from_service_account_json(key_path, project=project_id)
        
        print("Using default Google Application Credentials (ADC)")
        return bigquery.Client(project=project_id)

    # def _ensure_dataset_exists(self):
    #     """Creates the dataset if it doesn't exist in GCP."""
    #     dataset_id = self.config.get("default_schema")
    #     if not dataset_id:
    #         return

    #     full_dataset_id = f"{self.client.project}.{dataset_id}"
        
    #     try:
    #         self.client.get_dataset(full_dataset_id)
    #         print(f"✅ Dataset {dataset_id} already exists.")
    #     except NotFound:
    #         print(f"✨ Dataset {dataset_id} not found. Creating it now...")
    #         dataset = bigquery.Dataset(full_dataset_id)
    #         dataset.location = "US"  # Or "EU" depending on your preference
    #         self.client.create_dataset(dataset, timeout=30)
    #         print(f"✅ Dataset {dataset_id} created successfully.")
    
    def execute_query(self, query: str):
        client = self.connect({}) # In practice, you'd cache this connection
        query_job = client.query(query)
        return query_job.to_dataframe()

    def get_data(self, query: str):
        """Executes query and returns DataFrame."""
        return self.client.query(query).to_dataframe()

    def load_data(self, df, table_name: str, schema: str = None, if_exists: str = "append"):
        """Loads a dataframe into BigQuery."""
        # BigQuery uses project.dataset.table format
        dataset_id = schema or self.config.get("default_schema")
        if not dataset_id:
            raise ValueError("No dataset/schema specified for BigQuery.")
        table_id = f"{self.client.project}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND" if if_exists == "append" else "WRITE_TRUNCATE"
        )
        
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() # Wait for the load to finish
        print(f"✅ Loaded {len(df)} rows to BigQuery table: {table_id}")