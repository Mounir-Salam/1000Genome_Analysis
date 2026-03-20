# src/db_connection/connectors/bigquery.py
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from ..base import BaseConnector

class BigQueryConnector(BaseConnector):
    def __init__(self, config: dict = None):
        self.config = config
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

    def execute_query(self, query: str):
        client = self.connect({}) # In practice, you'd cache this connection
        query_job = client.query(query)
        return query_job.to_dataframe()