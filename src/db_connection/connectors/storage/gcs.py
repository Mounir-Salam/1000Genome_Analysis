# src/connectors/storage/gcs.py
from google.cloud import storage
import os

class GCSConnector:
    def __init__(self, config: dict):
        self.config = config
        # Store the bucket name from the YAML for easy access later
        self.default_bucket = config.get("bucket_name")
        self.client = self.connect(config)
    
    def connect(self, config: dict):
        key_path = config.get("credentials_path") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        if key_path:
            return storage.Client.from_service_account_json(key_path)
        return storage.Client()

    def get_bucket(self, bucket_name=None):
        """Returns the raw Google Bucket object."""
        name = bucket_name or self.default_bucket
        return self.client.bucket(name)
    
    # Genenal use blob method
    def get_blob(self, blob_name, bucket_name=None):
        # 1. Use the name passed to the function (if provided)
        # 2. Otherwise, fall back to the name from databases.yaml
        target_bucket_name = bucket_name or self.default_bucket
        
        if not target_bucket_name:
            raise ValueError("No bucket name provided and no default bucket found in config.")

        bucket = self.client.bucket(target_bucket_name)
        return bucket.blob(blob_name)
    
    def list_blobs(self, prefix=None):
        """
        Lists all files in the default bucket.
        Use 'prefix' to simulate looking inside a folder.
        """
        bucket = self.client.bucket(self.default_bucket)
        
        # prefix="raw/" will only return blobs starting with "raw/"
        blobs = self.client.list_blobs(bucket, prefix=prefix)
        
        # We return a list of names (strings) for easy use in ETL scripts
        return [blob.name for blob in blobs]