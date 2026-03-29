from pyspark.sql import SparkSession
from src.config import settings

GCS_CONNECTOR_VERSION = settings.GCS_CONNECTOR_VERSION

def get_spark_session(storage_connector):
    builder = SparkSession.builder.appName("1000Genomes-Analysis")
    
    # If using GCS, inject the credentials from our connector
    if storage_connector.__class__.__name__ == "GCSConnector":
        conf = storage_connector.config
        key_path = conf.get("credentials_path")
        
        builder.config("spark.jars.packages", f"com.google.cloud.bigdataoss:gcs-connector:{GCS_CONNECTOR_VERSION}") \
               .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
               .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
               .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    
    return builder.getOrCreate()