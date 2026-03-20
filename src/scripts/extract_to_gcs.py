from pyspark.sql import SparkSession
import pyspark
import os
from dotenv import load_dotenv
import traceback

# load_dotenv()

# spark = SparkSession.builder \
#     .appName("GCP-Connect") \
#     .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
#     .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
#     .config("spark.jars.packages", f"com.google.cloud.bigdataoss:gcs-connector:{os.getenv('GCS_CONNECTOR_VERSION')}") \
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#     .getOrCreate()

# try:
#     df = spark.read.parquet("gs://de-zoomcamp-484617-function_bucket/test.parquet")

#     df.show()
#     print(df.schema.json())
# except Exception as e:
#     traceback.print_exc()

# spark.stop()

import requests
from src.db_connection.builder import IOBuilder

connector = IOBuilder.get_resource("gcs_raw_data")
iterator = connector.client.list_buckets()

for bucket in iterator:
    print(bucket)

print("Current Bucket", connector.get_bucket())

print(f"Current working directory: {os.getcwd()}")