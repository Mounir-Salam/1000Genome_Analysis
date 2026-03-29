import os
import traceback
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, upper, when, first
from pyspark.sql.window import Window

from src.config import settings
from src.config.manager import ResourceManager # switchboard
from src.scripts.load_to_storage import download_file
from src.utils.spark_utils import get_spark_session

SEQUENCE_INDEX_URL = "https://ftp-trace.ncbi.nih.gov/1000genomes/ftp/sequence.index"
GCS_CONNECTOR_VERSION = settings.GCS_CONNECTOR_VERSION
SCHEMA = StructType([
    StructField('FASTQ_FILE', StringType(), True), 
    StructField('MD5', StringType(), True), 
    StructField('RUN_ID', StringType(), True), 
    StructField('STUDY_ID', StringType(), True), 
    StructField('STUDY_NAME', StringType(), True), 
    StructField('CENTER_NAME', StringType(), True), 
    StructField('SUBMISSION_ID', StringType(), True), 
    StructField('SUBMISSION_DATE', TimestampType(), True), 
    StructField('SAMPLE_ID', StringType(), True), 
    StructField('SAMPLE_NAME', StringType(), True), 
    StructField('POPULATION', StringType(), True), 
    StructField('EXPERIMENT_ID', StringType(), True), 
    StructField('INSTRUMENT_PLATFORM', StringType(), True), 
    StructField('INSTRUMENT_MODEL', StringType(), True), 
    StructField('LIBRARY_NAME', StringType(), True), 
    StructField('RUN_NAME', StringType(), True), 
    StructField('RUN_BLOCK_NAME', StringType(), True), 
    StructField('INSERT_SIZE', IntegerType(), True), 
    StructField('LIBRARY_LAYOUT', StringType(), True), 
    StructField('PAIRED_FASTQ', StringType(), True), 
    StructField('WITHDRAWN', IntegerType(), True), 
    StructField('WITHDRAWN_DATE', TimestampType(), True), 
    StructField('COMMENT', StringType(), True), 
    StructField('READ_COUNT', IntegerType(), True), 
    StructField('BASE_COUNT', IntegerType(), True), 
    StructField('ANALYSIS_GROUP', StringType(), True)
])

storage = ResourceManager.get_main_storage()
database = ResourceManager.get_main_db()
spark = get_spark_session(storage)
input_path = storage.get_abs_path("raw/sequence.index")

if storage.exists("raw/sequence.index"):
    print("Sequence index detected, skipping download...")
else:
    print(f"Sequence index missing, downloading to {input_path}")
    download_file("raw/sequence.index", SEQUENCE_INDEX_URL)
    
sequence = spark.read \
    .option("header", "true") \
    .schema(SCHEMA) \
    .csv(input_path, sep="\t")
    
# set columns to lower case
sequence = sequence.select([col(c).alias(c.lower()) for c in sequence.columns])

window_spec = Window.partitionBy("submission_id") \
    .orderBy("submission_id") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Set missing md5 to null (currently looking like this .........................)
sequence = sequence \
    .withColumn("md5", when(col("md5").rlike(r"^\.+$"), None) .otherwise(col("md5"))) \
    .withColumn("center_name", upper(col("center_name"))) \
    .withColumn("submission_date", first("submission_date", ignorenulls=True).over(window_spec))

sequence_df = sequence.toPandas()
database.load_data(sequence_df, "sequence_raw")

spark.stop()