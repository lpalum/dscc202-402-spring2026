# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Tweet Ingestion
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Use Spark Declarative Pipelines with CloudFiles for incremental ingestion
# MAGIC - Define explicit schemas for streaming data sources
# MAGIC - Add metadata columns for data lineage tracking
# MAGIC - Apply the Bronze layer pattern of the Medallion Architecture
# MAGIC
# MAGIC ## Business Context
# MAGIC The Bronze layer ingests raw JSON tweet files from the S3 bucket `s3://dsas-raw-tweets` using CloudFiles Auto Loader.
# MAGIC This layer preserves the original data structure and adds metadata for tracking source files and processing timestamps.
# MAGIC
# MAGIC ## Data Schema
# MAGIC **Source JSON Structure**:
# MAGIC - `date` (string): Tweet timestamp in Twitter format (EEE MMM dd HH:mm:ss zzz yyyy)
# MAGIC - `user` (string): Twitter username
# MAGIC - `text` (string): Tweet content
# MAGIC - `sentiment` (string): Original sentiment label (negative/neutral/positive)
# MAGIC
# MAGIC **Additional Columns to Add**:
# MAGIC - `source_file` (string): Path to the source JSON file (from _metadata.file_path)
# MAGIC - `processing_time` (timestamp): When this record was processed

# COMMAND ----------

from pyspark import FILL_IN as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Enable legacy time parser for Twitter datetime format (EEE MMM dd HH:mm:ss zzz yyyy)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create Bronze Streaming Table
# MAGIC
# MAGIC Use `dp.create_streaming_table()` to define the target table for raw tweet data.
# MAGIC This table will store ingested tweets as they arrive from the S3 bucket.

# COMMAND ----------

dp.create_streaming_table(FILL_IN, comment=FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Define Tweet Schema
# MAGIC
# MAGIC CloudFiles requires an explicit schema for JSON ingestion to ensure data quality.
# MAGIC Define a StructType with the four fields from the source JSON: date, user, text, sentiment.

# COMMAND ----------

tweet_schema = StructType([
  StructField(FILL_IN, FILL_IN, True),
  StructField(FILL_IN, FILL_IN, True),
  StructField(FILL_IN, FILL_IN, True),
  StructField(FILL_IN, FILL_IN, True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Define Ingestion Flow
# MAGIC
# MAGIC Create an `@dp.append_flow` that:
# MAGIC 1. Reads streaming data using CloudFiles (format: "cloudFiles")
# MAGIC 2. Specifies JSON as the file format
# MAGIC 3. Configures schema checkpoint location for CloudFiles metadata
# MAGIC 4. Applies the tweet schema
# MAGIC 5. Loads data from the S3 bucket: s3://dsas-raw-tweets/variable_partitions/
# MAGIC 6. Adds metadata columns: source_file (from _metadata.file_path) and processing_time (current timestamp)
# MAGIC
# MAGIC **Important Configuration**:
# MAGIC - CloudFiles format: "cloudFiles"
# MAGIC - Data format: "json"
# MAGIC - Schema checkpoint: "/Volumes/workspace/default/checkpoints/"
# MAGIC - Data source: "s3://dsas-raw-tweets/variable_partitions/"

# COMMAND ----------

@dp.append_flow(target = FILL_IN, name = FILL_IN)
def tweets_raw_ingest():
  return (
    spark.readStream
      .format(FILL_IN)
      .option("cloudFiles.format", FILL_IN)
      .option("cloudFiles.schemaLocation", FILL_IN)
      .schema(FILL_IN)
      .load(FILL_IN)
      .select("*",
              col(FILL_IN).alias("source_file"),
              FILL_IN.alias("processing_time"))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After the pipeline runs, the bronze table should contain:
# MAGIC - All fields from source JSON (date, user, text, sentiment)
# MAGIC - source_file column with the JSON file path from S3
# MAGIC - processing_time timestamp showing when each record was ingested
# MAGIC
# MAGIC Expected behavior:
# MAGIC - CloudFiles incrementally processes new JSON files as they arrive
# MAGIC - Schema is enforced to ensure data quality
# MAGIC - Metadata enables data lineage tracking
