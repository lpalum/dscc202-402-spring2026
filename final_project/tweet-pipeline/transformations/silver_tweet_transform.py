# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Tweet Preprocessing and Mention Extraction
# MAGIC
# MAGIC ## Purpose
# MAGIC Clean tweet text and extract @mentions for sentiment analysis.
# MAGIC Create one row per mention to enable per-user sentiment tracking.
# MAGIC
# MAGIC ## Requirements
# MAGIC - Extract @mentions using regex pattern
# MAGIC - Remove @mentions from text (create cleaned_text column)
# MAGIC - Explode mentions array into individual rows
# MAGIC - Parse Twitter date strings to timestamps
# MAGIC - Normalize mentions to lowercase
# MAGIC - Preserve tweets without mentions
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta table: `tweets_silver`
# MAGIC - Row count > bronze (due to mention explosion)
# MAGIC - cleaned_text has no @mentions
# MAGIC - Tweets without mentions have mention=NULL
# MAGIC - Timestamp properly parsed
# MAGIC
# MAGIC ## Reference
# MAGIC - Lab 0.1 Section 9: UDF creation patterns
# MAGIC - Lab 0.1 Section 7: Array explode operations

# COMMAND ----------

# TODO: Import necessary libraries
# You will need:
# - pyspark.pipelines (as dp)
# - pyspark.sql.types and pyspark.sql.functions
# - re module for regex operations


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create Silver Streaming Table
# MAGIC
# MAGIC TODO: Define streaming table "tweets_silver" with descriptive comment

# COMMAND ----------

# TODO: Create streaming table definition


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Define Mention Extraction UDF
# MAGIC
# MAGIC TODO: Create Python function that extracts @mentions from text
# MAGIC - Function name: find_mentions(text)
# MAGIC - Regex pattern: r"@[\w]+"
# MAGIC - Returns: List of @mentions found in text
# MAGIC - Register as Spark UDF with ArrayType(StringType()) return type
# MAGIC
# MAGIC Example: "@user1 and @user2" → ["@user1", "@user2"]

# COMMAND ----------

# TODO: Define find_mentions function and create UDF


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Define Silver Transformation Flow
# MAGIC
# MAGIC TODO: Create @dp.append_flow function that:
# MAGIC 1. Reads from tweets_bronze streaming table
# MAGIC 2. Removes @mentions from text using regexp_replace (pattern: "@\\S+")
# MAGIC 3. Extracts mentions using your UDF
# MAGIC 4. Explodes mentions array (use explode_outer to preserve tweets with no mentions)
# MAGIC 5. Converts mentions to lowercase
# MAGIC 6. Parses date string to timestamp (format: "EEE MMM dd HH:mm:ss zzz yyyy")
# MAGIC 7. Selects final columns: timestamp, mention, cleaned_text, text, sentiment
# MAGIC
# MAGIC Reference: Lab 0.1 for regexp_replace, explode_outer, and to_timestamp patterns

# COMMAND ----------

# TODO: Define append_flow function for silver transformation


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After pipeline execution, verify:
# MAGIC - Row count > bronze layer (mention explosion)
# MAGIC - cleaned_text has no @mentions
# MAGIC - mention column is lowercase
# MAGIC - Tweets without mentions have mention=NULL (not dropped)
# MAGIC - timestamp is TimestampType (not string)
