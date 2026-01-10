# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Tweet Preprocessing and Mention Extraction
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Create Python UDFs for custom text processing logic
# MAGIC - Use regular expressions for text cleaning and pattern matching
# MAGIC - Apply explode_outer() to create one row per mention
# MAGIC - Parse Twitter date strings to timestamps
# MAGIC - Understand the Silver layer pattern of the Medallion Architecture
# MAGIC
# MAGIC ## Business Context
# MAGIC The Silver layer transforms raw tweets into analysis-ready data:
# MAGIC 1. Extract @mentions from tweet text using regex patterns
# MAGIC 2. Clean tweet text by removing @mentions
# MAGIC 3. Explode mentions array so each mention gets its own row
# MAGIC 4. Convert Twitter date strings to proper timestamp format
# MAGIC 5. Normalize mentions to lowercase for consistency
# MAGIC
# MAGIC ## Data Transformation
# MAGIC **Input (Bronze)**: Raw tweets with original text
# MAGIC **Output (Silver)**: One row per mention with cleaned text and parsed timestamp
# MAGIC
# MAGIC ## Why Explode Mentions?
# MAGIC A single tweet like "@user1 and @user2 are great!" should create TWO rows:
# MAGIC - Row 1: mention=@user1, cleaned_text="and are great!"
# MAGIC - Row 2: mention=@user2, cleaned_text="and are great!"
# MAGIC
# MAGIC This enables analysis of sentiment toward each mentioned user separately.

# COMMAND ----------

from pyspark import FILL_IN as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *
import FILL_IN

# COMMAND ----------

dp.create_streaming_table(FILL_IN, comment=FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Define Mention Extraction UDF
# MAGIC
# MAGIC Create a Python function that extracts all @mentions from tweet text.
# MAGIC
# MAGIC **Regex Pattern**: `r"@[\w]+"` matches @ followed by one or more word characters (letters, digits, underscore)
# MAGIC
# MAGIC **Example**:
# MAGIC - Input: "@elonmusk and @twitter are trending"
# MAGIC - Output: ["@elonmusk", "@twitter"]
# MAGIC
# MAGIC **Why UDF?**: While Spark has built-in functions, UDFs let us implement custom logic in Python.

# COMMAND ----------

def find_mentions(text):
    pattern = FILL_IN
    return re.findall(FILL_IN, FILL_IN)

find_mentions_udf = udf(FILL_IN, ArrayType(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Define Text Cleaning Pattern
# MAGIC
# MAGIC Define a regex pattern to remove @mentions from tweet text.
# MAGIC We'll use this with regexp_replace() to clean the text.
# MAGIC
# MAGIC **Pattern**: `"@\\S+"` matches @ followed by any non-whitespace characters
# MAGIC (In Python strings, \\ is needed to escape the backslash)

# COMMAND ----------

pattern = FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Define Silver Transformation Flow
# MAGIC
# MAGIC Create the transformation logic that:
# MAGIC 1. Reads from tweets_bronze streaming table
# MAGIC 2. Cleans text by removing @mentions using regexp_replace()
# MAGIC 3. Extracts mentions using the UDF
# MAGIC 4. Explodes mentions array to create one row per mention (use explode_outer to keep tweets with no mentions)
# MAGIC 5. Converts mentions to lowercase for consistency
# MAGIC 6. Parses Twitter date string to timestamp format
# MAGIC 7. Selects final columns: timestamp, mention, cleaned_text, text, sentiment

# COMMAND ----------

@dp.append_flow(target = FILL_IN, name = FILL_IN)
def tweets_silver_transform():
  df = spark.readStream.table(FILL_IN)

  return (
    df
    .withColumn(FILL_IN, regexp_replace(FILL_IN, FILL_IN, ""))
    .withColumn(FILL_IN, FILL_IN(col(FILL_IN)))
    .withColumn("mention_", FILL_IN(FILL_IN))
    .withColumn(FILL_IN, lower(col(FILL_IN)))
    .withColumn(FILL_IN, to_timestamp(FILL_IN, FILL_IN))
    .select([FILL_IN, FILL_IN, FILL_IN, FILL_IN, FILL_IN])
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After the pipeline runs, verify:
# MAGIC - Row count > Bronze (due to mention explosion - one tweet with multiple mentions creates multiple rows)
# MAGIC - cleaned_text has no @mentions (they've been removed)
# MAGIC - mention column contains only lowercase @usernames
# MAGIC - timestamp is properly parsed as TimestampType (not string)
# MAGIC - Tweets with no mentions still appear with mention=NULL (explode_outer preserves them)
# MAGIC
# MAGIC **Example Data Flow**:
# MAGIC - Bronze: 1 row → "@user1 and @user2 are amazing!"
# MAGIC - Silver: 2 rows →
# MAGIC   - Row 1: mention=@user1, cleaned_text="and are amazing!"
# MAGIC   - Row 2: mention=@user2, cleaned_text="and are amazing!"
