# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: ML Inference for Sentiment Prediction
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Load ML models from Unity Catalog using MLflow
# MAGIC - Create Spark UDFs for distributed ML inference
# MAGIC - Parse model outputs and map labels to sentiment strings
# MAGIC - Create binary sentiment indicators for classification metrics
# MAGIC - Understand the Gold layer pattern of the Medallion Architecture
# MAGIC
# MAGIC ## Business Context
# MAGIC The Gold layer applies a pre-trained Hugging Face sentiment model to predict sentiment
# MAGIC for each tweet. This enables analysis of predicted sentiment vs. ground truth labels.
# MAGIC
# MAGIC ## ML Model Information
# MAGIC **Model**: twitter-roberta-base-sentiment (cardiffnlp)
# MAGIC **Architecture**: RoBERTa (125M parameters)
# MAGIC **Task**: Text classification (3 classes)
# MAGIC **Input**: Cleaned tweet text
# MAGIC **Output**: Struct with two fields:
# MAGIC - `label` (string): LABEL_0, LABEL_1, or LABEL_2
# MAGIC - `score` (double): Confidence score (0.0 to 1.0)
# MAGIC
# MAGIC ## Label Mapping
# MAGIC The model returns numeric labels that must be mapped to sentiment strings:
# MAGIC - LABEL_0 → "negative"
# MAGIC - LABEL_1 → "neutral"
# MAGIC - LABEL_2 → "positive"
# MAGIC
# MAGIC ## Binary Sentiment IDs
# MAGIC For classification metrics (precision, recall, F1), we create binary indicators:
# MAGIC - 0 = negative sentiment
# MAGIC - 1 = positive or neutral sentiment (combined)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *
import mlflow

# COMMAND ----------

dp.create_streaming_table("tweets_gold", comment="Gold table with sentiment predictions from ML model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Configure MLflow Registry
# MAGIC
# MAGIC Set the MLflow registry to Unity Catalog to load the sentiment model.
# MAGIC This tells MLflow to look for models in Unity Catalog rather than the legacy MLflow registry.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Define Model Output Schema
# MAGIC
# MAGIC Hugging Face transformers models return a struct with two fields:
# MAGIC - `label` (string): The predicted class label (LABEL_0, LABEL_1, or LABEL_2)
# MAGIC - `score` (double): Confidence score between 0.0 and 1.0
# MAGIC
# MAGIC We must define this schema so Spark knows how to parse the model output.

# COMMAND ----------

result_schema = StructType([
    StructField("label", StringType(), True),
    StructField("score", DoubleType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Load Model and Create Spark UDF
# MAGIC
# MAGIC Load the sentiment model from Unity Catalog and create a Spark UDF for distributed inference.
# MAGIC
# MAGIC **Model URI Format**: `models:/{catalog}.{schema}.{model_name}/{version}`
# MAGIC
# MAGIC The model was registered as:
# MAGIC - Catalog: workspace
# MAGIC - Schema: default
# MAGIC - Model name: tweet_sentiment_model
# MAGIC - Version: 1
# MAGIC
# MAGIC **Why Spark UDF?**: This enables distributed ML inference - the model runs in parallel across all Spark executors,
# MAGIC allowing us to process millions of tweets efficiently.

# COMMAND ----------

MODEL_URI = "models:/workspace.default.tweet_sentiment_model/1"

sentiment_model_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=MODEL_URI,
    result_type=result_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Define Gold Transformation Flow
# MAGIC
# MAGIC Create the transformation that:
# MAGIC 1. Reads from tweets_silver streaming table
# MAGIC 2. Applies model UDF to cleaned_text column
# MAGIC 3. Parses model output struct to extract label and score
# MAGIC 4. Maps LABEL_0/1/2 to negative/neutral/positive strings
# MAGIC 5. Converts score from 0-1 scale to 0-100 scale (percentage)
# MAGIC 6. Creates binary sentiment indicators for classification metrics
# MAGIC    - sentiment_id: binary version of ground truth (0=negative, 1=positive/neutral)
# MAGIC    - predicted_sentiment_id: binary version of prediction (0=negative, 1=positive/neutral)
# MAGIC 7. Selects final columns for gold table

# COMMAND ----------

@dp.append_flow(target = "tweets_gold", name = "gold_transformation")
def tweets_gold_transform():
  df = spark.readStream.table("tweets_silver")

  return (
     df
      .withColumn("model_output", sentiment_model_udf(col("cleaned_text")))
      .withColumn("predicted_sentiment_label", col("model_output.label"))
      .withColumn("score_percentage", col("model_output.score") * 100)
      .withColumn("predicted_sentiment",
                  when(col("predicted_sentiment_label") == "LABEL_0", "negative")
                  .when(col("predicted_sentiment_label") == "LABEL_1", "neutral")
                  .otherwise("positive"))
      .withColumn("sentiment_id",
                  when(col("sentiment") == "negative", 0)
                  .when(col("sentiment") == "positive", 1)
                  .otherwise(1))
      .withColumn("predicted_sentiment_id",
                  when(col("predicted_sentiment") == "negative", 0)
                  .when(col("predicted_sentiment") == "positive", 1)
                  .otherwise(1))
      .select("id", "created_at", "text", "cleaned_text", "sentiment",
              "sentiment_id", "predicted_sentiment", "predicted_sentiment_id", "score_percentage")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After the pipeline runs, verify:
# MAGIC - predicted_score is between 0-100 (converted from 0-1)
# MAGIC - predicted_sentiment is "negative", "neutral", or "positive" (mapped from LABEL_0/1/2)
# MAGIC - sentiment_id is 0 or 1 (binary ground truth)
# MAGIC - predicted_sentiment_id is 0 or 1 (binary prediction)
# MAGIC - Row count matches silver table (no rows lost during transformation)
# MAGIC
# MAGIC **Example Data Flow**:
# MAGIC - Silver: cleaned_text = "This product is amazing!"
# MAGIC - Model Output: {label: "LABEL_2", score: 0.95}
# MAGIC - Gold: predicted_sentiment = "positive", predicted_score = 95.0, predicted_sentiment_id = 1
