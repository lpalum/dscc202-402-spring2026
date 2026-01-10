# Databricks notebook source
# MAGIC %md
# MAGIC # Sentiment Model Performance Analysis
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Evaluate ML model performance using classification metrics
# MAGIC - Generate confusion matrices for binary classification
# MAGIC - Log metrics and artifacts to MLflow experiments
# MAGIC - Track model and data versions for reproducibility
# MAGIC
# MAGIC ## Business Context
# MAGIC This notebook analyzes the performance of the sentiment classification model
# MAGIC by comparing predicted sentiment against ground truth labels from the gold table.
# MAGIC
# MAGIC ## Evaluation Metrics
# MAGIC We'll compute standard classification metrics:
# MAGIC - **Accuracy**: Overall correctness (correct predictions / total predictions)
# MAGIC - **Precision**: Of predicted positives, how many were actually positive
# MAGIC - **Recall**: Of actual positives, how many did we predict correctly
# MAGIC - **F1-Score**: Harmonic mean of precision and recall
# MAGIC
# MAGIC ## Binary Classification
# MAGIC We treat this as binary classification:
# MAGIC - Class 0: Negative sentiment
# MAGIC - Class 1: Positive or neutral sentiment (combined)
# MAGIC
# MAGIC ## MLflow Tracking
# MAGIC We'll log all results to MLflow for:
# MAGIC - Version control (which model version produced these results?)
# MAGIC - Data lineage (which Delta table version was used?)
# MAGIC - Reproducibility (can we reproduce these exact results?)
# MAGIC
# MAGIC **Note**: This notebook is not executed as part of the pipeline.
# MAGIC Run it manually after the pipeline completes to analyze model performance.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

import pandas as pd
import FILL_IN
from mlflow.tracking import MlflowClient
from delta.tables import DeltaTable

import matplotlib.pyplot as plt
from sklearn.metrics import (
    FILL_IN,
    FILL_IN,
    FILL_IN
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Load Gold Data
# MAGIC
# MAGIC Read the tweets_gold table to get predicted and actual sentiments.
# MAGIC We need the binary sentiment IDs for classification metrics.

# COMMAND ----------

df_gold = spark.read.format(FILL_IN).table(FILL_IN)

print(f"Gold table rows: {df_gold.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Generate Classification Report
# MAGIC
# MAGIC Convert to pandas and compute precision, recall, F1-score for each class.

# COMMAND ----------

tmp = df_gold.toPandas()
y_true = tmp.FILL_IN.values
y_pred = tmp.FILL_IN.values

target_names = [FILL_IN, FILL_IN]

cr = classification_report(FILL_IN, FILL_IN, target_names=FILL_IN, output_dict=FILL_IN)

print(cr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Create Confusion Matrix
# MAGIC
# MAGIC Visualize model performance with a confusion matrix.
# MAGIC
# MAGIC **Confusion Matrix Layout**:
# MAGIC ```
# MAGIC                Predicted
# MAGIC              Neg    Pos
# MAGIC Actual  Neg   TN     FP
# MAGIC        Pos   FN     TP
# MAGIC ```
# MAGIC - TN (True Negative): Correctly predicted negative
# MAGIC - FP (False Positive): Predicted positive, actually negative
# MAGIC - FN (False Negative): Predicted negative, actually positive
# MAGIC - TP (True Positive): Correctly predicted positive

# COMMAND ----------

cm = FILL_IN(FILL_IN, FILL_IN)

disp = FILL_IN(
    confusion_matrix=FILL_IN,
    display_labels=FILL_IN
)

disp.plot()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Log Results to MLflow
# MAGIC
# MAGIC Track model performance metrics and artifacts in an MLflow experiment.
# MAGIC
# MAGIC **What we'll log**:
# MAGIC - **Metrics**: Accuracy (overall correctness percentage)
# MAGIC - **Parameters**: Model name, model version, data version (for reproducibility)
# MAGIC - **Artifacts**: Confusion matrix image
# MAGIC
# MAGIC **Why MLflow?**:
# MAGIC - Version control: Track which model produced these results
# MAGIC - Data lineage: Track which data version was used
# MAGIC - Reproducibility: Reproduce exact results later
# MAGIC - Comparison: Compare multiple model versions

# COMMAND ----------

mlflow.set_registry_uri(FILL_IN)

client = MlflowClient()
prod_version = FILL_IN

table_name = FILL_IN
deltaTable = DeltaTable.forPath(spark, table_name)

history_df = deltaTable.history() \
    .select(FILL_IN) \
    .orderBy(FILL_IN, ascending=False)

silver_delta_version = history_df.collect()[0][0]

with mlflow.start_run():
    mlflow.log_metric(FILL_IN, cr[FILL_IN])
    mlflow.log_param(FILL_IN, FILL_IN)
    mlflow.log_param(FILL_IN, FILL_IN)
    mlflow.log_param(FILL_IN, FILL_IN)
    mlflow.log_figure(FILL_IN, FILL_IN)

print("✅ Metrics logged to MLflow!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Check that the MLflow experiment contains:
# MAGIC - accuracy metric (e.g., 0.85 = 85% correct)
# MAGIC - model_name parameter (workspace.default.tweet_sentiment_model)
# MAGIC - model_version parameter (1)
# MAGIC - silver_delta_version parameter (Delta table version number)
# MAGIC - confusion_matrix.png artifact (visualization image)
# MAGIC
# MAGIC **How to view results**:
# MAGIC 1. Navigate to the "Experiments" tab in Databricks
# MAGIC 2. Find the experiment for this notebook
# MAGIC 3. Click on the latest run
# MAGIC 4. View metrics, parameters, and artifacts
# MAGIC
# MAGIC **Interpreting Results**:
# MAGIC - **High accuracy** (>80%): Model is performing well overall
# MAGIC - **Confusion matrix diagonal** (TN, TP): Correct predictions
# MAGIC - **Off-diagonal** (FP, FN): Misclassifications to investigate
# MAGIC - **Imbalanced matrix**: May indicate class imbalance or bias
# MAGIC
# MAGIC **Next Steps**:
# MAGIC - If accuracy is low (<70%), consider:
# MAGIC   - Using a different model
# MAGIC   - Fine-tuning the model on tweet data
# MAGIC   - Improving text preprocessing
# MAGIC - If confusion matrix shows bias (many FP or FN), investigate:
# MAGIC   - Class distribution in training data
# MAGIC   - Model confidence thresholds
# MAGIC   - Text cleaning quality
