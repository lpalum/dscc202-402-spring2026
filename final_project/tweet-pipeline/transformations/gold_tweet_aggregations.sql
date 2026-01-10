-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Application Layer: Tweet Sentiment Aggregations
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC - Create materialized views in Spark Declarative Pipelines
-- MAGIC - Use SQL aggregation functions (COUNT, MIN, MAX)
-- MAGIC - Apply filtered aggregations with COUNT(*) FILTER (WHERE ...)
-- MAGIC - Group and sort data for analytics
-- MAGIC - Understand the Application layer pattern of the Medallion Architecture
-- MAGIC
-- MAGIC ## Business Context
-- MAGIC The Application layer aggregates sentiment data by mentioned user (@username).
-- MAGIC This produces analytics-ready data showing:
-- MAGIC - How many positive mentions each user received
-- MAGIC - How many negative mentions each user received
-- MAGIC - Total mentions (positive + negative, excluding neutral)
-- MAGIC - Time range of mentions (earliest and latest timestamps)
-- MAGIC
-- MAGIC ## Output Schema
-- MAGIC - `mention`: Twitter username (@username)
-- MAGIC - `positive`: Count of positive sentiment tweets
-- MAGIC - `negative`: Count of negative sentiment tweets
-- MAGIC - `total`: Total mentions (positive + negative only, neutral excluded)
-- MAGIC - `min_timestamp`: Earliest tweet timestamp
-- MAGIC - `max_timestamp`: Latest tweet timestamp
-- MAGIC
-- MAGIC ## Materialized Views
-- MAGIC A materialized view is a pre-computed table that stores query results.
-- MAGIC Benefits:
-- MAGIC - Fast query performance (results are pre-aggregated)
-- MAGIC - Automatic refresh as new data arrives
-- MAGIC - Optimized for dashboard queries
-- MAGIC
-- MAGIC ## Filtered Aggregations
-- MAGIC SQL's FILTER clause allows conditional counting:
-- MAGIC - `COUNT(*) FILTER (WHERE condition)` counts only rows matching the condition
-- MAGIC - More efficient than using CASE WHEN inside COUNT()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task: Create Aggregation Materialized View
-- MAGIC
-- MAGIC Create a materialized view that aggregates sentiment by mention.
-- MAGIC
-- MAGIC **Requirements**:
-- MAGIC 1. Name: gold_tweet_aggregations
-- MAGIC 2. Source: tweets_gold table
-- MAGIC 3. Aggregations:
-- MAGIC    - Count positive mentions using COUNT(*) FILTER (WHERE predicted_sentiment = 'positive')
-- MAGIC    - Count negative mentions using COUNT(*) FILTER (WHERE predicted_sentiment = 'negative')
-- MAGIC    - Count total mentions (positive + negative only) using IN ('positive', 'negative')
-- MAGIC    - Get earliest timestamp with MIN(timestamp)
-- MAGIC    - Get latest timestamp with MAX(timestamp)
-- MAGIC 4. Filter: Exclude NULL mentions (WHERE mention IS NOT NULL)
-- MAGIC 5. Group by: mention
-- MAGIC 6. Sort by: total (descending) to see most-mentioned users first

-- COMMAND ----------

CREATE MATERIALIZED VIEW FILL_IN AS
SELECT
    FILL_IN,
    COUNT(*) FILTER (WHERE FILL_IN = FILL_IN) AS FILL_IN,
    COUNT(*) FILTER (WHERE FILL_IN = FILL_IN) AS FILL_IN,
    COUNT(*) FILTER (WHERE FILL_IN IN (FILL_IN, FILL_IN)) AS FILL_IN,
    FILL_IN(FILL_IN) AS min_timestamp,
    FILL_IN(FILL_IN) AS max_timestamp
FROM FILL_IN
WHERE FILL_IN IS NOT NULL
GROUP BY FILL_IN
ORDER BY FILL_IN DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation
-- MAGIC
-- MAGIC After the pipeline runs, verify:
-- MAGIC - View name is gold_tweet_aggregations
-- MAGIC - Mentions are sorted by total (highest first)
-- MAGIC - positive + negative ≤ total (neutral tweets excluded from total)
-- MAGIC - min_timestamp ≤ max_timestamp for all rows
-- MAGIC - No NULL mentions in output (filtered by WHERE clause)
-- MAGIC
-- MAGIC **Example Output**:
-- MAGIC | mention | positive | negative | total | min_timestamp | max_timestamp |
-- MAGIC |---------|----------|----------|-------|---------------|---------------|
-- MAGIC | @user1  | 150      | 50       | 200   | 2023-01-01    | 2023-12-31    |
-- MAGIC | @user2  | 80       | 70       | 150   | 2023-02-15    | 2023-11-20    |
-- MAGIC
-- MAGIC **Use Cases**:
-- MAGIC - Dashboard: "Top 10 Most Mentioned Users"
-- MAGIC - Dashboard: "Most Positive Users" (highest positive count)
-- MAGIC - Dashboard: "Most Negative Users" (highest negative count)
-- MAGIC - Analytics: Track sentiment trends for specific users over time
