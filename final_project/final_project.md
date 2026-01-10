# DSCC202-402 Final Project: Tweet Sentiment Analysis at Scale

## Overview

This final project demonstrates your understanding of:
- **Spark Declarative Pipelines** for data pipeline orchestration
- **Medallion Architecture** (Bronze → Silver → Gold → Application layers)
- **ML inference at scale** with Spark UDFs
- **Unity Catalog** for data and model governance
- **MLflow experiment tracking** for reproducibility

**Project Weight**: 25% of final grade

**Data Source**: Pre-provisioned tweets in `s3://dsas-datasets/tweets/`
- ~50,000 tweets in JSON format
- Distributed across multiple partition files
- Ready to ingest with CloudFiles Auto Loader

---

## Learning Objectives

By completing this project, you will:

1. **Implement Spark Declarative Pipelines**: Build streaming data transformations using the pipelines API
2. **Apply Text Processing**: Extract entities (@mentions) and clean text data using UDFs and regex
3. **Deploy ML Models**: Load models from Unity Catalog and apply them at scale using Spark UDFs
4. **Track Model Performance**: Log metrics and artifacts to MLflow for reproducibility
5. **Build Analytics**: Create aggregations for business intelligence dashboards
6. **Create Dashboards**: Design and implement visualizations in Databricks 
7. **Automate Data Workflows**: Configure Databricks Jobs to run pipelines and refresh dashboards daily

---

## Prerequisites

### 1. Databricks Environment
- **Edition**: Databricks workspace (Free Edition)
- **Runtime**: DBR (with Unity Catalog support)
- **Cluster**: Serverless

### 2. GitHub Repository 

**Verify project structure** in your workspace:
   ```
   final_project/
   ├── final_project.md (this file)
   └── tweet-pipeline/
       ├── utilities/
       │   └── Run me first.ipynb
       ├── transformations/
       │   ├── bronze_tweet_ingest.py
       │   ├── silver_tweet_transform.py
       │   ├── gold_tweet_transform.py
       │   └── gold_tweet_aggregations.sql
       └── explorations/
       |    └── Sentiment Model Performance Analysis.py
       └── _dashboards/
           └── 
   ```

### 3. Data Source Configuration

**Tweet Data Location**: `s3://dsas-datasets/tweets/`

**Data Format**: JSON files with schema:
```json
{
  "date": "Mon Apr 06 22:19:49 PDT 2009",
  "user": "username123",
  "text": "@someuser This is a sample tweet!",
  "sentiment": "4"
}
```

**Important Notes**:
- Data is already provisioned - NO download required
- ~50,000 tweets across multiple partition files
- CloudFiles Auto Loader handles incremental ingestion
- Original sentiment labels: negative, positive
- **New data may be added to the S3 bucket** - Your automated job ensures daily processing of any new tweets

---

## Project Architecture

### Pipeline Overview

```
S3 Bucket (s3://dsas-datasets/tweets) ← New data added daily
    ↓
Bronze Layer → Raw ingestion with CloudFiles
    ↓ (tweets_bronze table)
Silver Layer → Extract mentions, clean text
    ↓ (tweets_silver table)
Gold Layer → ML inference with Spark UDF
    ↓ (tweets_gold table)
Application Layer → Aggregated metrics by mention
    ↓ (gold_tweet_aggregations view)
Dashboard + MLflow Tracking
    ↓
Databricks Job (Daily Automated Execution)
    → Task 1: Run Pipeline
    → Task 2: Refresh Dashboard
```

### Medallion Architecture Layers

#### Bronze (Raw Ingestion)
- **Purpose**: Ingest raw JSON data from S3 without transformation
- **Technology**: CloudFiles Auto Loader for incremental processing
- **Schema Enforcement**: Explicit StructType schema validation
- **Metadata**: Add source_file and processing_time columns
- **Output**: `tweets_bronze` streaming table

#### Silver (Preprocessed)
- **Purpose**: Clean and structure data for analysis
- **Transformations**:
  - Extract @mentions using regex UDF
  - Clean tweet text (remove @mentions)
  - Explode mentions (one row per mention)
  - Parse Twitter date strings to timestamps
  - Normalize mentions to lowercase
- **Output**: `tweets_silver` streaming table

#### Gold (ML Predictions)
- **Purpose**: Enrich data with ML model predictions
- **Model**: twitter-roberta-base-sentiment (125M parameters)
- **Inference**: Spark UDF for distributed prediction
- **Features**:
  - Predicted sentiment (negative/neutral/positive)
  - Confidence score (0-100 scale)
  - Binary sentiment IDs for classification metrics
- **Output**: `tweets_gold` streaming table

#### Application (Aggregated)
- **Purpose**: Pre-compute analytics for dashboard queries
- **Aggregations**: Count positive/negative mentions per user
- **Metrics**: Total mentions, timestamp range
- **Output**: `gold_tweet_aggregations` materialized view

---

## Data Schema by Layer

### Bronze Schema
| Column | Type | Description |
|--------|------|-------------|
| `date` | string | Tweet timestamp in Twitter format |
| `user` | string | Twitter username |
| `text` | string | Tweet content |
| `sentiment` | string | Ground truth label |
| `source_file` | string | Source JSON file path |
| `processing_time` | timestamp | Ingestion timestamp |

### Silver Schema
| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | timestamp | Parsed tweet timestamp |
| `mention` | string | Extracted @username (lowercase) |
| `cleaned_text` | string | Text with mentions removed |
| `text` | string | Original tweet text |
| `sentiment` | string | Ground truth label |

### Gold Schema
| Column | Type | Description |
|--------|------|-------------|
| All silver columns | - | - |
| `predicted_score` | double | Model confidence (0-100) |
| `predicted_sentiment` | string | Predicted label |
| `sentiment_id` | int | Binary ground truth (0=neg, 1=pos/neutral) |
| `predicted_sentiment_id` | int | Binary prediction (0=neg, 1=pos/neutral) |

### Application Schema
| Column | Type | Description |
|--------|------|-------------|
| `mention` | string | Twitter username |
| `positive` | int | Count of positive mentions |
| `negative` | int | Count of negative mentions |
| `total` | int | Total mentions (pos + neg only) |
| `min_timestamp` | timestamp | Earliest mention |
| `max_timestamp` | timestamp | Latest mention |

---

## Step-by-Step Implementation Guide

### Phase 1: Setup (10 minutes)

#### Step 1.1: Run Pipeline Setup Utility
1. Open: `tweet-pipeline/utilities/Run me first.ipynb`
2. Click **Run All**
3. **Verify outputs**:
   - ✅ Checkpoint volume created (`/Volumes/workspace/default/checkpoints/`)
   - ✅ Dependencies installed (transformers, torch, etc)
   - ✅ Model registered to Unity Catalog (`workspace.default.tweet_sentiment_model`)

**Expected time**: 3-5 minutes

**Troubleshooting**:
- If volume creation fails, check Unity Catalog permissions
- If model registration fails, verify internet connectivity for Hugging Face

---

### Phase 2: Implement Pipeline (60-90 minutes)

Each notebook has detailed TODO comments and FILL_IN placeholders. Follow the patterns from labs 0.1-0.5.

#### Step 2.1: Bronze Layer - Raw Ingestion
**File**: `tweet-pipeline/transformations/bronze_tweet_ingest.py`

**Tasks**:
1. Import pipelines library (`from pyspark import pipelines as dp`)
2. Create streaming table "tweets_bronze"
3. Define tweet schema (4 fields: date, user, text, sentiment)
4. Configure CloudFiles autoloader:
   - Format: "cloudFiles"
   - Data format: "json"
   - Schema location: "/Volumes/workspace/default/checkpoints/"
   - Load path: "s3://dsas-datasets/tweets/"
5. Add metadata columns (source_file from `_metadata.file_path`, processing_time from `current_timestamp()`)

**Validation**:
- Run pipeline (see Phase 3)
- Verify tweets_bronze table exists
- Check row count: ~50,000
- Verify metadata columns present

**Key Concepts**:
- CloudFiles for incremental ingestion
- Schema enforcement for data quality
- Metadata tracking for lineage

---

#### Step 2.2: Silver Layer - Text Preprocessing
**File**: `tweet-pipeline/transformations/silver_tweet_transform.py`

**Tasks**:
1. Import re module for regex
2. Create streaming table "tweets_silver"
3. Define UDF to extract @mentions:
   - Pattern: `r"@[\w]+"`
   - Function: `find_mentions(text)` returns list of mentions
   - Register as UDF with `ArrayType(StringType())` return type
4. Define text cleaning pattern: `"@\\S+"`
5. Apply transformations in append_flow:
   - Clean text with `regexp_replace()`
   - Extract mentions with UDF
   - Explode mentions with `explode_outer()`
   - Convert mentions to lowercase
   - Parse Twitter date string to timestamp (format: "EEE MMM dd HH:mm:ss zzz yyyy")
6. Select final columns: timestamp, mention, cleaned_text, text, sentiment

**Validation**:
- Silver row count > bronze (mention explosion)
- cleaned_text has no @mentions
- timestamp is TimestampType
- Tweets with no mentions have mention=NULL

**Key Concepts**:
- Python UDFs in Spark
- Regex for text processing
- Array explosion for entity extraction
- Date parsing

---

#### Step 2.3: Gold Layer - ML Inference
**File**: `tweet-pipeline/transformations/gold_tweet_transform.py`

**Tasks**:
1. Import mlflow
2. Create streaming table "tweets_gold"
3. Configure MLflow: `mlflow.set_registry_uri("databricks-uc")`
4. Define model output schema:
   - StructType with fields: label (StringType), score (DoubleType)
5. Load model from Unity Catalog:
   - Model URI: "models:/workspace.default.tweet_sentiment_model/1"
   - Create Spark UDF with `mlflow.pyfunc.spark_udf(spark, model_uri, result_type)`
6. Apply transformations in append_flow:
   - Apply model UDF to cleaned_text
   - Extract label from struct: `col("model_output.label")`
   - Extract score and scale to 0-100: `col("model_output.score") * 100`
   - Map LABEL_0/1/2 to negative/neutral/positive
   - Create binary sentiment_id (0=negative, 1=positive/neutral)
   - Create binary predicted_sentiment_id
7. Select final columns (9 total)

**Validation**:
- Gold row count matches silver
- predicted_score is 0-100
- predicted_sentiment is valid string
- Binary IDs are 0 or 1

**Key Concepts**:
- MLflow model loading
- Spark UDFs for ML inference
- Struct column parsing
- Feature engineering

---

#### Step 2.4: Application Layer - Aggregations
**File**: `tweet-pipeline/transformations/gold_tweet_aggregations.sql`

**Tasks**:
1. Create materialized view "gold_tweet_aggregations"
2. Count positive mentions: `COUNT(*) FILTER (WHERE predicted_sentiment = 'positive')`
3. Count negative mentions: `COUNT(*) FILTER (WHERE predicted_sentiment = 'negative')`
4. Count total mentions: `COUNT(*) FILTER (WHERE predicted_sentiment IN ('positive', 'negative'))`
5. Get timestamp range: `MIN(timestamp)`, `MAX(timestamp)`
6. Filter NULL mentions: `WHERE mention IS NOT NULL`
7. Group by mention
8. Order by total DESC

**Validation**:
- View exists with correct name
- Aggregations sum correctly
- Sorted by total (descending)
- No NULL mentions in output

**Key Concepts**:
- Materialized views
- Filtered aggregations
- SQL analytics

---

#### Step 2.5: Model Performance Analysis
**File**: `tweet-pipeline/explorations/Sentiment Model Performance Analysis.py`

**Tasks**:
1. Import sklearn metrics: confusion_matrix, ConfusionMatrixDisplay, classification_report
2. Load gold table: `spark.read.format('delta').table("tweets_gold")`
3. Extract binary sentiment IDs: `y_true = tmp.sentiment_id.values`, `y_pred = tmp.predicted_sentiment_id.values`
4. Define target names: `['negative', 'positive']`
5. Generate classification report: `classification_report(y_true, y_pred, target_names, output_dict=True)`
6. Create confusion matrix: `confusion_matrix(y_true, y_pred)`
7. Display confusion matrix: `ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=target_names)`
8. Log to MLflow:
   - Set registry: `mlflow.set_registry_uri("databricks-uc")`
   - Log metric: accuracy from classification report
   - Log params: model_name ("workspace.default.tweet_sentiment_model"), model_version (1), silver_delta_version
   - Log figure: confusion_matrix.png

**Validation**:
- MLflow experiment has accuracy metric
- Confusion matrix artifact saved
- Model version and table version logged

**Key Concepts**:
- Model evaluation metrics
- MLflow experiment tracking
- Data lineage

---

### Phase 3: Run and Validate Pipeline (15-20 minutes)

#### Step 3.1: Create and Execute Pipeline

1. In Databricks, navigate to **Jobs & Pipelines**
2. Click **Create Pipeline**
3. Configure pipeline:
   - **Name**: "Tweet Sentiment Pipeline"
   - **Pipeline Mode**: Triggered (not continuous)
   - **Source Code**: Select all files in `tweet-pipeline/transformations/` directory
   - **Storage Location**: Let Databricks auto-configure or specify Unity Catalog location
   - **Cluster Mode**: Fixed size or serverless (recommended: serverless)
4. Click **Create**
5. Click **Start** to run the pipeline
6. Monitor execution in pipeline UI

**Expected time**: 10-15 minutes for 50K tweets

**What to Watch**:
- Bronze table populating from S3
- Silver table processing (watch row count increase due to mention explosion)
- Gold table running ML inference (this is the slowest step)
- Application view materializing aggregations

---

#### Step 3.2: Verify Outputs

Check each table/view in **Data Explorer**:

**tweets_bronze**:
- Row count: ~50,000
- Verify source_file and processing_time columns exist
- Sample data to verify JSON structure preserved

**tweets_silver**:
- Row count: >50,000 (mention explosion)
- Verify cleaned_text has no @mentions
- Check mention column for lowercase @usernames
- Verify timestamp is properly parsed

**tweets_gold**:
- Row count: Same as silver
- Verify predicted_score is 0-100
- Check predicted_sentiment is "negative"/"neutral"/"positive"
- Verify binary IDs are 0 or 1

**gold_tweet_aggregations**:
- Rows: Unique mention count
- Verify positive + negative ≤ total (neutral excluded)
- Check sorting by total DESC
- Verify no NULL mentions

---

#### Step 3.3: Run Performance Analysis

1. Open: `tweet-pipeline/explorations/Sentiment Model Performance Analysis.py`
2. Click **Run All**
3. Review outputs:
   - Classification report (precision, recall, F1)
   - Confusion matrix visualization
4. Navigate to **Experiments** tab
5. Find the experiment for this notebook
6. Click on the latest run
7. Verify logged items:
   - Metric: accuracy
   - Parameters: model_name, model_version, silver_delta_version
   - Artifact: confusion_matrix.png

---

#### Step 3.4: Create Dashboard

**Dashboard Requirements** - You must create a Databricks Dashboard with the following 6 visualizations:

##### Visualization 1: Total Tweets Counter
- **Type**: Counter
- **Data Source**: `tweets_bronze` table
- **Query**: `SELECT COUNT(*) as total_tweets FROM tweets_bronze`
- **Display**: Large number showing total tweets ingested

##### Visualization 2: Tweets with Mentions Counter
- **Type**: Counter
- **Data Source**: `tweets_silver` table
- **Query**: `SELECT COUNT(*) as tweets_with_mentions FROM tweets_silver WHERE mention IS NOT NULL`
- **Display**: Count of tweets that mention other users

##### Visualization 3: Tweets without Mentions Counter
- **Type**: Counter
- **Data Source**: `tweets_silver` table
- **Query**: `SELECT COUNT(*) as tweets_without_mentions FROM tweets_silver WHERE mention IS NULL`
- **Display**: Count of tweets with no mentions

##### Visualization 4: Top 10 Users with the Most Tweets Sent
- **Type**: Bar Chart (horizontal)
- **Data Source**: `tweets_bronze` table
- **Query**: `SELECT user, COUNT(*) as tweet_count FROM tweets_bronze GROUP BY user ORDER BY tweet_count DESC LIMIT 10`
- **X-axis**: tweet_count (number of tweets)
- **Y-axis**: user (username)
- **Title**: "Top 10 Users with the Most Tweets Sent"

##### Visualization 5: Top 10 Most Positively Mentioned Users
- **Type**: Bar Chart (horizontal)
- **Data Source**: `gold_tweet_aggregations` view
- **Query**: `SELECT mention, positive FROM gold_tweet_aggregations ORDER BY positive DESC LIMIT 10`
- **X-axis**: positive (count)
- **Y-axis**: mention (@username)
- **Title**: "Top 10 Users with Most Positive Mentions"
- **Color**: Green

##### Visualization 6: Top 10 Most Negatively Mentioned Users
- **Type**: Bar Chart (horizontal)
- **Data Source**: `gold_tweet_aggregations` view
- **Query**: `SELECT mention, negative FROM gold_tweet_aggregations ORDER BY negative DESC LIMIT 10`
- **X-axis**: negative (count)
- **Y-axis**: mention (@username)
- **Title**: "Top 10 Users with Most Negative Mentions"
- **Color**: Red

**Dashboard Layout**:
```
+------------------------+------------------------+------------------------+
| Total Tweets Counter  | With Mentions Counter | Without Mentions Ctr  |
+------------------------+------------------------+------------------------+
| Top 10 Users with Most Tweets Sent (Bar Chart)                        |
+------------------------------------------------------------------------+
| Top 10 Most Positive (Bar Chart)                                      |
+------------------------------------------------------------------------+
| Top 10 Most Negative (Bar Chart)                                      |
+------------------------------------------------------------------------+
```

**How to Create**:
1. In Databricks, go to **Dashboards** → **Create Dashboard**
2. Name: "Tweet Sentiment Analysis"
3. Add each visualization using SQL queries above
4. Arrange in recommended layout
5. Save dashboard

**Export Dashboard JSON**:
1. Click **Share** → **Export**
2. Save JSON file to your GitHub repo as `tweet-pipeline/Tweet_Sentiment_Dashboard.lvdash.json`
3. Commit and push to your fork

---

#### Step 3.5: Configure Automated Databricks Job

**Business Goal**: Ensure your pipeline and dashboard automatically refresh daily to reflect any new data written to the S3 bucket.

**Requirements**: Create a Databricks Job that:
1. Runs your Tweet Sentiment Pipeline
2. Refreshes your Tweet Analysis Dashboard
3. Executes automatically on a daily schedule

**Implementation Steps**:

1. **Navigate to Workflows**:
   - In Databricks, click **Workflows** in the left sidebar
   - Click **Create Job**

2. **Configure Job Settings**:
   - **Name**: "Daily Tweet Sentiment Analysis Job"
   - **Description**: "Automated pipeline execution and dashboard refresh for new tweets"

3. **Add Task 1: Run Pipeline**:
   - Click **Add task**
   - **Task name**: "Run Tweet Pipeline"
   - **Type**: Delta Live Tables pipeline
   - **Pipeline**: Select your "Tweet Sentiment Pipeline"
   - **Task description**: "Process new tweets from S3 through all layers"

4. **Add Task 2: Refresh Dashboard** (depends on Task 1):
   - Click **Add task** (this will be Task 2)
   - **Task name**: "Refresh Dashboard"
   - **Type**: Run dashboard refresh
   - **Dashboard**: Select your "Tweet Sentiment Analysis" dashboard
   - **Depends on**: Select "Run Tweet Pipeline" (ensures pipeline completes before refresh)
   - **Task description**: "Update dashboard visualizations with latest data"

5. **Configure Schedule**:
   - Click on **Schedule** tab
   - **Schedule type**: Scheduled
   - **Trigger type**: Cron
   - **Cron expression**: `0 2 * * *` (runs daily at 2:00 AM UTC)
   - **Time zone**: Select your preferred timezone
   - **Description**: "Daily execution to process new S3 data"

6. **Set Notifications** (optional but recommended):
   - Click **Alerts** tab
   - Add email notification on failure
   - Add email notification on success (optional)

7. **Save and Test**:
   - Click **Save**
   - Click **Run now** to test the job execution
   - Monitor both tasks completing successfully
   - Verify dashboard shows updated data

**Validation**:
- Job runs successfully with both tasks completing
- Pipeline processes any new data from S3
- Dashboard refreshes with updated metrics
- Job scheduled to run daily at configured time

**What to Submit**:
- Screenshot of the job configuration showing:
  - Both tasks (pipeline + dashboard refresh)
  - Task dependencies (Task 2 depends on Task 1)
  - Schedule configuration (daily cron)
  - Recent successful run history

---

## Grading Rubric (50 points total DSCC202 53 points total DSCC402)

### Run All Execution (7 points)
- [ ] Pipeline runs end-to-end without errors (3 pts)
- [ ] All tables created successfully (2 pts)
- [ ] Performance analysis notebook completes (2 pts)

### Bronze Layer Implementation (8 points)
- [ ] Correct schema definition (2 pts)
- [ ] CloudFiles configuration (2 pts)
- [ ] Metadata columns added (2 pts)
- [ ] ~50,000 rows ingested (2 pts)

### Silver Layer Implementation (10 points)
- [ ] Mention extraction UDF works (3 pts)
- [ ] Text cleaning removes mentions (2 pts)
- [ ] explode_outer() creates rows per mention (2 pts)
- [ ] Date parsing successful (2 pts)
- [ ] Row count > bronze (1 pt)

### Gold Layer Implementation (12 points)
- [ ] Model loads from Unity Catalog (3 pts)
- [ ] Spark UDF applies predictions (3 pts)
- [ ] Label mapping correct (2 pts)
- [ ] Binary IDs created (2 pts)
- [ ] All columns present (2 pts)

### Application Layer Implementation (5 points)
- [ ] Materialized view created (1 pt)
- [ ] Filtered aggregations work (2 pts)
- [ ] Correct grouping and sorting (2 pts)

### Performance Analysis (3 points)
- [ ] Classification report generated (1 pt)
- [ ] Confusion matrix created (1 pt)
- [ ] MLflow logging complete (1 pt)

### Dashboard Creation (2 points)
- [ ] All 6 required visualizations present (1 pt)
- [ ] Dashboard JSON committed to repo (1 pt)

### Automated Job Configuration (3 points)
- [ ] Job created with pipeline task (1 pt)
- [ ] Dashboard refresh task added with dependency (1 pt)
- [ ] Daily schedule configured correctly (1 pt)

### Graduate Students Only (DSCC-402): +3 points
- [ ] Written analysis of optimization opportunities (3 pts)
  - Address: spill, skew, shuffle, storage, serialization

---

## Expected Deliverables

### 1. Completed Notebooks
- All TODO/FILL_IN sections implemented correctly
- Notebooks run end-to-end without errors
- Code follows best practices from labs 0.1-0.5

### 2. Pipeline Execution
- Successfully executed pipeline visible in Databricks UI
- All tables populated with data:
  - tweets_bronze: ~50,000 rows
  - tweets_silver: >50,000 rows
  - tweets_gold: matches silver
  - gold_tweet_aggregations: unique mentions

### 3. MLflow Experiment
- Logged run with accuracy metric
- Confusion matrix artifact
- Model and data version parameters

### 4. Dashboard
- Dashboard with 6 required visualizations
- Screenshots of dashboard in submission

### 5. Automated Databricks Job
- Job configured with two tasks: pipeline execution + dashboard refresh
- Task dependency configured (dashboard depends on pipeline)
- Daily schedule set with cron expression
- Screenshot of job configuration and successful run

---

## Troubleshooting Guide

### Issue: Pipeline fails on bronze ingestion
**Solution**:
- Verify S3 path: `s3://dsas-datasets/tweets/`
- Check schema definition matches JSON structure
- Review CloudFiles error logs for specific field mismatches
- Ensure checkpoint volume exists: `/Volumes/workspace/default/checkpoints/`

### Issue: Model not found in Unity Catalog
**Solution**:
- Rerun `Run me first.ipynb` to register model
- Verify model exists: **Data Explorer** → **Models** → tweet_sentiment_model
- Check MODEL_URI in code: "models:/workspace.default.tweet_sentiment_model/1"

### Issue: Silver table has no rows
**Solution**:
- Check bronze table has data
- Verify UDF registration worked (no syntax errors in pattern)
- Test UDF on sample data: `spark.sql("SELECT find_mentions_udf('@user test') as mentions")` won't work directly, but test the Python function

### Issue: Gold predictions all same class
**Solution**:
- Verify model loaded correctly (check logs)
- Test model UDF on small sample
- Check label mapping logic (LABEL_0/1/2 → sentiment strings)
- Verify cleaned_text column has actual text (not empty)

### Issue: Pipeline stuck or very slow
**Solution**:
- Review Spark UI for bottlenecks
- ML inference is slowest - be patient (30 min for 50K rows)
- Ensure serverless compute is used for auto-scaling

### Issue: Dashboard queries timeout
**Solution**:
- Verify tables are fully populated (check row counts)
- Use materialized view for aggregations (not direct gold table queries)
- Check cluster is running when viewing dashboard

### Issue: Job fails or doesn't refresh dashboard
**Solution**:
- Verify pipeline task completes successfully before dashboard task runs
- Check task dependency is properly configured (Task 2 depends on Task 1)
- Ensure dashboard exists and is accessible to the job
- Verify job has permissions to run pipeline and refresh dashboard
- Check cluster configuration for the job tasks
- Review job run logs for specific error messages
- Test each task individually before running the full job

---

## Submission Instructions

### What to Submit

1. **GitHub Repository Link** (REQUIRED):
   - Your forked repository with completed notebooks
   - URL should be: `https://github.com/YOUR_USERNAME/dscc202-402-spring2026`
   - Ensure all files in `labs/final_project/` are complete
   - Include dashboard JSON: `tweet-pipeline/Tweet_Sentiment_Dashboard.lvdash.json`
   - Include commit history showing your work

2. **Screenshots** (via Canvas - 5 required):
   - Screenshot 1: Pipeline successful execution (Databricks UI showing all tables green/completed)
   - Screenshot 2: tweets_gold table row count (Data Explorer)
   - Screenshot 3: MLflow experiment with logged metrics (Experiments tab)
   - Screenshot 4: Dashboard with all 6 visualizations
   - Screenshot 5: Databricks Job showing both tasks, dependencies, schedule, and successful run

3. **Graduate Students (DSCC-402)** (REQUIRED):
   - Additional markdown file: `tweet-pipeline/SPARK_PERF_ANALYSIS.md`
   - Address all 5 dimensions: spill, skew, shuffle, storage, serialization
   - Provide specific recommendations for optimization

### Submission Deadline
**Due**: See Blackboard for exact date and time

### How to Submit
1. **Push to GitHub**: Commit and push all changes to your forked repository
2. **Submit on Canvas**:
   - Assignment: "Final Project - Tweet Sentiment Analysis"
   - Submit GitHub repository URL
   - Upload required screenshots 
   - Add any comments/notes about implementation challenges

---

## Tips for Success

### 1. Start Early
- **Week 1**: Fork repo, run setup, complete Bronze layer
- **Week 2**: Complete Silver and Gold layers
- **Week 3**: Complete Application layer, analysis, dashboard, job configuration, testing

### 2. Test Incrementally
- Don't wait to test until the full pipeline is implemented
- Use Pipeline UI to run individual transformations
- Verify row counts and data quality at each layer
- Sample data frequently: `display(df.limit(10))`

### 3. Use Existing Patterns
- Review labs 0.1-0.5 for similar operations:
  - Bronze ingestion similar to Lab 0.4 (Delta Lake)
  - Silver UDF similar to Lab 0.1 (Spark Core - UDF section)
  - Gold ML inference similar to Lab 0.5 (MLops)
  - Aggregations similar to Lab 0.1 (aggregations section)

### 4. Leverage Documentation
- Spark Declarative Pipelines: https://docs.databricks.com/en/delta-live-tables/index.html
- MLflow on UC: https://docs.databricks.com/en/mlflow/index.html
- Spark UDF docs: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.udf.html
- CloudFiles: https://docs.databricks.com/en/ingestion/auto-loader/index.html

### 5. Debug Effectively
- Read error messages carefully (they often tell you exactly what's wrong)
- Check Spark UI for job failures
- Use `display()` liberally to inspect intermediate results
- Verify data types match expected (StringType vs IntegerType, etc.)

### 6. Ask Questions
- Use office hours for conceptual questions
- Post technical issues to discussion board (with error messages)
- Form study groups (but submit your own work!)

---

## Academic Integrity

This is an **individual project**. While you may discuss concepts and approaches with classmates, all code and implementation must be your own work.

### Violations Include:
- Copying code from classmates
- Sharing completed notebooks
- Using external solutions (StackOverflow copy-paste without understanding)

### Allowed:
- Discussing concepts and debugging approaches
- Reviewing official documentation and course materials
- Asking instructors and TAs for guidance
- Sharing error messages for troubleshooting help

**Violations will result in zero credit and potential academic integrity proceedings.**

---

## Resources

### Course Materials
- Labs 0.1-0.5 (similar patterns and techniques)
- Lecture slides on Spark Declarative Pipelines, MLflow, and Unity Catalog

### Databricks Documentation
- Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/index.html
- Auto Loader (CloudFiles): https://docs.databricks.com/en/ingestion/auto-loader/index.html
- Lakeview Dashboards: https://docs.databricks.com/en/dashboards/index.html

### MLflow Resources
- MLflow Guide: https://mlflow.org/docs/latest/
- Model Registry with UC: https://docs.databricks.com/en/mlflow/model-registry.html
- Tracking API: https://mlflow.org/docs/latest/tracking.html

### Hugging Face Model
- Model card: https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment
- Usage examples and fine-tuning details
- Paper: https://arxiv.org/abs/2010.12421

---

## Support

For help with this project:
1. **Technical Issues**: Post to course discussion board with:
   - Error messages (full text)
   - Screenshots of the issue
   - What you've already tried
   - Relevant code snippet
2. **Conceptual Questions**: Attend office hours or email instructor
3. **Databricks Platform Issues**: Consult Databricks documentation first, then ask on discussion board

**Office Hours**: See Blackboard for schedule

---

## Conclusion

This project demonstrates real-world data engineering skills that are highly valued in industry:
- Building scalable data pipelines
- Applying ML at scale
- Tracking experiments for reproducibility
- Creating business intelligence dashboards

**Good luck!** 

Remember: The goal is not just to complete the project, but to understand how to build production-quality data pipelines that can scale to millions of records.
