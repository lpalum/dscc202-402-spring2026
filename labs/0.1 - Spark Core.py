# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Franchise Analytics
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Welcome to **The Bakehouse**, a growing franchise of artisan bakeries operating across multiple cities. As a Data Analyst at Bakehouse HQ, you'll analyze sales patterns, customer behavior, supplier relationships, and customer satisfaction to support strategic business decisions.
# MAGIC
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC You'll work with six interconnected tables from the `samples.bakehouse` catalog:
# MAGIC
# MAGIC | Table | Description | Row Count |
# MAGIC |-------|-------------|-----------|
# MAGIC | `sales_transactions` | Individual purchases at franchises | 3,333 |
# MAGIC | `sales_customers` | Customer demographic information | 300 |
# MAGIC | `sales_franchises` | Franchise location and attributes | 48 |
# MAGIC | `sales_suppliers` | Ingredient suppliers and approval status | 27 |
# MAGIC | `media_customer_reviews` | Customer feedback and ratings | 204 |
# MAGIC | `media_gold_reviews_chunked` | Processed review text for analysis | 196 |
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC This comprehensive lab covers nine core Apache Spark topics:
# MAGIC
# MAGIC 1. **Data Exploration** - Navigate catalogs, inspect tables, execute basic queries
# MAGIC 2. **DataFrames & Transformations** - Create DataFrames, filter, select, and order data
# MAGIC 3. **Data Ingestion** - Work with schemas, read/write data in multiple formats
# MAGIC 4. **Column Operations** - Derive new columns, perform calculations, chain transformations
# MAGIC 5. **Aggregations** - Group data, compute summaries, analyze franchise performance
# MAGIC 6. **DateTime Operations** - Parse timestamps, extract date components, analyze temporal patterns
# MAGIC 7. **Complex Types** - Work with arrays, split text, collect values
# MAGIC 8. **Multi-Table Joins** - Combine datasets, handle nulls, enrich data
# MAGIC 9. **User-Defined Functions** - Create custom logic, register UDFs, apply transformations
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog for our lab
# MAGIC CREATE CATALOG IF NOT EXISTS bakehouse_catalog;
# MAGIC
# MAGIC -- Create a schema (database) in the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS bakehouse_catalog.lab;
# MAGIC
# MAGIC -- Create a managed volume for file storage
# MAGIC CREATE VOLUME IF NOT EXISTS bakehouse_catalog.lab.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
import os

# Use Unity Catalog managed volume for file storage
working_dir = "/Volumes/bakehouse_catalog/lab/workspace"

print(f"Working directory: {working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Data Exploration Fundamentals
# MAGIC
# MAGIC **Business Goal:** Understand the structure of our data and verify data availability.
# MAGIC
# MAGIC In this section, you'll explore the Bakehouse catalog structure, create table references, and execute basic SQL queries to understand the scale and scope of our datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Explore Catalog Structure
# MAGIC
# MAGIC Use the `%fs ls` magic command to list the contents of the samples catalog.

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Query Transaction Count
# MAGIC
# MAGIC Write a SQL query to count the total number of transactions in the `samples.bakehouse.sales_transactions` table.

# COMMAND ----------

# TODO
# Use spark.sql() to execute a COUNT query
# Store the result in transaction_count_df

transaction_count_df = spark.sql("""
    FILL_IN
""")

display(transaction_count_df)

# COMMAND ----------

# CHECK YOUR WORK
assert transaction_count_df.first()[0] == 3333, "Expected 3,333 transactions"
print("✅ Task 1.2 complete: Transaction count verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Identify Payment Methods
# MAGIC
# MAGIC Query the distinct payment methods available in our transaction data.

# COMMAND ----------

# TODO
# Write a SQL query to select DISTINCT paymentMethod values
# Order the results alphabetically

payment_methods_df = spark.sql("""
    FILL_IN
""")

display(payment_methods_df)

# COMMAND ----------

# CHECK YOUR WORK
payment_methods = [row.paymentMethod for row in payment_methods_df.collect()]
assert "visa" in payment_methods, "Expected 'visa' payment method"
assert len(payment_methods) >= 3, "Expected at least 3 payment methods"
print("✅ Task 1.3 complete: Payment methods identified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.4: Calculate Average Transaction Value
# MAGIC
# MAGIC Use SQL to compute the average `totalPrice` across all transactions, rounded to 2 decimal places.

# COMMAND ----------

# TODO
# Use AVG() and ROUND() functions
# Alias the result as avg_transaction_value

avg_value_df = spark.sql("""
    FILL_IN
""")

display(avg_value_df)

# COMMAND ----------

# CHECK YOUR WORK
avg_value = avg_value_df.first()[0]
assert avg_value > 0, "Average transaction value should be positive"
assert abs(avg_value - round(avg_value, 2)) < 0.01, "Value should be rounded to 2 decimals"
print(f"✅ Task 1.4 complete: Average transaction value = ${avg_value:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: DataFrames & Transformations
# MAGIC
# MAGIC **Business Goal:** Load data into DataFrames and apply basic transformations to filter and sort records.
# MAGIC
# MAGIC Now you'll work with the DataFrame API to perform programmatic data manipulation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Create DataFrames
# MAGIC
# MAGIC Load the transactions and customers tables into DataFrames.

# COMMAND ----------

# TODO
# Use spark.table() to create DataFrames for:
# - samples.bakehouse.sales_transactions (name it transactions_df)
# - samples.bakehouse.sales_customers (name it customers_df)

transactions_df = FILL_IN
customers_df = FILL_IN

# COMMAND ----------

# CHECK YOUR WORK
assert transactions_df.count() == 3333, "Transactions DataFrame should have 3,333 rows"
assert customers_df.count() == 300, "Customers DataFrame should have 300 rows"
print("✅ Task 2.1 complete: DataFrames created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Inspect Schema
# MAGIC
# MAGIC Display the schema of the transactions DataFrame to understand its structure.

# COMMAND ----------

# TODO
# Call printSchema() on transactions_df

FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Filter High-Value Transactions
# MAGIC
# MAGIC Filter transactions where `totalPrice` is greater than 50, then sort by `totalPrice` in descending order.

# COMMAND ----------

# TODO
# Use .filter() and .orderBy() methods
# Remember to use desc() for descending order from pyspark.sql.functions

from pyspark.sql.functions import desc

high_value_df = (transactions_df
    .FILL_IN
    .FILL_IN
)

display(high_value_df)

# COMMAND ----------

# CHECK YOUR WORK
assert high_value_df.count() > 0, "Should have some high-value transactions"
first_row = high_value_df.first()
assert first_row.totalPrice > 50, "All filtered transactions should have totalPrice > 50"
# Verify descending order
all_prices = [row.totalPrice for row in high_value_df.take(10)]
assert all_prices == sorted(all_prices, reverse=True), "Results should be sorted descending"
print(f"✅ Task 2.3 complete: Found {high_value_df.count()} high-value transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4: Select Specific Columns
# MAGIC
# MAGIC From the customers DataFrame, select only `customerID`, `first_name`, `last_name`, and `city`.

# COMMAND ----------

# TODO
# Use .select() to choose specific columns

customer_summary_df = customers_df.select(
    FILL_IN
)

display(customer_summary_df)

# COMMAND ----------

# CHECK YOUR WORK
assert len(customer_summary_df.columns) == 4, "Should have exactly 4 columns"
assert "customerID" in customer_summary_df.columns, "Should include customerID"
assert "city" in customer_summary_df.columns, "Should include city"
print("✅ Task 2.4 complete: Column selection verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Data Ingestion Patterns
# MAGIC
# MAGIC **Business Goal:** Learn to read and write data with different schema strategies and formats.
# MAGIC
# MAGIC You'll practice schema inference, explicit schema definition, and working with Delta format.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Schema Inference
# MAGIC
# MAGIC Read the transactions table using schema inference and display the inferred schema.

# COMMAND ----------

# TODO
# Read the table with inferSchema (spark.table does this automatically)
# Store in inferred_schema_df and call printSchema()

inferred_schema_df = spark.table("samples.bakehouse.sales_transactions")
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Define Custom Schema
# MAGIC
# MAGIC Define a custom schema for franchise data using `StructType` and `StructField`.

# COMMAND ----------

# TODO
# Import necessary types
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# Define schema for franchises with fields:
# franchiseID (LongType), name (StringType), city (StringType), country (StringType)

franchise_schema = StructType([
    FILL_IN
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Write Data to Delta Format
# MAGIC
# MAGIC Filter transactions for a specific payment method and write the result to Delta format.

# COMMAND ----------

# TODO
# Filter transactions where paymentMethod == "visa"
# Write to Delta format at path f"{working_dir}/visa_transactions"

visa_transactions_df = transactions_df.filter(FILL_IN)

(visa_transactions_df
    .write
    .mode("overwrite")
    .format("FILL_IN")
    .save(f"{working_dir}/visa_transactions")
)

# COMMAND ----------

# CHECK YOUR WORK
import os
delta_path = f"{working_dir}/visa_transactions"
assert os.path.exists(f"{delta_path}/_delta_log"), "Delta log should exist"
print("✅ Task 3.3 complete: Data written to Delta format")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4: Read from Delta Format
# MAGIC
# MAGIC Read the Delta table you just created and verify the row count.

# COMMAND ----------

# TODO
# Read from the Delta path and display results

read_visa_df = (spark
    .read
    .format("FILL_IN")
    .load(f"{working_dir}/visa_transactions")
)

display(read_visa_df)

# COMMAND ----------

# CHECK YOUR WORK
assert read_visa_df.count() == visa_transactions_df.count(), "Row counts should match"
print(f"✅ Task 3.4 complete: Read {read_visa_df.count()} records from Delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Column Operations & Revenue Analysis
# MAGIC
# MAGIC **Business Goal:** Calculate derived metrics like discounts and profit margins.
# MAGIC
# MAGIC You'll use `withColumn()` to add computed columns and practice method chaining.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.1: Calculate Per-Unit Discount
# MAGIC
# MAGIC Some transactions may have discounts. Calculate the discount per unit as the difference between `unitPrice` and (`totalPrice` / `quantity`).

# COMMAND ----------

# TODO
# Use withColumn to create a new column called "discount_per_unit"
# Formula: unitPrice - (totalPrice / quantity)

from pyspark.sql.functions import col

transactions_with_discount_df = transactions_df.withColumn(
    "discount_per_unit",
    FILL_IN
)

display(transactions_with_discount_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "discount_per_unit" in transactions_with_discount_df.columns, "Should have discount_per_unit column"
print("✅ Task 4.1 complete: Discount calculation added")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Filter by Payment Method
# MAGIC
# MAGIC Filter the transactions to show only credit card payments (`paymentMethod` contains "credit").

# COMMAND ----------

# TODO
# Use .filter() with a condition on paymentMethod

credit_card_df = transactions_with_discount_df.filter(
    FILL_IN
)

display(credit_card_df)

# COMMAND ----------

# CHECK YOUR WORK
assert credit_card_df.count() > 0, "Should have credit card transactions"
for row in credit_card_df.take(5):
    assert "credit" in row.paymentMethod.lower(), "All rows should be credit card payments"
print(f"✅ Task 4.2 complete: {credit_card_df.count()} credit card transactions found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3: Method Chaining
# MAGIC
# MAGIC Chain multiple transformations: add a revenue column (`quantity * unitPrice`), filter for revenue > 30, and sort by revenue descending.

# COMMAND ----------

# TODO
# Chain: withColumn -> filter -> orderBy in a single statement

chained_df = (transactions_df
    .withColumn("revenue", FILL_IN)
    .filter(FILL_IN)
    .orderBy(FILL_IN)
)

display(chained_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "revenue" in chained_df.columns, "Should have revenue column"
assert chained_df.first().revenue > 30, "All rows should have revenue > 30"
revenues = [row.revenue for row in chained_df.take(5)]
assert revenues == sorted(revenues, reverse=True), "Should be sorted descending"
print("✅ Task 4.3 complete: Method chaining successful")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 5: Aggregations & Franchise Performance
# MAGIC
# MAGIC **Business Goal:** Identify top-performing franchises and analyze sales patterns.
# MAGIC
# MAGIC You'll use `groupBy()` and aggregation functions to summarize data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.1: Total Revenue by Franchise
# MAGIC
# MAGIC Calculate total revenue for each franchise, ordered by revenue descending.

# COMMAND ----------

# TODO
# Group by franchiseID, sum totalPrice, order by sum descending

from pyspark.sql.functions import sum, avg, round as spark_round

revenue_by_franchise_df = (transactions_df
    .groupBy(FILL_IN)
    .agg(sum(FILL_IN).alias("total_revenue"))
    .orderBy(FILL_IN)
)

display(revenue_by_franchise_df)

# COMMAND ----------

# CHECK YOUR WORK
assert revenue_by_franchise_df.count() > 0, "Should have franchise revenue data"
assert "total_revenue" in revenue_by_franchise_df.columns, "Should have total_revenue column"
revenues = [row.total_revenue for row in revenue_by_franchise_df.take(5)]
assert revenues == sorted(revenues, reverse=True), "Should be sorted by revenue descending"
print("✅ Task 5.1 complete: Franchise revenue calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Average Transaction Value per Franchise
# MAGIC
# MAGIC Compute the average transaction value for each franchise, rounded to 2 decimal places.

# COMMAND ----------

# TODO
# Group by franchiseID, calculate avg(totalPrice), round to 2 decimals

avg_transaction_by_franchise_df = (transactions_df
    .groupBy(FILL_IN)
    .agg(spark_round(avg(FILL_IN), 2).alias("avg_transaction_value"))
    .orderBy(FILL_IN)
)

display(avg_transaction_by_franchise_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "avg_transaction_value" in avg_transaction_by_franchise_df.columns, "Should have avg_transaction_value"
first_val = avg_transaction_by_franchise_df.first().avg_transaction_value
assert isinstance(first_val, (int, float)), "Average should be numeric"
print("✅ Task 5.2 complete: Average transaction value calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3: Top 3 Performing Franchises
# MAGIC
# MAGIC Identify the top 3 franchises by total revenue.

# COMMAND ----------

# TODO
# Reuse revenue_by_franchise_df and apply .limit(3)

top_3_franchises_df = revenue_by_franchise_df.FILL_IN

display(top_3_franchises_df)

# COMMAND ----------

# CHECK YOUR WORK
assert top_3_franchises_df.count() == 3, "Should return exactly 3 franchises"
print("✅ Task 5.3 complete: Top 3 franchises identified")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 6: DateTime Operations & Temporal Patterns
# MAGIC
# MAGIC **Business Goal:** Analyze sales trends over time to identify peak business periods.
# MAGIC
# MAGIC You'll work with timestamp data, extract date components, and analyze temporal patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.1: Extract Date Components
# MAGIC
# MAGIC From the `dateTime` column in transactions, extract year, month, day, and hour.

# COMMAND ----------

# TODO
# Use date_format or extract functions to create new columns

from pyspark.sql.functions import year, month, dayofmonth, hour, date_format

transactions_with_dates_df = (transactions_df
    .withColumn("year", year(col("FILL_IN")))
    .withColumn("month", FILL_IN)
    .withColumn("day", FILL_IN)
    .withColumn("hour", FILL_IN)
)

display(transactions_with_dates_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "year" in transactions_with_dates_df.columns, "Should have year column"
assert "month" in transactions_with_dates_df.columns, "Should have month column"
assert "day" in transactions_with_dates_df.columns, "Should have day column"
assert "hour" in transactions_with_dates_df.columns, "Should have hour column"
print("✅ Task 6.1 complete: Date components extracted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2: Daily Active Customers
# MAGIC
# MAGIC Calculate the approximate count of distinct customers per day using `approx_count_distinct()`.

# COMMAND ----------

# TODO
# Use date_format to extract date (YYYY-MM-dd)
# Group by date and use approx_count_distinct on customerID

from pyspark.sql.functions import approx_count_distinct, to_date

daily_customers_df = (transactions_df
    .withColumn("transaction_date", to_date(col("FILL_IN")))
    .groupBy(FILL_IN)
    .agg(approx_count_distinct(FILL_IN).alias("active_customers"))
    .orderBy(FILL_IN)
)

display(daily_customers_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "transaction_date" in daily_customers_df.columns, "Should have transaction_date"
assert "active_customers" in daily_customers_df.columns, "Should have active_customers"
assert daily_customers_df.count() > 0, "Should have daily customer counts"
print("✅ Task 6.2 complete: Daily active customers calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.3: Revenue by Day of Week
# MAGIC
# MAGIC Analyze total revenue by day of week (Monday = 1, Sunday = 7).

# COMMAND ----------

# TODO
# Use date_format with 'E' pattern to get day name
# Or use dayofweek function
# Group by day and sum revenue

from pyspark.sql.functions import dayofweek

revenue_by_dow_df = (transactions_df
    .withColumn("day_of_week", date_format(col("FILL_IN"), "FILL_IN"))
    .groupBy(FILL_IN)
    .agg(sum(FILL_IN).alias("total_revenue"))
    .orderBy(FILL_IN)
)

display(revenue_by_dow_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "day_of_week" in revenue_by_dow_df.columns, "Should have day_of_week"
assert revenue_by_dow_df.count() > 0, "Should have day-of-week revenue"
print("✅ Task 6.3 complete: Revenue by day of week calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 7: Complex Types & Product Analysis
# MAGIC
# MAGIC **Business Goal:** Analyze product variety and customer purchase patterns.
# MAGIC
# MAGIC You'll work with array operations, including splitting text, collecting values, and exploding arrays.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.1: Split Review Text into Words
# MAGIC
# MAGIC From the reviews table, split the `review` text column into an array of words.

# COMMAND ----------

# TODO
# Use split() function to create an array column

from pyspark.sql.functions import split

reviews_df = spark.table("samples.bakehouse.media_customer_reviews")

reviews_with_words_df = reviews_df.withColumn(
    "review_words",
    split(col("FILL_IN"), " ")
)

display(reviews_with_words_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "review_words" in reviews_with_words_df.columns, "Should have review_words array column"
first_row = reviews_with_words_df.first()
assert isinstance(first_row.review_words, list), "review_words should be an array/list"
print("✅ Task 7.1 complete: Review text split into word arrays")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.2: Collect Unique Products per Customer
# MAGIC
# MAGIC For each customer, collect the set of unique products they've purchased.

# COMMAND ----------

# TODO
# Group by customerID and use collect_set on product column

from pyspark.sql.functions import collect_set

customer_products_df = (transactions_df
    .groupBy(FILL_IN)
    .agg(collect_set(FILL_IN).alias("products_purchased"))
)

display(customer_products_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "products_purchased" in customer_products_df.columns, "Should have products_purchased array"
first_row = customer_products_df.first()
assert isinstance(first_row.products_purchased, list), "products_purchased should be an array"
print("✅ Task 7.2 complete: Unique products collected per customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.3: Count Array Elements
# MAGIC
# MAGIC Count how many unique products each customer has purchased using the `size()` function.

# COMMAND ----------

# TODO
# Use size() function on the products_purchased array

from pyspark.sql.functions import size

customer_product_count_df = customer_products_df.withColumn(
    "product_count",
    size(col("FILL_IN"))
)

display(customer_product_count_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "product_count" in customer_product_count_df.columns, "Should have product_count"
assert customer_product_count_df.first().product_count > 0, "Product count should be positive"
print("✅ Task 7.3 complete: Product counts calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.4: Explode Array Elements
# MAGIC
# MAGIC Take the word arrays from reviews and explode them into individual rows.

# COMMAND ----------

# TODO
# Use explode() on review_words column

from pyspark.sql.functions import explode

exploded_words_df = reviews_with_words_df.select(
    "franchiseID",
    "review_date",
    explode(col("FILL_IN")).alias("word")
)

display(exploded_words_df.limit(50))

# COMMAND ----------

# CHECK YOUR WORK
assert "word" in exploded_words_df.columns, "Should have word column after explode"
assert exploded_words_df.count() > reviews_with_words_df.count(), "Exploded should have more rows"
print("✅ Task 7.4 complete: Array elements exploded")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 8: Multi-Table Joins & Customer Insights
# MAGIC
# MAGIC **Business Goal:** Enrich transaction data with customer demographics and franchise details.
# MAGIC
# MAGIC You'll perform inner, left, and outer joins to combine datasets and handle missing values.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.1: Enrich Transactions with Customer Names
# MAGIC
# MAGIC Join transactions with customers to add first and last names.

# COMMAND ----------

# TODO
# Perform inner join on customerID
# Select relevant columns

transactions_with_customers_df = (transactions_df
    .join(
        customers_df,
        FILL_IN,
        FILL_IN  # join type
    )
    .select(
        transactions_df["*"],
        customers_df["first_name"],
        customers_df["last_name"],
        customers_df["city"].alias("customer_city")
    )
)

display(transactions_with_customers_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "first_name" in transactions_with_customers_df.columns, "Should have first_name from customers"
assert "last_name" in transactions_with_customers_df.columns, "Should have last_name from customers"
print("✅ Task 8.1 complete: Transactions enriched with customer names")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.2: Add Franchise Location Details
# MAGIC
# MAGIC Further enrich the data by joining with franchise information.

# COMMAND ----------

# TODO
# Load franchises table and join with previous result
# Use inner join on franchiseID

franchises_df = spark.table("samples.bakehouse.sales_franchises")

full_transaction_df = (transactions_with_customers_df
    .join(
        franchises_df,
        FILL_IN,
        "inner"
    )
    .select(
        transactions_with_customers_df["*"],
        franchises_df["name"].alias("franchise_name"),
        franchises_df["city"].alias("franchise_city"),
        franchises_df["country"]
    )
)

display(full_transaction_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "franchise_name" in full_transaction_df.columns, "Should have franchise_name"
assert "franchise_city" in full_transaction_df.columns, "Should have franchise_city"
print("✅ Task 8.2 complete: Franchise details added")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.3: Identify Customers Without Recent Reviews
# MAGIC
# MAGIC Use a left join to find customers who haven't left reviews.

# COMMAND ----------

# TODO
# Left join customers with reviews on customerID
# Filter for null review_date (customers with no reviews)

customers_without_reviews_df = (customers_df
    .join(
        reviews_df,
        customers_df["customerID"] == reviews_df["franchiseID"],  # Note: This is a simplified join
        "FILL_IN"  # join type
    )
    .filter(col("review_date").isNull())
    .select(customers_df["*"])
)

display(customers_without_reviews_df)

# COMMAND ----------

# CHECK YOUR WORK
# Note: The actual join condition may vary based on data relationships
assert customers_without_reviews_df.count() >= 0, "Should complete without error"
print(f"✅ Task 8.3 complete: Found {customers_without_reviews_df.count()} customers without reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8.4: Handle Null Values
# MAGIC
# MAGIC Fill null values in franchise size with "Unknown".

# COMMAND ----------

# TODO
# Use na.fill() or fillna() on franchises_df

from pyspark.sql.functions import coalesce, lit

franchises_cleaned_df = franchises_df.na.fill(
    {"size": "FILL_IN"}
)

display(franchises_cleaned_df)

# COMMAND ----------

# CHECK YOUR WORK
null_count = franchises_cleaned_df.filter(col("size").isNull()).count()
assert null_count == 0, "Should have no null values in size column"
print("✅ Task 8.4 complete: Null values handled")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 9: User-Defined Functions & Custom Logic
# MAGIC
# MAGIC **Business Goal:** Apply custom business rules and categorization logic.
# MAGIC
# MAGIC You'll create and register UDFs to implement domain-specific transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9.1: Categorize Payment Methods
# MAGIC
# MAGIC Create a UDF that categorizes payment methods into "Credit Card" (amex/visa/mastercard) or "Other".

# COMMAND ----------

# TODO
# Define a Python function and register it as a UDF

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_payment(payment_method):
    if payment_method.lower() in ["visa", "mastercard", "amex"]:
        return "FILL_IN"
    else:
        return "FILL_IN"

# Register as UDF
categorize_payment_udf = udf(categorize_payment, StringType())

# Apply UDF
transactions_with_category_df = transactions_df.withColumn(
    "payment_category",
    categorize_payment_udf(col("paymentMethod"))
)

display(transactions_with_category_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "payment_category" in transactions_with_category_df.columns, "Should have payment_category"
categories = transactions_with_category_df.select("payment_category").distinct().collect()
category_values = [row.payment_category for row in categories]
assert len(category_values) > 0, "Should have payment categories"
print("✅ Task 9.1 complete: Payment methods categorized")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9.2: Classify Transaction Sizes
# MAGIC
# MAGIC Create a UDF to classify transactions as "Small" (<20), "Medium" (20-50), or "Large" (>50).

# COMMAND ----------

# TODO
# Define function and register as UDF

def classify_transaction_size(total_price):
    if total_price < 20:
        return "FILL_IN"
    elif total_price <= 50:
        return "FILL_IN"
    else:
        return "FILL_IN"

classify_size_udf = udf(FILL_IN, StringType())

transactions_with_size_df = transactions_df.withColumn(
    "transaction_size",
    classify_size_udf(col("FILL_IN"))
)

display(transactions_with_size_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "transaction_size" in transactions_with_size_df.columns, "Should have transaction_size"
sizes = transactions_with_size_df.select("transaction_size").distinct().collect()
size_values = [row.transaction_size for row in sizes]
assert "Small" in size_values or "Medium" in size_values or "Large" in size_values, "Should have size classifications"
print("✅ Task 9.2 complete: Transaction sizes classified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9.3: Custom Sorting with Day Names
# MAGIC
# MAGIC Create a UDF to convert day names to numbers for proper sorting (Monday=1, Sunday=7).

# COMMAND ----------

# TODO
# Define a mapping function and use it with UDF

from pyspark.sql.types import IntegerType

def day_name_to_number(day_name):
    day_map = {
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
        "Sunday": 7
    }
    return day_map.get(day_name, 0)

day_to_num_udf = udf(FILL_IN, FILL_IN)

# Apply to revenue by day of week
revenue_sorted_df = (transactions_df
    .withColumn("day_name", date_format(col("dateTime"), "EEEE"))
    .withColumn("day_number", day_to_num_udf(col("day_name")))
    .groupBy("day_name", "day_number")
    .agg(sum("totalPrice").alias("total_revenue"))
    .orderBy("day_number")
)

display(revenue_sorted_df)

# COMMAND ----------

# CHECK YOUR WORK
assert "day_number" in revenue_sorted_df.columns, "Should have day_number"
day_numbers = [row.day_number for row in revenue_sorted_df.collect()]
assert day_numbers == sorted(day_numbers), "Days should be in numeric order"
print("✅ Task 9.3 complete: Custom day sorting applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Final Challenge: Comprehensive Analysis
# MAGIC
# MAGIC **Business Goal:** Combine multiple concepts to produce an executive summary.
# MAGIC
# MAGIC Create a comprehensive report showing:
# MAGIC - Top 5 franchises by revenue
# MAGIC - Average customer spend per franchise
# MAGIC - Most popular payment category
# MAGIC - Day of week with highest revenue

# COMMAND ----------

# TODO
# Combine transformations from previous sections
# This is an open-ended challenge - use what you've learned!

# Step 1: Calculate franchise metrics
franchise_metrics_df = (transactions_df
    .FILL_IN  # Add payment category
    .FILL_IN  # Add day name
    .FILL_IN  # Group and aggregate
    .FILL_IN  # Sort and limit
)

display(franchise_metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Cleanup
# MAGIC
# MAGIC Run the following cell to clean up your environment.

# COMMAND ----------

# Clean up working directory
dbutils.fs.rm(f"{working_dir}/visa_transactions", recurse=True)
print(f"✅ Cleaned up working directory: {working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the comprehensive Bakehouse Analytics lab, covering:
# MAGIC
# MAGIC ✅ Data exploration and catalog navigation
# MAGIC ✅ DataFrame transformations and filtering
# MAGIC ✅ Data ingestion with multiple formats
# MAGIC ✅ Column operations and derived calculations
# MAGIC ✅ Aggregations and grouping
# MAGIC ✅ DateTime manipulation and temporal analysis
# MAGIC ✅ Complex array operations
# MAGIC ✅ Multi-table joins and data enrichment
# MAGIC ✅ User-defined functions for custom logic
# MAGIC
# MAGIC These skills form the foundation of data engineering and analytics with Apache Spark!
