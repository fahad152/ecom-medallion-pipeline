# Databricks notebook source
from pyspark.sql import functions as F
import uuid

# ---------------- CONFIG ----------------
catalog = "ecom-medallion-pipeline"

# Use catalog (because catalog name has '-')
spark.sql(f"USE CATALOG `{catalog}`")

# Ensure gold schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# ---------------- Pipeline Run ID ----------------
pipeline_run_id = str(uuid.uuid4())

# ---------------- Read Silver curated orders ----------------
orders_curated_df = spark.table("silver.orders_curated")

# ---------------- Safety filters ----------------
gold_base_df = (
    orders_curated_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("order_date").isNotNull())
    .filter(F.col("quantity").isNotNull())
    .filter(F.col("unit_price").isNotNull())
    .filter(F.col("order_amount").isNotNull())
)

# ---------------- Gold 1: Daily Product Sales ----------------
daily_product_sales_df = (
    gold_base_df
    .groupBy("order_date", "product_id", "product_name", "category")
    .agg(
        F.countDistinct("order_id").cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_quantity"),
        F.round(F.sum("order_amount"), 2).cast("double").alias("total_sales")
    )
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# ---------------- Gold 2: Category Sales ----------------
category_sales_df = (
    gold_base_df
    .groupBy("order_date", "category")
    .agg(
        F.countDistinct("order_id").cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_quantity"),
        F.round(F.sum("order_amount"), 2).cast("double").alias("total_sales")
    )
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# ---------------- Write Gold tables ----------------
daily_product_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_date") \
    .saveAsTable("gold.daily_product_sales")

category_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_date") \
    .saveAsTable("gold.category_sales")

# ---------------- Validation ----------------
print("Pipeline Run ID:", pipeline_run_id)

print("Daily Product Sales Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM gold.daily_product_sales").show()

print("Category Sales Count:")
spark.sql("SELECT COUNT(*) AS total_rows FROM gold.category_sales").show()

print("Daily Product Sales Preview:")
display(spark.table("gold.daily_product_sales"))

print("Category Sales Preview:")
display(spark.table("gold.category_sales"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `ecom-medallion-pipeline`;
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT ROUND(SUM(order_amount),2) FROM silver.orders_curated) AS silver_total_sales,
# MAGIC   (SELECT ROUND(SUM(total_sales),2) FROM gold.daily_product_sales) AS gold_daily_total_sales,
# MAGIC   (SELECT ROUND(SUM(total_sales),2) FROM gold.category_sales) AS gold_category_total_sales;

# COMMAND ----------

