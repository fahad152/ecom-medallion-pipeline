# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, regexp_extract, col
import uuid

# ==========================================================
# RAW TO BRONZE INGESTION - DATABRICKS VERSION
# ==========================================================

# 1. Define source paths (FIXED: your catalog + your folder name)
catalog = "ecom-medallion-pipeline"
load_folder = "load_date=2026-sep-29"

orders_path   = f"/Volumes/{catalog}/data_layer/source_1/{load_folder}/"
products_path = f"/Volumes/{catalog}/data_layer/source_2/{load_folder}/"

# 2. Generate pipeline run id
pipeline_run_id = str(uuid.uuid4())

# 3. Read raw JSON files from volumes
orders_df = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .load(orders_path)
    .select("*", "_metadata")
)

products_df = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .load(products_path)
    .select("*", "_metadata")
)

# 4. Add metadata columns (FIXED: flexible regex for load_date)
orders_bronze_df = (
    orders_df
    .withColumn("source_file_name", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
    .withColumn(
        "load_date",
        regexp_extract(col("_metadata.file_path"), r"load_date=([^/]+)", 1)
    )
    .drop("_metadata")
)

products_bronze_df = (
    products_df
    .withColumn("source_file_name", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
    .withColumn(
        "load_date",
        regexp_extract(col("_metadata.file_path"), r"load_date=([^/]+)", 1)
    )
    .drop("_metadata")
)

# 5. Write Bronze tables (FIXED: your catalog + _new tables to avoid duplicates)
orders_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .saveAsTable(f"`{catalog}`.bronze.orders_bronze_new")

products_bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .saveAsTable(f"`{catalog}`.bronze.products_bronze_new")

# 6. Validation (FIXED: correct table names with backticks)
print("Pipeline Run ID:", pipeline_run_id)

print("Orders Bronze Count:")
spark.sql(f"SELECT COUNT(*) AS total_rows FROM `{catalog}`.bronze.orders_bronze_new").show()

print("Products Bronze Count:")
spark.sql(f"SELECT COUNT(*) AS total_rows FROM `{catalog}`.bronze.products_bronze_new").show()

print("Orders Bronze Preview:")
display(spark.table(f"`{catalog}`.bronze.orders_bronze_new"))

print("Products Bronze Preview:")
display(spark.table(f"`{catalog}`.bronze.products_bronze_new"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN `ecom-medallion-pipeline`.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders_bronze' AS tbl, COUNT(*) AS cnt
# MAGIC FROM `ecom-medallion-pipeline`.bronze.orders_bronze
# MAGIC UNION ALL
# MAGIC SELECT 'orders_bronze_new' AS tbl, COUNT(*) AS cnt
# MAGIC FROM `ecom-medallion-pipeline`.bronze.orders_bronze_new
# MAGIC UNION ALL
# MAGIC SELECT 'products_bronze_new' AS tbl, COUNT(*) AS cnt
# MAGIC FROM `ecom-medallion-pipeline`.bronze.products_bronze_new;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN `ecom-medallion-pipeline`.bronze;
# MAGIC SELECT COUNT(*) FROM `ecom-medallion-pipeline`.bronze.orders_bronze;
# MAGIC SELECT COUNT(*) FROM `ecom-medallion-pipeline`.bronze.products_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders_bronze'   AS tbl, COUNT(*) AS cnt
# MAGIC FROM `ecom-medallion-pipeline`.bronze.orders_bronze
# MAGIC UNION ALL
# MAGIC SELECT 'products_bronze' AS tbl, COUNT(*) AS cnt
# MAGIC FROM `ecom-medallion-pipeline`.bronze.products_bronze;

# COMMAND ----------

