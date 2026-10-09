# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

# ==========================================================
# BRONZE TO SILVER - DATABRICKS VERSION (IMPROVED)
# ==========================================================

# --------- CONFIG ----------
catalog = "ecom-medallion-pipeline"

bronze_orders_tbl = "bronze.orders_bronze"
bronze_products_tbl = "bronze.products_bronze"

silver_orders_tbl = "silver.orders_curated"
silver_reject_tbl = "silver.rejected_orders"

# --------- Set Catalog (important because catalog name has '-') ----------
spark.sql(f"USE CATALOG `{catalog}`")

# Ensure silver schema exists (safe)
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# --------- Pipeline Run ID ----------
pipeline_run_id = str(uuid.uuid4())

# --------- Read Bronze ----------
orders_df = spark.table(bronze_orders_tbl)
products_df = spark.table(bronze_products_tbl)

# --------- Helper: blank strings -> NULL ----------
def blank_as_null(c):
    return F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c))

# ==========================================================
# 1) PREPARE + STANDARDIZE ORDERS
# ==========================================================

# Clean order_date text first (handle 'T' and extra spaces)
orders_prepared_df = (
    orders_df
    .withColumn("order_id", blank_as_null("order_id"))
    .withColumn("customer_id", blank_as_null("customer_id"))
    .withColumn("product_id", blank_as_null("product_id"))
    .withColumn("order_status_std", F.lower(F.trim(F.col("order_status"))))
    .withColumn("quantity_num", F.col("quantity").cast("int"))
    .withColumn("unit_price_num", F.col("unit_price").cast("double"))
    .withColumn("order_date_clean", F.trim(blank_as_null("order_date")))
    .withColumn("order_date_clean", F.regexp_replace(F.col("order_date_clean"), "T", " "))
    # If your data ever has trailing Z, this removes it safely:
    .withColumn("order_date_clean", F.regexp_replace(F.col("order_date_clean"), "Z$", ""))
    .withColumn(
        "order_timestamp",
        F.coalesce(
            # IMPORTANT: 2nd argument must be lit(...) in your environment
            F.try_to_timestamp(F.col("order_date_clean"), F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp(F.col("order_date_clean"), F.lit("yyyy-MM-dd HH:mm"))
        )
    )
)

# ==========================================================
# 2) DUPLICATE CHECK (order_id)
# ==========================================================
order_dup_window = Window.partitionBy("order_id").orderBy(
    F.col("load_date").asc(),
    F.col("source_file_name").asc(),
    F.col("ingestion_timestamp").asc()
)

orders_with_dup_flag_df = orders_prepared_df.withColumn("dup_rank", F.row_number().over(order_dup_window))

# ==========================================================
# 3) DATA QUALITY RULES
# ==========================================================
orders_validated_df = (
    orders_with_dup_flag_df
    .withColumn(
        "rejection_reason",
        F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID"))
         .when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
         .when(F.col("product_id").isNull(), F.lit("NULL_PRODUCT_ID"))
         .when(F.col("order_timestamp").isNull(), F.lit("INVALID_ORDER_DATE"))
         .when(F.col("quantity_num").isNull(), F.lit("NULL_OR_INVALID_QUANTITY"))
         .when(F.col("quantity_num") <= 0, F.lit("INVALID_QUANTITY"))
         .when(F.col("unit_price_num").isNull(), F.lit("NULL_OR_INVALID_UNIT_PRICE"))
         .when(F.col("unit_price_num") <= 0, F.lit("INVALID_UNIT_PRICE"))
         .when(~F.col("order_status_std").isin("completed", "pending", "cancelled"), F.lit("INVALID_ORDER_STATUS"))
         .when((F.col("order_id").isNotNull()) & (F.col("dup_rank") > 1), F.lit("DUPLICATE_ORDER_ID"))
         .otherwise(F.lit(None))
    )
)

rejected_orders_df = (
    orders_validated_df
    .filter(F.col("rejection_reason").isNotNull())
    .select(
        F.to_json(
            F.struct(
                F.col("order_id"),
                F.col("customer_id"),
                F.col("product_id"),
                F.col("order_date"),
                F.col("quantity"),
                F.col("unit_price"),
                F.col("order_status")
            )
        ).alias("raw_record"),
        F.col("rejection_reason"),
        F.col("source_file_name"),
        F.col("load_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

valid_orders_df = orders_validated_df.filter(F.col("rejection_reason").isNull())

# ==========================================================
# 4) CLEAN + DEDUP PRODUCTS
# ==========================================================
products_prepared_df = (
    products_df
    .withColumn("product_id", blank_as_null("product_id"))
    .withColumn("product_name", blank_as_null("product_name"))
    .withColumn("category", blank_as_null("category"))
    .withColumn("brand", blank_as_null("brand"))
)

products_valid_df = (
    products_prepared_df
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("product_name").isNotNull())
    .filter(F.col("category").isNotNull())
    .filter(F.col("brand").isNotNull())
)

product_dup_window = Window.partitionBy("product_id").orderBy(
    F.col("load_date").asc(),
    F.col("source_file_name").asc(),
    F.col("ingestion_timestamp").asc()
)

products_dedup_df = (
    products_valid_df
    .withColumn("prod_rank", F.row_number().over(product_dup_window))
    .filter(F.col("prod_rank") == 1)
    .select(
        F.col("product_id").alias("p_product_id"),
        "product_name",
        "category",
        "brand"
    )
)

# ==========================================================
# 5) ENRICH ORDERS (join products)
# ==========================================================
orders_curated_df = (
    valid_orders_df.alias("o")
    .join(products_dedup_df.alias("p"), F.col("o.product_id") == F.col("p.p_product_id"), "left")
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.product_id").alias("product_id"),
        F.coalesce(F.col("p.product_name"), F.lit("UNKNOWN")).alias("product_name"),
        F.coalesce(F.col("p.category"), F.lit("UNKNOWN")).alias("category"),
        F.coalesce(F.col("p.brand"), F.lit("UNKNOWN")).alias("brand"),
        F.col("o.order_timestamp").alias("order_timestamp"),
        F.to_date(F.col("o.order_timestamp")).alias("order_date"),
        F.col("o.quantity_num").alias("quantity"),
        F.col("o.unit_price_num").alias("unit_price"),
        (F.col("o.quantity_num") * F.col("o.unit_price_num")).cast("double").alias("order_amount"),
        F.col("o.order_status_std").alias("order_status"),
        F.col("o.load_date").alias("load_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

# ==========================================================
# 6) WRITE SILVER (overwrite for tutorial)
# ==========================================================
orders_curated_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("load_date") \
    .saveAsTable(silver_orders_tbl)

rejected_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("load_date") \
    .saveAsTable(silver_reject_tbl)

# ==========================================================
# 7) VALIDATION
# ==========================================================
print("Pipeline Run ID:", pipeline_run_id)

print("Curated Orders Count:")
spark.sql(f"SELECT COUNT(*) AS total_rows FROM {silver_orders_tbl}").show()

print("Rejected Orders Count:")
spark.sql(f"SELECT COUNT(*) AS total_rows FROM {silver_reject_tbl}").show()

print("Rejection Summary:")
spark.sql(f"""
SELECT rejection_reason, COUNT(*) AS cnt
FROM {silver_reject_tbl}
GROUP BY rejection_reason
ORDER BY cnt DESC
""").show()

display(spark.table(silver_orders_tbl))
display(spark.table(silver_reject_tbl))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, COUNT(*) cnt
# MAGIC FROM `ecom-medallion-pipeline`.bronze.orders_bronze
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

