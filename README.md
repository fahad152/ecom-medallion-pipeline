# ecom-medallion-pipeline (Databricks)

An end-to-end **Medallion Architecture** pipeline on Databricks using **Unity Catalog Volumes**:

**Bronze → Silver → Gold**

## What this project does

### Bronze (Raw ingestion)
- Reads raw JSON files from Unity Catalog Volumes
- Adds ingestion metadata (source file, ingestion timestamp, pipeline run id, load_date)
- Writes to Delta tables:
  - `ecom-medallion-pipeline.bronze.orders_bronze`
  - `ecom-medallion-pipeline.bronze.products_bronze`

### Silver (Data quality + curation)
- Applies data quality rules on orders (null checks, invalid dates, invalid quantity/price, invalid status)
- Detects and rejects duplicate `order_id`
- Cleans & deduplicates product master
- Enriches valid orders with product attributes
- Writes:
  - `ecom-medallion-pipeline.silver.orders_curated`
  - `ecom-medallion-pipeline.silver.rejected_orders`

### Gold (Analytics aggregates)
- Aggregates curated orders into business-friendly tables:
  - `ecom-medallion-pipeline.gold.daily_product_sales`
  - `ecom-medallion-pipeline.gold.category_sales`

---

## Catalog & Data Layout (Unity Catalog)

**Catalog:** `ecom-medallion-pipeline`

### Volumes (raw sources)
Orders:
- `/Volumes/ecom-medallion-pipeline/data_layer/source_1/load_date=YYYY-MMM-DD/`

Products:
- `/Volumes/ecom-medallion-pipeline/data_layer/source_2/load_date=YYYY-MMM-DD/`

Example folder used during development:
- `load_date=2026-sep-29/`

---

## Scripts in this repo

- `01-Source_to_bronze.py` — raw JSON → bronze Delta tables
- `02_bronze_to_silver.py` — bronze → silver (quality + curation + rejects)
- `03_silver_to_gold.py` — silver → gold aggregations

> Note: These scripts are exported from Databricks notebooks as `.py` for version control.

---

## How to run (manual)

Run in this order on Databricks (Serverless or a cluster with UC enabled):

1. `01-Source_to_bronze.py`
2. `02_bronze_to_silver.py`
3. `03_silver_to_gold.py`

---

## Orchestration (Databricks Workflows / Jobs)

A Databricks Job orchestrates the pipeline with 3 tasks:

1. **bronze_task** → runs `01-Source_to_bronze`
2. **silver_task** → runs `02_bronze_to_silver` (depends on bronze_task)
3. **gold_task** → runs `03_silver_to_gold` (depends on silver_task)

Expected flow:
`bronze_task → silver_task → gold_task`

---

## Output tables

### Bronze
- `ecom-medallion-pipeline.bronze.orders_bronze`
- `ecom-medallion-pipeline.bronze.products_bronze`

### Silver
- `ecom-medallion-pipeline.silver.orders_curated`
- `ecom-medallion-pipeline.silver.rejected_orders`

### Gold
- `ecom-medallion-pipeline.gold.daily_product_sales`
- `ecom-medallion-pipeline.gold.category_sales`

---

## Notes / Tips

- The catalog name contains a dash (`ecom-medallion-pipeline`), so in SQL you may need backticks:
  - `` `ecom-medallion-pipeline`.bronze.orders_bronze ``
- This repo intentionally contains **code only** (no raw JSON files, no credentials).
- If the workflow fails due to "TABLE_ALREADY_EXISTS", ensure there are no one-time cleanup SQL cells
  (e.g., `ALTER TABLE ... RENAME TO ...`) included in the production notebooks.

---

## License
For learning/demo purposes.
