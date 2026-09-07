# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Schema Evolution Demo — Scenario 1: New Data Source Onboarding
# MAGIC
# MAGIC **Story:** ABMB launches a new digital initiative. The CRM exports a new customer file
# MAGIC with 2 additional attributes. We show how AutoLoader handles this automatically —
# MAGIC no pipeline downtime, no schema conflicts, new columns appear in the bronze table instantly.

# COMMAND ----------
# MAGIC %md ## ⚙️ RESET DEMO — Run this cell first to start fresh

# COMMAND ----------
# RESET CELL — idempotent, run before every demo
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_customers_evolution")
dbutils.fs.rm(f"{VOLUME_DATA}/schema_evolution/", recurse=True)
dbutils.fs.rm(f"{VOLUME_DATA}/_schemas/customers_evolution/", recurse=True)
dbutils.fs.mkdirs(f"{VOLUME_DATA}/schema_evolution/v1/")
dbutils.fs.mkdirs(f"{VOLUME_DATA}/schema_evolution/v2/")
print("✅ Reset complete — ready for demo")

# COMMAND ----------
# MAGIC %md ## Act 1: Load V1 schema — 43 columns (baseline)

# COMMAND ----------
import pandas as pd, random
from pyspark.sql.functions import current_timestamp, col
random.seed(42)

v1_records = [{"cif_number": f"CIF{i:06d}", "legal_name": f"Customer {i}",
               "annual_income_amount": random.randint(24000, 200000),
               "risk_rating": random.choice(["low","medium","high"])}
              for i in range(1, 101)]
df_v1 = pd.DataFrame(v1_records)
df_v1.to_csv(f"/dbfs{VOLUME_DATA}/schema_evolution/v1/customers_v1.csv", index=False)
print(f"✅ Written {len(df_v1)} rows with {len(df_v1.columns)} columns")

# COMMAND ----------
(spark.readStream
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true").option("header", "true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/customers_evolution")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{VOLUME_DATA}/schema_evolution/v1/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/customers_evolution/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{FULL_SCHEMA}.bronze_customers_evolution")
).awaitTermination()
print("✅ V1 loaded")

# COMMAND ----------
# Show current schema — columns + _rescued_data, _ingest_timestamp
display(spark.sql(f"DESCRIBE {FULL_SCHEMA}.bronze_customers_evolution"))

# COMMAND ----------
# MAGIC %md ## Act 2: New source onboarded — V2 adds occupation_code + is_shariah_preferred

# COMMAND ----------
v2_records = [{"cif_number": f"CIF{i:06d}", "legal_name": f"Customer {i}",
               "annual_income_amount": random.randint(24000, 200000),
               "risk_rating": random.choice(["low","medium","high"]),
               "occupation_code": random.choice(["K","M","J","A","B"]),   # NEW
               "is_shariah_preferred": random.choice([True, False])}       # NEW
              for i in range(1001, 1051)]
df_v2 = pd.DataFrame(v2_records)
df_v2.to_csv(f"/dbfs{VOLUME_DATA}/schema_evolution/v2/customers_v2.csv", index=False)
print(f"✅ Written {len(df_v2)} rows with {len(df_v2.columns)} columns (2 NEW fields)")

# COMMAND ----------
# Same AutoLoader call — addNewColumns mode handles schema change automatically, no code change!
(spark.readStream
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true").option("header", "true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/customers_evolution")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{VOLUME_DATA}/schema_evolution/v2/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/customers_evolution/checkpoint")
    .trigger(availableNow=True).option("mergeSchema", "true")
    .toTable(f"{FULL_SCHEMA}.bronze_customers_evolution")
).awaitTermination()
print("✅ V2 loaded — new columns automatically added")

# COMMAND ----------
# MAGIC %md ## Act 3: Validate — new columns appear, old rows have NULL for new fields

# COMMAND ----------
print(f"Total rows: {spark.table(f'{FULL_SCHEMA}.bronze_customers_evolution').count()}")
display(spark.sql(f"""
SELECT cif_number, annual_income_amount, occupation_code, is_shariah_preferred, _ingest_timestamp
FROM {FULL_SCHEMA}.bronze_customers_evolution
ORDER BY _ingest_timestamp DESC LIMIT 20
"""))
# Key insight: V1 rows show NULL for occupation_code and is_shariah_preferred — schema evolved gracefully

# COMMAND ----------
# MAGIC %md
# MAGIC ### What just happened?
# MAGIC
# MAGIC | | V1 rows (CIF000001–CIF000100) | V2 rows (CIF001001–CIF001050) |
# MAGIC |---|---|---|
# MAGIC | `occupation_code` | **NULL** | populated |
# MAGIC | `is_shariah_preferred` | **NULL** | populated |
# MAGIC | Pipeline downtime | **zero** | — |
# MAGIC
# MAGIC AutoLoader's `addNewColumns` mode detected the new columns in V2, updated the schema
# MAGIC automatically, and backfilled `NULL` for all existing rows.
# MAGIC **No manual ALTER TABLE. No pipeline restart. No data loss.**
# MAGIC
# MAGIC > **Presenter note:** This directly addresses ABMB's concern about frequent CRM schema
# MAGIC > changes breaking their overnight batch pipelines. The SDP pattern absorbs new attributes
# MAGIC > without any engineering intervention.
