# Databricks notebook source
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Schema Evolution Demo — New Data Source Onboarding
# MAGIC
# MAGIC **Story:** DBX Bank launches a new digital initiative. The CRM exports a new customer file
# MAGIC with 2 additional attributes. We show how Databricks handles this automatically —
# MAGIC no pipeline downtime, no schema conflicts, new columns appear in the bronze table instantly.

# COMMAND ----------
# MAGIC %md ## ⚙️ RESET — Run this cell first to start fresh

# COMMAND ----------
import os, shutil, pandas as pd, random
from pyspark.sql.functions import current_timestamp, lit

random.seed(42)

# Clean up previous run
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_customers_evolution")
for p in [f"{VOLUME_DATA}/schema_evolution", f"{VOLUME_DATA}/_schemas/customers_evolution"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs(f"{VOLUME_DATA}/schema_evolution/v1", exist_ok=True)
os.makedirs(f"{VOLUME_DATA}/schema_evolution/v2", exist_ok=True)
print("✅ Reset complete — ready for demo")

# COMMAND ----------
# MAGIC %md ## Act 1: Load V1 schema — baseline (4 columns)

# COMMAND ----------
v1_records = [{"cif_number": f"CIF{i:06d}", "legal_name": f"Customer {i}",
               "annual_income_amount": random.randint(24000, 200000),
               "risk_rating": random.choice(["low","medium","high"])}
              for i in range(1, 101)]
df_v1 = pd.DataFrame(v1_records)
df_v1.to_csv(f"{VOLUME_DATA}/schema_evolution/v1/customers_v1.csv", index=False)
print(f"✅ Written {len(df_v1)} rows with {len(df_v1.columns)} columns to v1/")

# COMMAND ----------
# Ingest V1 with batch read — rescuedDataColumn captures any unexpected fields
spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("rescuedDataColumn", "_rescued_data") \
    .load(f"{VOLUME_DATA}/schema_evolution/v1/") \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.bronze_customers_evolution")

print(f"✅ V1 loaded — {spark.table(f'{FULL_SCHEMA}.bronze_customers_evolution').count()} rows")
display(spark.sql(f"DESCRIBE {FULL_SCHEMA}.bronze_customers_evolution"))

# COMMAND ----------
# MAGIC %md ## Act 2: New source onboarded — V2 adds `occupation_code` + `is_shariah_preferred`

# COMMAND ----------
v2_records = [{"cif_number": f"CIF{i:06d}", "legal_name": f"Customer {i}",
               "annual_income_amount": random.randint(24000, 200000),
               "risk_rating": random.choice(["low","medium","high"]),
               "occupation_code": random.choice(["K","M","J","A","B"]),   # ← NEW
               "is_shariah_preferred": random.choice([True, False])}       # ← NEW
              for i in range(1001, 1051)]
df_v2 = pd.DataFrame(v2_records)
df_v2.to_csv(f"{VOLUME_DATA}/schema_evolution/v2/customers_v2.csv", index=False)
print(f"✅ Written {len(df_v2)} rows with {len(df_v2.columns)} columns to v2/ (2 NEW fields)")

# COMMAND ----------
# Same read pattern — mergeSchema handles new columns automatically, no code change!
spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("rescuedDataColumn", "_rescued_data") \
    .load(f"{VOLUME_DATA}/schema_evolution/v2/") \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.bronze_customers_evolution")

print("✅ V2 loaded with mergeSchema=true — new columns automatically added to existing table")

# COMMAND ----------
# MAGIC %md ## Act 3: Validate — new columns appear, V1 rows have NULL for new fields

# COMMAND ----------
total = spark.table(f"{FULL_SCHEMA}.bronze_customers_evolution").count()
print(f"Total rows: {total}  (expected 150 = 100 V1 + 50 V2)")

display(spark.sql(f"""
SELECT cif_number, annual_income_amount, occupation_code, is_shariah_preferred, _ingest_timestamp
FROM   {FULL_SCHEMA}.bronze_customers_evolution
ORDER BY cif_number
LIMIT  20
"""))
# Key insight: V1 rows (CIF000001–100) show NULL for the 2 new columns — graceful schema evolution

# COMMAND ----------
# Summary assertion
v1_nulls = spark.sql(f"""
    SELECT COUNT(*) FROM {FULL_SCHEMA}.bronze_customers_evolution
    WHERE occupation_code IS NULL""").collect()[0][0]
v2_populated = spark.sql(f"""
    SELECT COUNT(*) FROM {FULL_SCHEMA}.bronze_customers_evolution
    WHERE occupation_code IS NOT NULL""").collect()[0][0]

print(f"\n📊 Schema Evolution Results:")
print(f"  V1 rows (occupation_code IS NULL):     {v1_nulls}  ← schema gracefully extended")
print(f"  V2 rows (occupation_code IS NOT NULL): {v2_populated}  ← new attributes populated")
assert v1_nulls == 100 and v2_populated == 50, "❌ Unexpected row counts!"
print("\n✅ VALIDATION PASSED — schema evolved with zero downtime and no data loss")

# COMMAND ----------
# MAGIC %md
# MAGIC ### What just happened?
# MAGIC
# MAGIC | | V1 rows (CIF000001–100) | V2 rows (CIF001001–1050) |
# MAGIC |---|---|---|
# MAGIC | `occupation_code` | **NULL** | populated |
# MAGIC | `is_shariah_preferred` | **NULL** | populated |
# MAGIC | Pipeline downtime | **zero** | — |
# MAGIC
# MAGIC `mergeSchema=true` detected the new columns in V2, updated the Delta table schema
# MAGIC automatically, and backfilled `NULL` for all existing rows.
# MAGIC **No manual ALTER TABLE. No pipeline restart. No data loss.**
# MAGIC
# MAGIC > **Presenter note:** This directly addresses DBX Bank's concern about frequent CRM schema
# MAGIC > changes breaking their overnight batch pipelines. The pattern absorbs new attributes
# MAGIC > without any engineering intervention.
