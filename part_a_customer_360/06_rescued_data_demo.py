# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../shared/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Rescued Data Column Demo — Handling Malformed Values
# MAGIC
# MAGIC **Story:** The Cards system occasionally sends malformed data — a string where a number
# MAGIC is expected, an invalid date, an unknown extra column. AutoLoader's `_rescued_data`
# MAGIC column captures all malformed values as JSON, preserving every row while quarantining
# MAGIC the bad fields. Downstream SDP pipelines then route these rows via:
# MAGIC
# MAGIC ```python
# MAGIC @dp.expect("no_rescued_data", "_rescued_data IS NULL")   # ALLOW — flags but keeps
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## ⚙️ RESET — Run first

# COMMAND ----------

import os, shutil, pandas as pd
from pyspark.sql.functions import current_timestamp

spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_rescued_demo")
for p in [f"{VOLUME_DATA}/rescued_demo"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs(f"{VOLUME_DATA}/rescued_demo/clean",    exist_ok=True)
os.makedirs(f"{VOLUME_DATA}/rescued_demo/malformed", exist_ok=True)
print("✅ Reset complete")

# COMMAND ----------

# MAGIC %md ## Step 1: Clean ingestion baseline — all 20 rows pass, `_rescued_data` = NULL

# COMMAND ----------

clean_data = pd.DataFrame([
    {"txn_id": f"T{i:06d}", "amount": round(i * 100.50, 2),
     "posting_date": "2026-09-01", "status": "settled"}
    for i in range(1, 21)
])
clean_data.to_csv(f"{VOLUME_DATA}/rescued_demo/clean/txns_clean.csv", index=False)

spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("rescuedDataColumn", "_rescued_data") \
    .load(f"{VOLUME_DATA}/rescued_demo/clean/") \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.bronze_rescued_demo")

rescued_count = spark.sql(f"SELECT COUNT(*) FROM {FULL_SCHEMA}.bronze_rescued_demo WHERE _rescued_data IS NOT NULL").collect()[0][0]
print(f"✅ {spark.table(f'{FULL_SCHEMA}.bronze_rescued_demo').count()} rows loaded")
print(f"   _rescued_data IS NOT NULL: {rescued_count}  ← should be 0")
display(spark.sql(f"SELECT txn_id, amount, posting_date, status, _rescued_data FROM {FULL_SCHEMA}.bronze_rescued_demo"))

# COMMAND ----------

# MAGIC %md ## Step 2: Inject 3 malformed rows — each has a different data quality problem
# MAGIC
# MAGIC **Note on implementation:** In production, AutoLoader streaming rescues values automatically
# MAGIC during ingestion (bad field → NULL in column, raw value → JSON in `_rescued_data`).
# MAGIC For this demo we construct the rescued rows explicitly — showing the exact state the
# MAGIC bronze table would be in after AutoLoader processes malformed data.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# These rows represent what AutoLoader produces after rescuing malformed fields:
#   T000021: amount="TWO HUNDRED" → amount=NULL, _rescued_data={"amount":"TWO HUNDRED"}
#   T000022: posting_date="not-a-date" → posting_date=NULL, _rescued_data={"posting_date":"not-a-date"}
#   T000023: unknown column fraud_score → _rescued_data={"fraud_score":"0.95"}
rescued_rows = spark.createDataFrame([
    ("T000021", None,   "2026-09-02", "settled", '{"amount": "TWO HUNDRED"}'),
    ("T000022", 500.00, None,         "settled", '{"posting_date": "not-a-date"}'),
    ("T000023", 750.00, "2026-09-02", "settled", '{"fraud_score": "0.95"}'),
], ["txn_id", "amount", "posting_date", "status", "_rescued_data"]) \
    .withColumn("_ingest_timestamp", current_timestamp())

rescued_rows.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.bronze_rescued_demo")

print(f"✅ Total rows now: {spark.table(f'{FULL_SCHEMA}.bronze_rescued_demo').count()}  (expected 23)")

# COMMAND ----------

# MAGIC %md ## Step 3: Inspect rescued rows — malformed values captured as JSON, row preserved

# COMMAND ----------

display(spark.sql(f"""
SELECT txn_id, amount, posting_date, status, _rescued_data
FROM   {FULL_SCHEMA}.bronze_rescued_demo
WHERE  _rescued_data IS NOT NULL
ORDER BY txn_id
"""))
# Expected:
# T000021: amount=NULL,         _rescued_data={{"amount":"TWO HUNDRED"}}
# T000022: posting_date=NULL,   _rescued_data={{"posting_date":"not-a-date"}}
# T000023: all cols populated,  _rescued_data={{"fraud_score":"0.95"}}  ← unknown col rescued

# COMMAND ----------

# Validate
rescued = spark.sql(f"""
    SELECT txn_id, _rescued_data
    FROM   {FULL_SCHEMA}.bronze_rescued_demo
    WHERE  _rescued_data IS NOT NULL
    ORDER BY txn_id
""").collect()

print("📊 Rescued Data Validation:")
for row in rescued:
    print(f"  {row.txn_id}: {row._rescued_data}")

assert len(rescued) == 3, f"Expected 3 rescued rows, got {len(rescued)}"
assert any("T000021" in r.txn_id for r in rescued), "T000021 not rescued"
assert any("T000022" in r.txn_id for r in rescued), "T000022 not rescued"
assert any("T000023" in r.txn_id for r in rescued), "T000023 not rescued"
print("\n✅ VALIDATION PASSED — 3 malformed rows captured in _rescued_data, 0 rows lost")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How the SDP pipeline handles these downstream
# MAGIC
# MAGIC In the silver DLT pipeline, a single DQ expectation routes rescued rows automatically:
# MAGIC
# MAGIC ```python
# MAGIC @dp.expect("no_rescued_data", "_rescued_data IS NULL")  # ALLOW — passes through, flagged
# MAGIC ```
# MAGIC
# MAGIC | Row | Fault | `_rescued_data` | Silver outcome |
# MAGIC |---|---|---|---|
# MAGIC | T000021 | `amount = "TWO HUNDRED"` | `{"amount": "TWO HUNDRED"}` | flagged in DQ metrics |
# MAGIC | T000022 | `posting_date = "not-a-date"` | `{"posting_date": "not-a-date"}` | flagged in DQ metrics |
# MAGIC | T000023 | extra column `fraud_score` | `{"fraud_score": "0.95"}` | flagged in DQ metrics |
# MAGIC | T000001–T000020 | none | NULL | ✅ clean pass |
# MAGIC
# MAGIC **Bronze never loses a row.** Malformed values are preserved as JSON in `_rescued_data`,
# MAGIC giving the ops team full visibility and the ability to reprocess once the source is fixed.
# MAGIC
# MAGIC > **Presenter note:** This addresses DBX Bank's concern about data quality from legacy
# MAGIC > Cards and GL systems. No custom error-handling code required — the SDP pattern provides
# MAGIC > a built-in quarantine with full audit trail.