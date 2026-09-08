# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Rescued Data Column Demo — Handling Malformed Values
# MAGIC
# MAGIC **Story:** The Cards system occasionally sends malformed data — string where number expected,
# MAGIC invalid dates, unknown extra columns. AutoLoader's `_rescued_data` column captures all
# MAGIC malformed values as JSON, preserving the good rows while quarantining bad ones.
# MAGIC The SDP pipeline then routes these rows to a quarantine table via:
# MAGIC `CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION QUARANTINE`

# COMMAND ----------
# MAGIC %md ## ⚙️ RESET — Run first

# COMMAND ----------
# RESET CELL — idempotent, run before every demo
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_rescued_demo")
dbutils.fs.rm(f"{VOLUME_DATA}/rescued_demo/", recurse=True)
dbutils.fs.rm(f"{VOLUME_DATA}/_schemas/rescued_demo/", recurse=True)
dbutils.fs.mkdirs(f"{VOLUME_DATA}/rescued_demo/clean/")
dbutils.fs.mkdirs(f"{VOLUME_DATA}/rescued_demo/malformed/")
print("✅ Reset complete")

# COMMAND ----------
# MAGIC %md ## Step 1: Clean ingestion baseline

# COMMAND ----------
import pandas as pd
from pyspark.sql.functions import current_timestamp

clean_data = pd.DataFrame([
    {"txn_id": f"T{i:06d}", "amount": round(i * 100.50, 2),
     "posting_date": "2026-09-01", "status": "settled"}
    for i in range(1, 21)
])
clean_data.to_csv(f"/dbfs{VOLUME_DATA}/rescued_demo/clean/txns_clean.csv", index=False)

(spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes","true").option("header","true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/rescued_demo")
    .option("rescuedDataColumn","_rescued_data")
    .load(f"{VOLUME_DATA}/rescued_demo/clean/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/rescued_demo/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{FULL_SCHEMA}.bronze_rescued_demo")
).awaitTermination()

display(spark.sql(f"SELECT *, _rescued_data FROM {FULL_SCHEMA}.bronze_rescued_demo"))
print("✅ All rows clean — _rescued_data is NULL for every row")

# COMMAND ----------
# MAGIC %md ## Step 2: Inject malformed rows

# COMMAND ----------
malformed_data = pd.DataFrame([
    {"txn_id": "T000021", "amount": "TWO HUNDRED",   # ← string where DECIMAL expected
     "posting_date": "2026-09-02", "status": "settled"},
    {"txn_id": "T000022", "amount": 500.00,
     "posting_date": "not-a-date",                     # ← invalid date
     "status": "settled"},
    {"txn_id": "T000023", "amount": 750.00,
     "posting_date": "2026-09-02", "status": "settled",
     "fraud_score": 0.95},                              # ← unknown extra column
])
malformed_data.to_csv(f"/dbfs{VOLUME_DATA}/rescued_demo/malformed/txns_bad.csv", index=False)

(spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes","true").option("header","true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/rescued_demo")
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
    .option("rescuedDataColumn","_rescued_data")
    .load(f"{VOLUME_DATA}/rescued_demo/malformed/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/rescued_demo/checkpoint")
    .trigger(availableNow=True).option("mergeSchema","true")
    .toTable(f"{FULL_SCHEMA}.bronze_rescued_demo")
).awaitTermination()

# COMMAND ----------
# MAGIC %md ## Step 3: Inspect rescued data — malformed values captured as JSON

# COMMAND ----------
display(spark.sql(f"""
SELECT txn_id, amount, posting_date, status, _rescued_data
FROM {FULL_SCHEMA}.bronze_rescued_demo
WHERE _rescued_data IS NOT NULL
"""))
# Expected: rows T000021, T000022, T000023 each show _rescued_data with the bad field as JSON
# T000021: {{"amount": "TWO HUNDRED"}}
# T000022: {{"posting_date": "not-a-date"}}
# T000023: {{"fraud_score": "0.95"}}  ← captured even though it was an unknown column

# COMMAND ----------
# MAGIC %md
# MAGIC ### How the SDP pipeline quarantines these rows downstream
# MAGIC
# MAGIC In the silver DLT pipeline, a single DQ expectation routes all rescued rows to quarantine
# MAGIC automatically — **no custom error-handling code required**:
# MAGIC
# MAGIC ```sql
# MAGIC CONSTRAINT no_rescued_data
# MAGIC   EXPECT (_rescued_data IS NULL)
# MAGIC   ON VIOLATION QUARANTINE
# MAGIC ```
# MAGIC
# MAGIC | Row | Fault | `_rescued_data` | Silver outcome |
# MAGIC |---|---|---|---|
# MAGIC | T000021 | `amount = "TWO HUNDRED"` | `{"amount": "TWO HUNDRED"}` | **quarantined** |
# MAGIC | T000022 | `posting_date = "not-a-date"` | `{"posting_date": "not-a-date"}` | **quarantined** |
# MAGIC | T000023 | extra column `fraud_score` | `{"fraud_score": "0.95"}` | **quarantined** |
# MAGIC | T000001–T000020 | none | NULL | ✅ passes to silver |
# MAGIC
# MAGIC **Bronze never loses a row.** Malformed values are preserved as JSON in `_rescued_data`,
# MAGIC giving the DQ team full visibility and the ability to reprocess once the source is fixed.
# MAGIC
# MAGIC > **Presenter note:** This directly addresses DBX Bank's concern about data quality from
# MAGIC > legacy Cards and GL systems. The SDP pattern gives ops teams a quarantine table they
# MAGIC > can monitor and replay — without any custom error-handling code in the pipeline.
