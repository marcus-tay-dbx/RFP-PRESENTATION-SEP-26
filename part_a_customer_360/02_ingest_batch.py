# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # AutoLoader Batch Ingestion — Bronze Layer
# MAGIC
# MAGIC **Scenario 2:** Every night at 23:00, Core Banking pushes CSV extracts to the landing zone.
# MAGIC AutoLoader detects new files, validates schema, and loads to bronze Delta tables.
# MAGIC
# MAGIC Key AutoLoader options used:
# MAGIC - `cloudFiles.schemaEvolutionMode = addNewColumns` — new columns in source automatically added to bronze
# MAGIC - `rescuedDataColumn = _rescued_data` — malformed values captured as JSON, no row loss
# MAGIC - `trigger(availableNow=True)` — processes all pending files then terminates (batch semantics)

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, col

def load_bronze(source_path, table_name, schema_loc):
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{schema_loc}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{FULL_SCHEMA}.{table_name}")
    ).awaitTermination()
    print(f"✅ {table_name} loaded")

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/customer_master/",
    table_name="bronze_customer_master",
    schema_loc=f"{VOLUME_DATA}/_schemas/customer_master"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/accounts/",
    table_name="bronze_core_banking_accounts",
    schema_loc=f"{VOLUME_DATA}/_schemas/accounts"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/loans/",
    table_name="bronze_loans",
    schema_loc=f"{VOLUME_DATA}/_schemas/loans"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/transactions/",
    table_name="bronze_core_banking_txn",
    schema_loc=f"{VOLUME_DATA}/_schemas/transactions"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/cards/",
    table_name="bronze_cards_txn",
    schema_loc=f"{VOLUME_DATA}/_schemas/cards"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/digital_events/",
    table_name="bronze_digital_events",
    schema_loc=f"{VOLUME_DATA}/_schemas/digital_events"
)

# COMMAND ----------
# MAGIC %md ### Validate bronze tables

# COMMAND ----------
for tbl, expected_min in [
    ("bronze_customer_master",      990),
    ("bronze_core_banking_accounts",1100),
    ("bronze_loans",                500),
    ("bronze_core_banking_txn",     45000),
    ("bronze_cards_txn",            9000),
    ("bronze_digital_events",       8000),
]:
    cnt = spark.table(f"{FULL_SCHEMA}.{tbl}").count()
    assert cnt >= expected_min, f"{tbl}: {cnt} rows (expected >= {expected_min})"
    print(f"✅ {tbl}: {cnt} rows")
