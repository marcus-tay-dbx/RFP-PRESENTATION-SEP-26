# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Near-Real-Time AutoLoader Ingestion — Bronze Layer
# MAGIC
# MAGIC **CDC Simulation:** CTOS/CCRIS and Telco partner push JSON updates every 30 seconds to the
# MAGIC landing zone. AutoLoader micro-batch processing picks up new files within the trigger interval
# MAGIC and appends to bronze Delta tables.
# MAGIC
# MAGIC Key AutoLoader options used:
# MAGIC - `cloudFiles.format = json` — parses JSON payloads from credit bureau and telco partners
# MAGIC - `multiLine = true` — supports multi-line JSON documents in each file
# MAGIC - `cloudFiles.schemaEvolutionMode = addNewColumns` — partner schema additions land automatically
# MAGIC - `rescuedDataColumn = _rescued_data` — unexpected fields captured, no row loss
# MAGIC - `trigger(processingTime="30 seconds")` — micro-batch: stream stays alive for continuous demo

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, col

def load_bronze_json(source_path, table_name, schema_loc):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
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
            .trigger(processingTime="30 seconds")
            .toTable(f"{FULL_SCHEMA}.{table_name}")
    )

# COMMAND ----------
# MAGIC %md ### Start near-RT streams

# COMMAND ----------
credit_query = load_bronze_json(
    source_path=f"{VOLUME_DATA}/nearrt/credit_bureau/",
    table_name="bronze_credit_bureau",
    schema_loc=f"{VOLUME_DATA}/_schemas/credit_bureau"
)
print("✅ bronze_credit_bureau stream started")

# COMMAND ----------
telco_query = load_bronze_json(
    source_path=f"{VOLUME_DATA}/nearrt/telco_events/",
    table_name="bronze_telco_events",
    schema_loc=f"{VOLUME_DATA}/_schemas/telco_events"
)
print("✅ bronze_telco_events stream started")

# COMMAND ----------
# MAGIC %md ### Monitor active streams

# COMMAND ----------
for stream in spark.streams.active:
    print(f"Stream: {stream.name}  |  Status: {stream.status['message']}  |  isActive: {stream.isActive}")

# COMMAND ----------
# MAGIC %md ### Stop streams (run when demo is complete)

# COMMAND ----------
credit_query.stop()
telco_query.stop()
print("✅ Near-RT streams stopped")
