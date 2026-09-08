# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Batch Inference — Score All Alliance Bank Customers
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Loads the `@champion` model (promotes `@dev` → `@champion` if none exists)
# MAGIC 2. Scores all customers via `fe.score_batch()` (feature lookups resolved automatically)
# MAGIC 3. Appends `model_version` and `scored_at` columns
# MAGIC 4. Writes to `da.inference_log_table` with Change Data Feed enabled
# MAGIC 5. Injects 5 synthetic drift rows (high-confidence INVESTMENT customers)
# MAGIC 6. Creates an InferenceLog Lakehouse Monitor (classification, daily granularity)

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
import pandas as pd
import datetime

fe     = FeatureEngineeringClient()
client = MlflowClient()

# COMMAND ----------
# MAGIC %md ## 1. Resolve Champion Model (promote @dev if needed)

# COMMAND ----------
def get_or_promote_champion(model_name: str) -> str:
    """Return the version string for @champion, promoting @dev if no champion exists."""
    try:
        champ = client.get_model_version_by_alias(model_name, "champion")
        print(f"Using existing @champion: version {champ.version}")
        return champ.version
    except Exception:
        dev = client.get_model_version_by_alias(model_name, "dev")
        client.set_registered_model_alias(model_name, "champion", dev.version)
        print(f"No @champion found — promoted @dev version {dev.version} to @champion")
        return dev.version

champion_version = get_or_promote_champion(da.model_name)
model_uri        = f"models:/{da.model_name}@champion"
print(f"Model URI: {model_uri}")

# COMMAND ----------
# MAGIC %md ## 2. Score Batch via Feature Store

# COMMAND ----------
# Build the spine — just the party_id column for lookup
spine_df = spark.table(da.feature_table).select("party_id")
print(f"Scoring {spine_df.count():,} customers")

# Photon is incompatible with the pyfunc UDF wrapper used by score_batch
spark.conf.set("spark.databricks.photon.enabled", "false")

scored_df = fe.score_batch(
    model_uri=model_uri,
    df=spine_df,
)

spark.conf.set("spark.databricks.photon.enabled", "true")

# Rename the prediction column (it may come back as "prediction" or "next_best_product")
pred_col = "prediction" if "prediction" in scored_df.columns else "next_best_product"
scored_df = scored_df.withColumnRenamed(pred_col, "predicted_label")

print(f"Scored {scored_df.count():,} customers")
display(scored_df.limit(5))

# COMMAND ----------
# MAGIC %md ## 3. Add Metadata Columns

# COMMAND ----------
inference_df = (
    scored_df
    .withColumn("model_version", F.lit(str(champion_version)))
    .withColumn("scored_at",     F.current_timestamp())
    .withColumn("batch_id",      F.lit(f"batch_{datetime.date.today().isoformat()}"))
)

# COMMAND ----------
# MAGIC %md ## 4. Write Inference Log Table (CDF enabled)

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {da.inference_log_table} (
        party_id        STRING,
        predicted_label STRING,
        model_version   STRING,
        scored_at       TIMESTAMP,
        batch_id        STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    COMMENT 'Alliance Bank product recommendation batch inference log'
""")

(
    inference_df
    .select("party_id", "predicted_label", "model_version", "scored_at", "batch_id")
    .write.format("delta")
    .mode("append")
    .saveAsTable(da.inference_log_table)
)

row_count = spark.table(da.inference_log_table).count()
print(f"Scored {row_count:,} customers → {da.inference_log_table}")

# COMMAND ----------
# MAGIC %md ## 5. Inject Synthetic Drift Rows
# MAGIC
# MAGIC For the observability demo, we inject 5 customers that look like
# MAGIC high-confidence INVESTMENT prospects but are labelled NO_ACTION.
# MAGIC This simulates model drift and triggers the monitoring alert.

# COMMAND ----------
drift_rows = [
    ("DRIFT_001", "NO_ACTION", str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_002", "NO_ACTION", str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_003", "NO_ACTION", str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_004", "NO_ACTION", str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_005", "INVESTMENT", str(champion_version), datetime.datetime.now(), "drift_injection"),
]

drift_schema = ["party_id", "predicted_label", "model_version", "scored_at", "batch_id"]
drift_df = spark.createDataFrame(drift_rows, schema=drift_schema)

(
    drift_df.write.format("delta")
    .mode("append")
    .saveAsTable(da.inference_log_table)
)
print(f"Injected 5 drift rows into {da.inference_log_table}")

# COMMAND ----------
# MAGIC %md ## 6. Create Lakehouse Monitoring — InferenceLog Monitor

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorInferenceLog,
    MonitorInferenceLogProblemType,
    MonitorTimeSeries,
)

w = WorkspaceClient()

# Drop existing monitor if present
try:
    w.quality_monitors.delete(table_name=da.inference_log_table)
    print("Existing monitor deleted")
except Exception:
    pass

monitor = w.quality_monitors.create(
    table_name=da.inference_log_table,
    inference_log=MonitorInferenceLog(
        problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
        prediction_col="predicted_label",
        model_id_col="model_version",
        timestamp_col="scored_at",
        label_col=None,        # no ground-truth labels in batch log
        granularities=["1 day"],
    ),
    output_schema_name=da.full_schema,
    assets_dir=f"/Users/{da.username}/monitors/product_recommendation",
)
print(f"Monitor created on: {da.inference_log_table}")
print(f"  Status: {monitor.status}")

# Trigger first refresh
try:
    refresh = w.quality_monitors.run_refresh(table_name=da.inference_log_table)
    print(f"Monitor refresh triggered: {refresh.refresh_id}")
except Exception as e:
    print(f"Monitor refresh (will run on schedule): {e}")

# COMMAND ----------
# MAGIC %md ## 7. Prediction Distribution

# COMMAND ----------
display(
    spark.table(da.inference_log_table)
         .groupBy("predicted_label")
         .count()
         .orderBy("count", ascending=False)
)

# COMMAND ----------
print("Batch inference complete.")
print(f"  Scored customers: {row_count:,}")
print(f"  Inference log:    {da.inference_log_table}")
print(f"  Model version:    @champion (v{champion_version})")
print(f"  Next step:        06-Real-Time-Inference.py")
