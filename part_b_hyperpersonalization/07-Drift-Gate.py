# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Drift Gate — Job Task Notebook
# MAGIC
# MAGIC This notebook runs as a standalone job task in the Retrain-on-Drift pipeline.
# MAGIC
# MAGIC **Logic:**
# MAGIC - Reads Lakehouse Monitoring drift metrics for `da.inference_log_table`
# MAGIC - Checks if Jensen-Shannon distance exceeds threshold (default 0.15)
# MAGIC - Emits `retrain_needed` task value (`true` / `false`)
# MAGIC
# MAGIC **Widget:** `force_retrain=true` always emits `retrain_needed=true` (demo mode).

# COMMAND ----------
dbutils.widgets.text("force_retrain", "true",  "Force retrain (demo override)")
dbutils.widgets.text("js_threshold",  "0.15",  "Jensen-Shannon distance threshold")

FORCE_RETRAIN = dbutils.widgets.get("force_retrain").strip().lower() == "true"
JS_THRESHOLD  = float(dbutils.widgets.get("js_threshold").strip())

print(f"force_retrain = {FORCE_RETRAIN}")
print(f"js_threshold  = {JS_THRESHOLD}")

# COMMAND ----------
# MAGIC %md ## Check Lakehouse Monitor Drift Metrics

# COMMAND ----------
from pyspark.sql import functions as F

retrain_needed = False
drift_reason   = "no_drift"

if FORCE_RETRAIN:
    retrain_needed = True
    drift_reason   = "force_retrain_override"
    print("DEMO: force_retrain=true — will trigger retraining regardless of metrics")
else:
    # Try to read monitor drift metrics
    monitor_drift_table = f"{da.full_schema}.product_recommendation_inference_log_drift_metrics"
    try:
        drift_df = spark.table(monitor_drift_table)
        # Jensen-Shannon distance on predicted_label distribution
        js_row = (
            drift_df
            .filter(F.col("column_name") == "predicted_label")
            .filter(F.col("metric_name") == "js_distance")
            .orderBy(F.col("window_start_time").desc())
            .limit(1)
            .collect()
        )
        if js_row:
            js_distance = float(js_row[0]["metric_value"])
            print(f"Latest JS distance on predicted_label: {js_distance:.4f} (threshold: {JS_THRESHOLD})")
            if js_distance > JS_THRESHOLD:
                retrain_needed = True
                drift_reason   = f"js_distance={js_distance:.4f}_exceeds_{JS_THRESHOLD}"
                print(f"DRIFT DETECTED: {drift_reason}")
            else:
                print(f"No drift detected (JS={js_distance:.4f} <= threshold {JS_THRESHOLD})")
        else:
            print("No drift metrics available yet — skipping retrain")
            retrain_needed = False
            drift_reason   = "no_metrics_available"

    except Exception as e:
        print(f"Could not read drift metrics table ({monitor_drift_table}): {e}")
        print("Skipping retrain (metrics not available)")
        retrain_needed = False
        drift_reason   = f"metrics_unavailable: {str(e)[:80]}"

# COMMAND ----------
# MAGIC %md ## Emit Task Value

# COMMAND ----------
dbutils.jobs.taskValues.set("retrain_needed", str(retrain_needed).lower())
dbutils.jobs.taskValues.set("drift_reason",   drift_reason)

print(f"\nTask values emitted:")
print(f"  retrain_needed = {str(retrain_needed).lower()}")
print(f"  drift_reason   = {drift_reason}")
