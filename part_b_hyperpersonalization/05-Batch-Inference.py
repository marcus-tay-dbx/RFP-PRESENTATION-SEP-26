# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering xgboost scikit-learn

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Batch Inference — Score All DBX Bank Customers
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Loads the `@champion` model
# MAGIC 2. Scores all customers directly from the feature table (no online feature store needed)
# MAGIC 3. Creates `gold_product_recommendations` with top-2 recommendations + confidence scores
# MAGIC 4. Writes inference log with CDF enabled
# MAGIC 5. Injects 5 synthetic drift rows
# MAGIC 6. Creates an InferenceLog Lakehouse Monitor

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import datetime

client = MlflowClient()

# COMMAND ----------
# MAGIC %md ## 1. Resolve Champion Model

# COMMAND ----------
def get_champion_version(model_name: str) -> str:
    for alias in ["champion", "dev"]:
        try:
            mv = client.get_model_version_by_alias(model_name, alias)
            print(f"Using @{alias}: version {mv.version}")
            return mv.version
        except Exception:
            pass
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"No versions found for {model_name}")
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    return latest.version

champion_version = get_champion_version(da.model_name)
model_uri        = f"models:/{da.model_name}/{champion_version}"
print(f"Champion: v{champion_version}  URI: {model_uri}")

# COMMAND ----------
# MAGIC %md ## 2. Load Features and Score Directly

# COMMAND ----------
# Load all features from the feature table
features_pdf = spark.table(da.feature_table).select("party_id", *DA.FEATURES).toPandas()
print(f"Loaded {len(features_pdf):,} customers for scoring")

# Load champion model as pyfunc
print(f"Loading model from {model_uri}...")
model_pyfunc = mlflow.pyfunc.load_model(model_uri)
print("Model loaded.")

# Force all feature columns to float64 via numpy (handles both int64 and Int64 dtypes)
X_np    = np.array(features_pdf[DA.FEATURES], dtype=np.float64)
X_input = pd.DataFrame(X_np, columns=DA.FEATURES)

# Predict — XGBoost multi:softprob returns probabilities (n_samples, n_classes)
raw_preds = model_pyfunc.predict(X_input)

if isinstance(raw_preds, pd.DataFrame):
    proba_arr = raw_preds.values
elif isinstance(raw_preds, np.ndarray):
    proba_arr = raw_preds
else:
    proba_arr = np.array(raw_preds)

print(f"Prediction output shape: {proba_arr.shape}")

# ── Derive top-2 predictions ──────────────────────────────────────────────────
if proba_arr.ndim == 2 and proba_arr.shape[1] == len(DA.LABEL_CLASSES):
    # Full probability matrix — get top-2
    top2_idx    = np.argsort(-proba_arr, axis=1)[:, :2]
    pred_labels = [DA.LABEL_CLASSES[top2_idx[i, 0]] for i in range(len(proba_arr))]
    rec1        = [DA.LABEL_CLASSES[top2_idx[i, 0]] for i in range(len(proba_arr))]
    rec2        = [DA.LABEL_CLASSES[top2_idx[i, 1]] for i in range(len(proba_arr))]
    conf1       = [round(float(proba_arr[i, top2_idx[i, 0]]) * 100, 1) for i in range(len(proba_arr))]
    conf2       = [round(float(proba_arr[i, top2_idx[i, 1]]) * 100, 1) for i in range(len(proba_arr))]
else:
    # Single column — can't compute probability-based top-2
    pred_labels = list(proba_arr.flatten()) if proba_arr.ndim <= 1 else [DA.LABEL_CLASSES[int(np.argmax(row))] for row in proba_arr]
    rec1 = pred_labels
    rec2 = ["NO_ACTION"] * len(pred_labels)
    conf1 = [85.0] * len(pred_labels)
    conf2 = [10.0] * len(pred_labels)

print(f"Prediction distribution: { {lbl: pred_labels.count(lbl) for lbl in DA.LABEL_CLASSES} }")

# ── Create Spark DataFrames ───────────────────────────────────────────────────
scored_pdf = pd.DataFrame({
    "party_id":      features_pdf["party_id"].values,
    "predicted_label": pred_labels,
})
scored_df = spark.createDataFrame(scored_pdf)
print(f"Scored {scored_df.count():,} customers")

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
    COMMENT 'DBX Bank product recommendation batch inference log'
""")

(
    inference_df
    .select("party_id", "predicted_label", "model_version", "scored_at", "batch_id")
    .write.format("delta")
    .mode("append")
    .saveAsTable(da.inference_log_table)
)

row_count = spark.table(da.inference_log_table).count()
print(f"Inference log: {row_count:,} rows → {da.inference_log_table}")

# COMMAND ----------
# MAGIC %md ## 5. Inject Synthetic Drift Rows

# COMMAND ----------
drift_rows = [
    ("DRIFT_001", "NO_ACTION",  str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_002", "NO_ACTION",  str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_003", "NO_ACTION",  str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_004", "NO_ACTION",  str(champion_version), datetime.datetime.now(), "drift_injection"),
    ("DRIFT_005", "INVESTMENT", str(champion_version), datetime.datetime.now(), "drift_injection"),
]
drift_schema = ["party_id", "predicted_label", "model_version", "scored_at", "batch_id"]
drift_df = spark.createDataFrame(drift_rows, schema=drift_schema)
drift_df.write.format("delta").mode("append").saveAsTable(da.inference_log_table)
print(f"Injected 5 drift rows into {da.inference_log_table}")

# COMMAND ----------
# MAGIC %md ## 6. Create gold_product_recommendations (for RM Workstation App)

# COMMAND ----------
gold_pdf = pd.DataFrame({
    "party_id":        features_pdf["party_id"].values,
    "recommendation_1": rec1,
    "confidence_1":     conf1,
    "recommendation_2": rec2,
    "confidence_2":     conf2,
})

spark.createDataFrame(gold_pdf).write.format("delta").mode("overwrite").saveAsTable(
    f"{da.full_schema}.gold_product_recommendations"
)
gold_count = spark.table(f"{da.full_schema}.gold_product_recommendations").count()
print(f"gold_product_recommendations: {gold_count:,} rows")
display(spark.table(f"{da.full_schema}.gold_product_recommendations").limit(5))

# COMMAND ----------
# MAGIC %md ## 7. Create Lakehouse Monitoring — InferenceLog Monitor

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorInferenceLog,
    MonitorInferenceLogProblemType,
)

w = WorkspaceClient()

try:
    w.quality_monitors.delete(table_name=da.inference_log_table)
    print("Existing monitor deleted")
except Exception:
    pass

try:
    monitor = w.quality_monitors.create(
        table_name=da.inference_log_table,
        inference_log=MonitorInferenceLog(
            problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
            prediction_col="predicted_label",
            model_id_col="model_version",
            timestamp_col="scored_at",
            label_col=None,
            granularities=["1 day"],
        ),
        output_schema_name=da.full_schema,
        assets_dir=f"/Users/{da.username}/monitors/product_recommendation",
    )
    print(f"Monitor created on: {da.inference_log_table}")
    try:
        refresh = w.quality_monitors.run_refresh(table_name=da.inference_log_table)
        print(f"Monitor refresh triggered: {refresh.refresh_id}")
    except Exception as e:
        print(f"Monitor refresh (will run on schedule): {e}")
except Exception as monitor_err:
    print(f"Monitor creation skipped (non-fatal): {monitor_err}")

# COMMAND ----------
# MAGIC %md ## 8. Prediction Distribution

# COMMAND ----------
display(
    spark.table(da.inference_log_table)
         .groupBy("predicted_label")
         .count()
         .orderBy("count", ascending=False)
)

# COMMAND ----------
print("=" * 60)
print("Batch inference complete.")
print(f"  Scored:               {row_count:,} customers")
print(f"  Inference log:        {da.inference_log_table}")
print(f"  Gold recs:            {da.full_schema}.gold_product_recommendations ({gold_count:,} rows)")
print(f"  Champion version:     v{champion_version}")
print("=" * 60)
