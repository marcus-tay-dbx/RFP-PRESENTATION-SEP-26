# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install shap databricks-feature-engineering

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Observability — Model Monitoring, SHAP, and Drift Detection
# MAGIC
# MAGIC This notebook covers the full observability stack for DBX Bank's
# MAGIC recommendation model:
# MAGIC
# MAGIC | Section | Topic |
# MAGIC |---|---|
# MAGIC | A | MLflow run comparison (last 5 experiments) |
# MAGIC | B | Feature importance (RF built-in) + SHAP beeswarm |
# MAGIC | C | Prediction distribution & classification report |
# MAGIC | D | Unpack payload inference log → eval table, trigger monitor refresh |
# MAGIC | E | Create Retrain-on-Drift job (07-Drift-Gate → condition → 04-Model-Training) |

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, JobCluster, ClusterSpec,
    TaskDependency, ConditionTask, ConditionTaskOp,
    RunIf, Source,
)
from databricks.sdk.service.catalog import MonitorRefreshType
import pandas as pd
import numpy as np
import shap
import matplotlib.pyplot as plt
import json

client = MlflowClient()
fe     = FeatureEngineeringClient()
w      = WorkspaceClient()

# COMMAND ----------
# MAGIC %md ## Section A: MLflow Run Comparison (Last 5 Runs)

# COMMAND ----------
exp = mlflow.get_experiment_by_name(da.experiment_name)
if exp is None:
    print(f"Experiment not found: {da.experiment_name}")
    print("Run 04-Model-Training first.")
else:
    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=["metrics.test_f1_macro DESC"],
        max_results=5,
    )
    display_cols = ["run_id", "tags.mlflow.runName", "metrics.test_f1_macro",
                    "params.algorithm", "params.n_train"]
    available    = [c for c in display_cols if c in runs.columns]
    display(runs[available])

# COMMAND ----------
# MAGIC %md ## Section B: Feature Importance + SHAP Beeswarm

# COMMAND ----------
# Load champion model
champion_uri = f"models:/{da.model_name}@champion"
try:
    loaded_model = mlflow.pyfunc.load_model(champion_uri)
    print(f"Loaded model from: {champion_uri}")
except Exception as e:
    raise RuntimeError(f"Champion model not found. Run 05-Batch-Inference first.\n{e}")

# Load a held-out sample for analysis
feature_pdf = spark.table(da.feature_table).select(DA.FEATURES + ["next_best_product"]).toPandas()
X_sample = feature_pdf[DA.FEATURES].values[:200]
y_sample = feature_pdf["next_best_product"].values[:200]

# Try to extract the underlying sklearn/xgboost model for SHAP
try:
    from sklearn.ensemble import RandomForestClassifier
    import xgboost as xgb
    from sklearn.preprocessing import LabelEncoder

    inner_model = loaded_model._model_impl.python_model.model()

    explainer = shap.TreeExplainer(inner_model)
    shap_vals = explainer.shap_values(X_sample)

    # For multi-output models shap_vals is a list (one per class)
    if isinstance(shap_vals, list):
        # Use average absolute SHAP across classes for feature importance
        mean_abs_shap = np.mean([np.abs(sv) for sv in shap_vals], axis=0)
        importance_order = np.argsort(mean_abs_shap.mean(axis=0))[::-1]
        print("Top 10 features by mean |SHAP|:")
        for i in importance_order[:10]:
            print(f"  {DA.FEATURES[i]:<40} {mean_abs_shap.mean(axis=0)[i]:.4f}")

        # Beeswarm for the most-predicted class
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        le.fit(DA.LABEL_CLASSES)
        most_common_idx = int(np.bincount(le.transform(y_sample)).argmax())
        shap.summary_plot(shap_vals[most_common_idx], X_sample,
                          feature_names=DA.FEATURES, show=False)
        plt.title(f"SHAP Beeswarm — {DA.LABEL_CLASSES[most_common_idx]}")
        plt.tight_layout()
        plt.savefig("/tmp/shap_beeswarm.png", bbox_inches="tight")
        plt.show()
        plt.close()
    else:
        shap.summary_plot(shap_vals, X_sample, feature_names=DA.FEATURES, show=False)
        plt.tight_layout()
        plt.savefig("/tmp/shap_beeswarm.png", bbox_inches="tight")
        plt.show()
        plt.close()

    print("SHAP beeswarm plot generated")

except Exception as shap_err:
    print(f"SHAP (tree explainer) not available: {shap_err}")
    print("Falling back to MLflow logged importance artifact")
    try:
        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["metrics.test_f1_macro DESC"],
            max_results=1,
        )
        if not runs.empty:
            run_id = runs.iloc[0]["run_id"]
            imp_cols = [c for c in runs.columns if c.startswith("metrics.importance_")]
            if imp_cols:
                feat_imp = {c.replace("metrics.importance_", ""): runs.iloc[0][c]
                            for c in imp_cols}
                sorted_imp = sorted(feat_imp.items(), key=lambda x: x[1], reverse=True)
                print("Top 10 features by MLflow importance:")
                for feat, val in sorted_imp[:10]:
                    print(f"  {feat:<40} {val:.4f}")
    except Exception as fallback_err:
        print(f"Fallback importance also failed: {fallback_err}")

# COMMAND ----------
# MAGIC %md ## Section C: Prediction Distribution & Classification Report

# COMMAND ----------
from sklearn.preprocessing import LabelEncoder

le = LabelEncoder()
le.fit(DA.LABEL_CLASSES)

# Score the full sample
preds = loaded_model.predict(pd.DataFrame(X_sample, columns=DA.FEATURES))
pred_labels = preds if preds.dtype == object else le.inverse_transform(preds.astype(int))

import collections
dist = collections.Counter(pred_labels)
print("Prediction distribution on held-out sample:")
for lbl, cnt in sorted(dist.items(), key=lambda x: -x[1]):
    bar = "#" * int(cnt / 2)
    print(f"  {lbl:<20} {cnt:>4}  {bar}")

# If we have ground truth labels, show classification report
y_true_encoded = le.transform(y_sample)
try:
    y_pred_encoded = le.transform(pred_labels)
    from sklearn.metrics import classification_report
    print("\nClassification Report:")
    print(classification_report(y_sample, pred_labels, labels=DA.LABEL_CLASSES))
except Exception as cr_err:
    print(f"Classification report skipped: {cr_err}")

# COMMAND ----------
# MAGIC %md ## Section D: Unpack Payload Inference Log → Eval Table

# COMMAND ----------
payload_table = f"{da.inference_log_table}_payload_inference_table"

try:
    payload_df = spark.table(payload_table)
    print(f"Payload table found: {payload_table} ({payload_df.count():,} rows)")
    display(payload_df.limit(5))
except Exception:
    print(f"Payload table not yet available: {payload_table}")
    print("Run 06-Real-Time-Inference and send some requests first.")
    print("Creating eval_log_table from batch inference log as fallback...")
    payload_df = None

if payload_df is not None:
    from pyspark.sql import functions as F

    eval_df = (
        payload_df
        .withColumn(
            "predicted_label",
            F.get_json_object(F.col("response"), "$.predictions[0]")
        )
        .withColumn("scored_at", F.to_timestamp(F.col("timestamp_ms") / 1000))
        .withColumn("source", F.lit("realtime_endpoint"))
        .select(
            F.col("databricks_request_id").alias("request_id"),
            "predicted_label",
            "scored_at",
            "source"
        )
        .filter(F.col("predicted_label").isNotNull())
    )

    (
        eval_df.write.format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(da.eval_log_table)
    )
    print(f"Eval log table written: {da.eval_log_table} ({eval_df.count():,} rows)")

    # Trigger monitor refresh on the eval log
    try:
        w.quality_monitors.run_refresh(table_name=da.inference_log_table)
        print("Monitor refresh triggered on inference_log_table")
    except Exception as e:
        print(f"Monitor refresh: {e}")
else:
    # Create eval_log from batch inference log
    (
        spark.table(da.inference_log_table)
             .withColumn("request_id", F.monotonically_increasing_id().cast("string"))
             .withColumn("source", F.lit("batch"))
             .select("request_id", "predicted_label", "scored_at", "source")
             .write.format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(da.eval_log_table)
    )
    print(f"Eval log created from batch inference: {da.eval_log_table}")

# COMMAND ----------
# MAGIC %md ## Section E: Create Retrain-on-Drift Job
# MAGIC
# MAGIC Wires 3 tasks:
# MAGIC 1. `drift_gate` — runs `07-Drift-Gate.py`, emits `retrain_needed` task value
# MAGIC 2. `condition_check` — conditional: continues only if `retrain_needed == true`
# MAGIC 3. `retrain` — runs `04-Model-Training.py` if condition is met

# COMMAND ----------
# Get current notebook's path to derive repo root
ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()
repo_root     = "/".join(notebook_path.split("/")[:-1])   # strip notebook filename

print(f"Repo root for job tasks: {repo_root}")

# Get default cluster id (use current cluster's policy as template)
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")

JOB_NAME = "DBX Recommendation Retrain-on-Drift"

# Delete existing job with same name
for job in w.jobs.list():
    if job.settings and job.settings.name == JOB_NAME:
        w.jobs.delete(job.job_id)
        print(f"Deleted existing job: {job.job_id}")

retrain_job = w.jobs.create(
    name=JOB_NAME,
    tasks=[
        Task(
            task_key="drift_gate",
            notebook_task=NotebookTask(
                notebook_path=f"{repo_root}/07-Drift-Gate",
                base_parameters={"force_retrain": "true"},  # demo override
            ),
            existing_cluster_id=cluster_id,
            description="Check drift metrics and emit retrain_needed task value",
        ),
        Task(
            task_key="condition_check",
            condition_task=ConditionTask(
                op=ConditionTaskOp.EQUAL_TO,
                left="{{tasks.drift_gate.values.retrain_needed}}",
                right="true",
            ),
            depends_on=[TaskDependency(task_key="drift_gate")],
            description="Only retrain if drift gate says retrain_needed=true",
        ),
        Task(
            task_key="retrain",
            notebook_task=NotebookTask(
                notebook_path=f"{repo_root}/04-Model-Training",
                base_parameters={},
            ),
            existing_cluster_id=cluster_id,
            depends_on=[TaskDependency(task_key="condition_check", outcome="true")],
            description="Retrain model if drift detected",
        ),
    ],
    run_as_user_name=da.username,
)

print(f"Job created: {JOB_NAME} (ID: {retrain_job.job_id})")
print(f"  drift_gate       → 07-Drift-Gate")
print(f"  condition_check  → if retrain_needed == true")
print(f"  retrain          → 04-Model-Training")

# COMMAND ----------
print("\nObservability setup complete.")
print(f"  Experiment:       {da.experiment_name}")
print(f"  Inference monitor: {da.inference_log_table}")
print(f"  Eval log table:    {da.eval_log_table}")
print(f"  Retrain job:       {JOB_NAME}")
print(f"  Next step:         08-Continuous-Deployment.py")
