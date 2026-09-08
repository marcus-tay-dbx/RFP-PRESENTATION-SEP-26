# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Cleanup — Drop All Part B Assets
# MAGIC Removes the serving endpoint, all registered model versions, the MLflow experiment,
# MAGIC and the feature / inference / eval log tables created by Part B notebooks.
# MAGIC
# MAGIC **Default:** `dry_run = true` — prints what would be deleted without touching anything.
# MAGIC Set `dry_run = false` AND type `YES` in the confirm widget to execute.

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run")
dbutils.widgets.text("confirm", "", "Type YES to confirm (only needed when dry_run=false)")

DRY_RUN = dbutils.widgets.get("dry_run") == "true"
CONFIRM  = dbutils.widgets.get("confirm").strip()

if not DRY_RUN and CONFIRM != "YES":
    raise ValueError("Set confirm widget to YES to execute destructive cleanup.")

def act(label, fn):
    if DRY_RUN:
        print(f"[DRY RUN] Would delete: {label}")
    else:
        try:
            fn()
            print(f"[DELETED] {label}")
        except Exception as e:
            print(f"[SKIP]    {label} — {e}")

print(f"Mode: {'DRY RUN (safe)' if DRY_RUN else 'LIVE DELETE'}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## 1. Delete Serving Endpoint

# COMMAND ----------
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

act(f"Serving endpoint: {da.endpoint_name}", lambda: w.serving_endpoints.delete(da.endpoint_name))

# COMMAND ----------
# MAGIC %md ## 2. Delete AI Gateway / GLM Service

# COMMAND ----------
act(f"AI Gateway service: {da.gateway_service}", lambda: w.serving_endpoints.delete(da.gateway_service))

# COMMAND ----------
# MAGIC %md ## 3. Delete All Registered Model Versions

# COMMAND ----------
from mlflow import MlflowClient
client = MlflowClient()

def delete_model():
    versions = client.search_model_versions(f"name='{da.model_name}'")
    for v in versions:
        client.delete_model_version(da.model_name, v.version)
        print(f"  Deleted version {v.version}")
    client.delete_registered_model(da.model_name)

act(f"Registered model: {da.model_name}", delete_model)

# COMMAND ----------
# MAGIC %md ## 4. Delete MLflow Experiment

# COMMAND ----------
def delete_experiment():
    exp = mlflow.get_experiment_by_name(da.experiment_name)
    if exp:
        mlflow.delete_experiment(exp.experiment_id)

act(f"MLflow experiment: {da.experiment_name}", delete_experiment)

# COMMAND ----------
# MAGIC %md ## 5. Drop Unity Catalog Tables

# COMMAND ----------
tables_to_drop = [
    da.feature_table,
    da.inference_log_table,
    da.eval_log_table,
    f"{da.inference_log_table}_payload",
    f"{da.inference_log_table}_payload_inference_table",
    f"{da.full_schema}.glm_email_payload_inference_table",
]

for tbl in tables_to_drop:
    act(f"Table: {tbl}", lambda t=tbl: spark.sql(f"DROP TABLE IF EXISTS {t}"))

# COMMAND ----------
# MAGIC %md ## 6. Drop Lakehouse Monitoring Monitors

# COMMAND ----------
from databricks.sdk.service.catalog import MonitorRefreshType

def drop_monitor(table_name):
    try:
        w.quality_monitors.delete(table_name=table_name)
    except Exception:
        pass  # monitor may not exist

act(f"Monitor on: {da.inference_log_table}",              lambda: drop_monitor(da.inference_log_table))
act(f"Monitor on: {da.inference_log_table}_payload",      lambda: drop_monitor(f"{da.inference_log_table}_payload"))

# COMMAND ----------
# MAGIC %md ## 7. Delete Retrain & CI/CD Jobs

# COMMAND ----------
def delete_jobs_by_prefix(prefix):
    for job in w.jobs.list():
        if job.settings.name and job.settings.name.startswith(prefix):
            act(f"Job: {job.settings.name} (ID {job.job_id})", lambda jid=job.job_id: w.jobs.delete(jid))

delete_jobs_by_prefix("DBX Recommendation")

# COMMAND ----------
print("\nCleanup complete.")
print("Re-run Part B notebooks from 03-Feature-Engineering onward to rebuild all assets.")
