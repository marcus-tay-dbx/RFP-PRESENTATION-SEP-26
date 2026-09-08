# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Continuous Deployment — 3-Task MLflow CI/CD Pipeline
# MAGIC
# MAGIC This notebook wires the full CI/CD pipeline for Alliance Bank's recommendation model:
# MAGIC
# MAGIC ```
# MAGIC  [08-MLFlow-Evaluate]  →  [08-MLFlow-Approve]  →  [08-MLFlow-Deploy]
# MAGIC        (score @dev)          (F1 quality gate)       (set @champion,
# MAGIC        (tag eval_f1)         (raise if below           update endpoint)
# MAGIC                               threshold)
# MAGIC ```
# MAGIC
# MAGIC A **UC model trigger** is attached so the job auto-runs whenever a new model
# MAGIC version is registered to `da.model_name`.

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, TaskDependency,
    JobEmailNotifications,
)
import datetime

client = MlflowClient()
w      = WorkspaceClient()

# COMMAND ----------
# MAGIC %md ## 1. Resolve Repo Root for Notebook Paths

# COMMAND ----------
ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()
repo_root     = "/".join(notebook_path.split("/")[:-1])
cluster_id    = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")

print(f"Repo root:   {repo_root}")
print(f"Cluster ID:  {cluster_id}")

# COMMAND ----------
# MAGIC %md ## 2. Delete Existing CI/CD Job (idempotent)

# COMMAND ----------
CD_JOB_NAME = "DBX Recommendation Continuous Deployment"

for job in w.jobs.list():
    if job.settings and job.settings.name == CD_JOB_NAME:
        w.jobs.delete(job.job_id)
        print(f"Deleted existing job: {job.job_id}")

# COMMAND ----------
# MAGIC %md ## 3. Create 3-Task Deployment Pipeline

# COMMAND ----------
cd_job = w.jobs.create(
    name=CD_JOB_NAME,
    tasks=[
        Task(
            task_key="evaluate",
            notebook_task=NotebookTask(
                notebook_path=f"{repo_root}/08-MLFlow-Evaluate",
                base_parameters={},
            ),
            existing_cluster_id=cluster_id,
            description="Score @dev model on full feature table, tag eval_f1_macro",
        ),
        Task(
            task_key="approve",
            notebook_task=NotebookTask(
                notebook_path=f"{repo_root}/08-MLFlow-Approve",
                base_parameters={},
            ),
            existing_cluster_id=cluster_id,
            depends_on=[TaskDependency(task_key="evaluate")],
            description=f"F1 quality gate: approve if eval_f1 >= {DA.F1_THRESHOLD}, else fail",
        ),
        Task(
            task_key="deploy",
            notebook_task=NotebookTask(
                notebook_path=f"{repo_root}/08-MLFlow-Deploy",
                base_parameters={},
            ),
            existing_cluster_id=cluster_id,
            depends_on=[TaskDependency(task_key="approve")],
            description="Set @champion alias, update serving endpoint, poll until READY",
        ),
    ],
    run_as_user_name=da.username,
    email_notifications=JobEmailNotifications(
        on_failure=[da.username],
    ),
)

print(f"CI/CD pipeline created: {CD_JOB_NAME} (ID: {cd_job.job_id})")
print(f"  Task 1: evaluate  → 08-MLFlow-Evaluate")
print(f"  Task 2: approve   → 08-MLFlow-Approve   (F1 gate: {DA.F1_THRESHOLD})")
print(f"  Task 3: deploy    → 08-MLFlow-Deploy")

# COMMAND ----------
# MAGIC %md ## 4. Attach UC Model Version Trigger
# MAGIC
# MAGIC Whenever a new version of `da.model_name` is registered, the pipeline
# MAGIC automatically runs — closing the loop between training and production.

# COMMAND ----------
import requests

ctx_api  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url  = ctx_api.apiUrl().get()
host     = api_url if api_url.startswith("https://") else f"https://{api_url}"
token    = ctx_api.apiToken().get()
headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

trigger_payload = {
    "new_settings": {
        "trigger": {
            "model_arrival_trigger": {
                "model_full_name":      da.model_name,
                "minimum_time_interval_seconds": 300,   # debounce: max once per 5 min
            }
        }
    }
}

resp = requests.patch(
    f"{host}/api/2.1/jobs/{cd_job.job_id}",
    headers=headers,
    json=trigger_payload
)
if resp.status_code == 200:
    print(f"UC model trigger attached: new versions of '{da.model_name}' → auto-trigger pipeline")
else:
    print(f"Trigger API: {resp.status_code} — {resp.text[:300]}")
    print("You can add the trigger manually in the Jobs UI: Triggers → Unity Catalog Model")

# COMMAND ----------
# MAGIC %md ## 5. Run the Pipeline Now (Manual Trigger for Demo)

# COMMAND ----------
run = w.jobs.run_now(job_id=cd_job.job_id)
print(f"\nPipeline triggered manually for demo.")
print(f"  Job ID:  {cd_job.job_id}")
print(f"  Run ID:  {run.run_id}")
print(f"  Monitor: {host}/#job/{cd_job.job_id}/run/{run.run_id}")
print(f"\nJob will: Evaluate @dev → Approve (F1 >= {DA.F1_THRESHOLD}) → Deploy @champion → Update endpoint")

# COMMAND ----------
print("\nContinuous Deployment setup complete.")
print(f"  CI/CD Job:     {CD_JOB_NAME} (ID: {cd_job.job_id})")
print(f"  Trigger:       UC model '{da.model_name}' new version → auto-run")
print(f"  F1 gate:       {DA.F1_THRESHOLD}")
print(f"  Endpoint:      {da.endpoint_name}")
print(f"  Next step:     09-Unity-AI-Gateway.py")
