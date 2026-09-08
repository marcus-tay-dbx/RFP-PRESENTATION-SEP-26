# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # MLflow Approve — CI/CD Job Task
# MAGIC
# MAGIC This notebook is the **second task** in the Continuous Deployment pipeline.
# MAGIC
# MAGIC It acts as an **F1 quality gate**:
# MAGIC - Reads `eval_f1_macro` from the upstream `evaluate` task value
# MAGIC - Compares against `DA.F1_THRESHOLD` (currently {DA.F1_THRESHOLD})
# MAGIC - **If F1 >= threshold**: emits `approved=true`, pipeline continues to Deploy
# MAGIC - **If F1 < threshold**: raises an exception → job fails, requires manual repair/override
# MAGIC
# MAGIC This pattern mirrors a human approval gate — the job stays in FAILED state
# MAGIC until an engineer either fixes the model or manually overrides via job repair.

# COMMAND ----------
from mlflow import MlflowClient

client = MlflowClient()

# COMMAND ----------
# Read task values from upstream evaluate task
model_version = dbutils.jobs.taskValues.get(
    taskKey="evaluate", key="model_version", debugValue="1"
)
eval_f1_str = dbutils.jobs.taskValues.get(
    taskKey="evaluate", key="eval_f1_macro", debugValue=str(DA.F1_THRESHOLD + 0.05)
)
eval_f1 = float(eval_f1_str)

print(f"Model version:   {model_version}")
print(f"Eval macro F1:   {eval_f1:.4f}")
print(f"F1 threshold:    {DA.F1_THRESHOLD}")
print(f"Decision:        {'APPROVE' if eval_f1 >= DA.F1_THRESHOLD else 'REJECT'}")

# COMMAND ----------
# Gate check
if eval_f1 < DA.F1_THRESHOLD:
    client.set_model_version_tag(da.model_name, model_version, "approval_status", "rejected")
    client.set_model_version_tag(da.model_name, model_version, "rejection_reason",
                                 f"f1={eval_f1:.4f} < threshold={DA.F1_THRESHOLD}")
    raise ValueError(
        f"Model version {model_version} REJECTED.\n"
        f"  Eval F1: {eval_f1:.4f}\n"
        f"  Required: >= {DA.F1_THRESHOLD}\n"
        f"To override: repair this job task and set approved=true manually."
    )

# COMMAND ----------
# Approved
client.set_model_version_tag(da.model_name, model_version, "approval_status", "approved")
client.set_model_version_tag(da.model_name, model_version, "approval_f1",     str(round(eval_f1, 4)))

dbutils.jobs.taskValues.set("approved",       "true")
dbutils.jobs.taskValues.set("model_version",  model_version)

print(f"\nModel version {model_version} APPROVED for deployment")
print(f"  F1 = {eval_f1:.4f} >= threshold {DA.F1_THRESHOLD}")
