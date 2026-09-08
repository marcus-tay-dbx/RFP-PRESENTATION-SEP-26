# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Part B Setup — Hyperpersonalization
# MAGIC **Dependency check:** Validates that Part A's gold_customer_360 table exists and has data.

# COMMAND ----------
# Validate Part A dependency
try:
    cnt = spark.table(f"{FULL_SCHEMA}.gold_customer_360").count()
    assert cnt >= 800, f"Expected >= 800 customers, got {cnt}"  # ~88% lifecycle_status='active'
    print(f"✅ gold_customer_360: {cnt} rows — Part A dependency satisfied")
except Exception as e:
    raise RuntimeError(f"❌ Part A gold_customer_360 not found or insufficient data. Run Part A first.\n{e}")

# COMMAND ----------
# Create MLflow experiment
import mlflow
mlflow.set_registry_uri("databricks-uc")
experiment = mlflow.set_experiment(f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/dbx-product-recommendation-training")
print(f"✅ MLflow experiment: {experiment.experiment_id}")

# COMMAND ----------
# Confirm Lakebase connectivity (project must exist)
import subprocess, json
result = subprocess.run(
    ["databricks", "lakebase", "instances", "list", "--profile", "fevm-master-classic-marcus"],
    capture_output=True, text=True
)
print("✅ Lakebase accessible" if result.returncode == 0 else f"⚠️ Lakebase check: {result.stderr}")
print(f"✅ Setup complete. Catalog: {FULL_SCHEMA}")
