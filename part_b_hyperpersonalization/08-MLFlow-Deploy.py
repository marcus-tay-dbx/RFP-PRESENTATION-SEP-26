# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # MLflow Deploy — CI/CD Job Task
# MAGIC
# MAGIC This notebook is the **third and final task** in the Continuous Deployment pipeline.
# MAGIC
# MAGIC It:
# MAGIC 1. Reads `model_version` from the upstream `approve` task value
# MAGIC 2. Sets the `@champion` alias on that version in Unity Catalog
# MAGIC 3. Updates the live serving endpoint to serve the new version
# MAGIC 4. Polls until the endpoint is READY
# MAGIC 5. Tags the model version with `deployed_at` timestamp

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ServedEntityInput,
    EndpointStateReady,
)
import time
import datetime

client = MlflowClient()
w      = WorkspaceClient()

# COMMAND ----------
# Read approved model version from upstream task
model_version = dbutils.jobs.taskValues.get(
    taskKey="approve", key="model_version", debugValue="1"
)
print(f"Deploying model version: {model_version}")

# COMMAND ----------
# MAGIC %md ## 1. Set @champion Alias

# COMMAND ----------
# Move @champion to the approved version
client.set_registered_model_alias(da.model_name, "champion", model_version)
print(f"@champion → version {model_version}")

# COMMAND ----------
# MAGIC %md ## 2. Update Live Serving Endpoint

# COMMAND ----------
served_entity = ServedEntityInput(
    entity_name=da.model_name,
    entity_version=str(model_version),
    workload_size="Small",
    scale_to_zero_enabled=True,
)

existing = [e.name for e in w.serving_endpoints.list()]

if da.endpoint_name in existing:
    w.serving_endpoints.update_config(
        name=da.endpoint_name,
        served_entities=[served_entity]
    )
    print(f"Endpoint updated: {da.endpoint_name} → version {model_version}")
else:
    from databricks.sdk.service.serving import EndpointCoreConfigInput
    w.serving_endpoints.create_and_wait(
        name=da.endpoint_name,
        config=EndpointCoreConfigInput(served_entities=[served_entity]),
        timeout=datetime.timedelta(minutes=20),
    )
    print(f"Endpoint created: {da.endpoint_name}")

# COMMAND ----------
# MAGIC %md ## 3. Poll Until READY

# COMMAND ----------
print(f"Polling endpoint until READY...")
deadline = time.time() + 1200

while time.time() < deadline:
    ep    = w.serving_endpoints.get(da.endpoint_name)
    state = ep.state.ready if ep.state else None
    print(f"  [{time.strftime('%H:%M:%S')}] State: {state}")
    if state == EndpointStateReady.READY:
        print("Endpoint is READY")
        break
    time.sleep(30)
else:
    raise TimeoutError(f"Endpoint did not become READY within 20 minutes")

# COMMAND ----------
# MAGIC %md ## 4. Tag Model Version with Deployment Metadata

# COMMAND ----------
deployed_at = datetime.datetime.utcnow().isoformat()
client.set_model_version_tag(da.model_name, model_version, "deployed_at",      deployed_at)
client.set_model_version_tag(da.model_name, model_version, "deployment_status", "deployed")
client.set_model_version_tag(da.model_name, model_version, "serving_endpoint",  da.endpoint_name)

print(f"\nDeployment complete.")
print(f"  Model:     {da.model_name} version {model_version}")
print(f"  Alias:     @champion")
print(f"  Endpoint:  {da.endpoint_name}")
print(f"  Time:      {deployed_at}")
