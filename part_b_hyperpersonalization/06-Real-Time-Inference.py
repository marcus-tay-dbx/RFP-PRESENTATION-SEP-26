# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Real-Time Inference — Deploy Champion to Model Serving
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Promotes `@dev` → `@champion` if not already done
# MAGIC 2. Creates / updates the Model Serving endpoint (`da.endpoint_name`)
# MAGIC 3. Enables AI Gateway inference logging → `da.inference_log_table + "_payload"`
# MAGIC 4. Polls until endpoint is READY
# MAGIC 5. Queries the live endpoint with 3 sample customers
# MAGIC 6. Prints round-trip latency

# COMMAND ----------
import mlflow
import mlflow.deployments
from mlflow import MlflowClient
import time
import json
import requests
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    EndpointStateReady,
)

client = MlflowClient()
w      = WorkspaceClient()

# COMMAND ----------
# MAGIC %md ## 1. Promote @dev → @champion (idempotent)

# COMMAND ----------
def ensure_champion(model_name: str) -> str:
    try:
        champ = client.get_model_version_by_alias(model_name, "champion")
        print(f"@champion already set: version {champ.version}")
        return champ.version
    except Exception:
        dev = client.get_model_version_by_alias(model_name, "dev")
        client.set_registered_model_alias(model_name, "champion", dev.version)
        print(f"Promoted @dev version {dev.version} → @champion")
        return dev.version

champion_version = ensure_champion(da.model_name)
model_uri        = f"models:/{da.model_name}@champion"
print(f"Deploying: {model_uri}")

# COMMAND ----------
# MAGIC %md ## 2. Create or Update Serving Endpoint

# COMMAND ----------
served_entity = ServedEntityInput(
    entity_name=da.model_name,
    entity_version=str(champion_version),
    workload_size="Small",
    scale_to_zero_enabled=True,
)

endpoint_config = EndpointCoreConfigInput(
    served_entities=[served_entity]
)

# Check if endpoint already exists
existing_endpoints = [e.name for e in w.serving_endpoints.list()]

if da.endpoint_name in existing_endpoints:
    print(f"Endpoint {da.endpoint_name} exists — updating config")
    w.serving_endpoints.update_config(
        name=da.endpoint_name,
        served_entities=[served_entity]
    )
else:
    print(f"Creating endpoint: {da.endpoint_name}")
    w.serving_endpoints.create_and_wait(
        name=da.endpoint_name,
        config=endpoint_config,
        timeout=datetime.timedelta(minutes=20),
    )

# COMMAND ----------
# MAGIC %md ## 3. Enable AI Gateway Inference Logging

# COMMAND ----------
import datetime

ctx      = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url  = ctx.apiUrl().get()
host     = api_url if api_url.startswith("https://") else f"https://{api_url}"
token    = ctx.apiToken().get()
headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

payload_table_prefix = da.inference_log_table.split(".")[-1] + "_payload"

gateway_config = {
    "ai_gateway": {
        "inference_table_config": {
            "catalog_name":      da.catalog,
            "schema_name":       da.schema,
            "table_name_prefix": payload_table_prefix,
            "enabled":           True
        }
    }
}

resp = requests.patch(
    f"{host}/api/2.0/serving-endpoints/{da.endpoint_name}/ai-gateway",
    headers=headers,
    json=gateway_config
)
if resp.status_code in [200, 201]:
    print(f"AI Gateway inference logging enabled → {da.catalog}.{da.schema}.{payload_table_prefix}_inference_table")
else:
    print(f"AI Gateway config (may retry after READY): {resp.status_code} — {resp.text[:200]}")

# COMMAND ----------
# MAGIC %md ## 4. Poll Until Endpoint is READY

# COMMAND ----------
print(f"Waiting for endpoint '{da.endpoint_name}' to reach READY state...")
deadline = time.time() + 1200   # 20 minutes max

while time.time() < deadline:
    ep = w.serving_endpoints.get(da.endpoint_name)
    state = ep.state.ready if ep.state else None
    print(f"  [{time.strftime('%H:%M:%S')}] State: {state}")
    if state == EndpointStateReady.READY:
        print("Endpoint is READY")
        break
    time.sleep(30)
else:
    raise TimeoutError(f"Endpoint {da.endpoint_name} did not reach READY within 20 minutes")

# COMMAND ----------
# MAGIC %md ## 5. Query Live Endpoint — 3 Sample Customers

# COMMAND ----------
# Load 3 sample customers from the feature table
sample = (
    spark.table(da.feature_table)
         .select(DA.FEATURES)
         .limit(3)
         .toPandas()
)

# Build the request payload
payload = {"dataframe_records": sample.to_dict(orient="records")}

print("Sending inference request to live endpoint...")
t0   = time.time()
resp = requests.post(
    f"{host}/serving-endpoints/{da.endpoint_name}/invocations",
    headers=headers,
    json=payload,
    timeout=30
)
latency_ms = (time.time() - t0) * 1000

if resp.status_code == 200:
    result = resp.json()
    print(f"Response ({latency_ms:.0f} ms):")
    print(json.dumps(result, indent=2))
else:
    print(f"Error {resp.status_code}: {resp.text[:500]}")

# COMMAND ----------
# MAGIC %md ## 6. Summary

# COMMAND ----------
print("Real-time serving deployment complete.")
print(f"  Endpoint:         {da.endpoint_name}")
print(f"  Model version:    @champion (v{champion_version})")
print(f"  Inference log:    {da.catalog}.{da.schema}.{payload_table_prefix}_inference_table")
print(f"  Round-trip:       {latency_ms:.0f} ms")
print(f"  Next step:        07-Observability.py")
