# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install mlflow xgboost scikit-learn databricks-sdk

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Real-Time Inference — Deploy Champion to Model Serving
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Gets the @champion model run_id
# MAGIC 2. Registers a raw (no feature-store) version for endpoint serving
# MAGIC 3. Creates / updates the Model Serving endpoint
# MAGIC 4. Enables AI Gateway inference logging
# MAGIC 5. Polls until endpoint is READY (30 min max, non-fatal)
# MAGIC 6. Queries the live endpoint with 3 sample customers

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
import time
import json
import datetime
import requests
import pandas as pd
import urllib.request
import ssl
import urllib.parse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    EndpointStateReady,
)

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
w      = WorkspaceClient()

# Context for REST calls
ctx     = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = ctx.apiUrl().get()
host    = api_url if api_url.startswith("https://") else f"https://{api_url}"
token   = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------
# MAGIC %md ## 1. Resolve @champion and register raw serving model

# COMMAND ----------
def get_champion_run_id(model_name: str):
    """Return (version_str, mlflow_run_id) for @champion, falling back to @dev then latest."""
    for alias in ["champion", "dev"]:
        try:
            mv = client.get_model_version_by_alias(model_name, alias)
            print(f"@{alias}: version={mv.version} run_id={mv.run_id}")
            return mv.version, mv.run_id
        except Exception:
            pass
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"No versions found for {model_name}")
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    return latest.version, latest.run_id

def check_fs_deps_via_rest(model_name: str, version_str: str) -> bool:
    """Return True if the model version has feature-store table dependencies."""
    try:
        encoded = urllib.parse.quote(model_name, safe='')
        ssl_ctx = ssl.create_default_context()
        req = urllib.request.Request(
            f"{host}/api/2.1/unity-catalog/models/{encoded}/versions/{version_str}",
            headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as resp:
            r = json.loads(resp.read())
            deps = r.get("model_version_dependencies", {}).get("dependencies", [])
            return len(deps) > 0
    except Exception as e:
        print(f"  Could not check deps for v{version_str}: {e}")
        return False  # Assume no deps if uncertain

champ_version, champ_run_id = get_champion_run_id(da.model_name)
print(f"Champion: v{champ_version}  run_id={champ_run_id}")

# ── Register raw model (no feature-store wrapper) ──────────────────────────────
# The feature-store model requires an online feature store at serving time.
# We register the raw artifact from the same training run instead.
serving_version = None

for artifact_path in ["model", "xgboost_model", "sklearn_model"]:
    try:
        print(f"Registering runs:/{champ_run_id}/{artifact_path} …")
        new_mv = mlflow.register_model(
            f"runs:/{champ_run_id}/{artifact_path}",
            da.model_name,
        )
        # Wait for READY (up to 90 s)
        for _ in range(30):
            mv_check = client.get_model_version(da.model_name, new_mv.version)
            if str(mv_check.status) in ("READY", "ModelVersionStatus.READY"):
                break
            time.sleep(3)

        # Verify no feature-store deps
        if check_fs_deps_via_rest(da.model_name, new_mv.version):
            print(f"  v{new_mv.version} still has FS deps — skipping")
            continue

        serving_version = new_mv.version
        client.set_registered_model_alias(da.model_name, "serving", serving_version)
        client.set_registered_model_alias(da.model_name, "champion", serving_version)
        print(f"Registered raw model v{serving_version} (no FS deps) ✓")
        break
    except Exception as e:
        print(f"  '{artifact_path}' failed: {e}")

if serving_version is None:
    print(f"Could not register raw model — falling back to champion v{champ_version}")
    serving_version = champ_version

print(f"\nServing version: {serving_version}")

# COMMAND ----------
# MAGIC %md ## 2. Create or Update Serving Endpoint

# COMMAND ----------
served_entity = ServedEntityInput(
    entity_name=da.model_name,
    entity_version=str(serving_version),
    workload_size="Small",
    scale_to_zero_enabled=True,
)
endpoint_config = EndpointCoreConfigInput(served_entities=[served_entity])

def _delete_endpoint():
    print(f"Deleting endpoint {da.endpoint_name}…")
    try:
        w.serving_endpoints.delete(name=da.endpoint_name)
        for _ in range(12):
            try:
                w.serving_endpoints.get(da.endpoint_name)
                time.sleep(10)
            except Exception:
                break
        print("Endpoint deleted.")
    except Exception as e:
        print(f"Delete: {e}")

def _create_endpoint():
    print(f"Creating endpoint {da.endpoint_name} with v{serving_version}…")
    w.serving_endpoints.create(name=da.endpoint_name, config=endpoint_config)

# Decide create vs update
try:
    existing = w.serving_endpoints.get(da.endpoint_name)
    cfg_str  = str(existing.state.config_update) if existing.state else ""
    rdy_str  = str(existing.state.ready) if existing.state else ""
    print(f"Endpoint exists — ready={rdy_str}  config_update={cfg_str}")

    if "FAILED" in cfg_str:
        print("Endpoint in FAILED state → delete + recreate")
        _delete_endpoint()
        _create_endpoint()
    else:
        w.serving_endpoints.update_config(name=da.endpoint_name, served_entities=[served_entity])
        print("Config update triggered.")

except Exception as err:
    if any(x in str(err).lower() for x in ["does not exist", "not found", "404"]):
        _create_endpoint()
    else:
        print(f"Endpoint error ({err}) → delete + recreate")
        _delete_endpoint()
        _create_endpoint()

# COMMAND ----------
# MAGIC %md ## 3. Enable AI Gateway Inference Logging

# COMMAND ----------
payload_table_prefix = da.inference_log_table.split(".")[-1] + "_payload"
gateway_config = {
    "ai_gateway": {
        "inference_table_config": {
            "catalog_name": da.catalog, "schema_name": da.schema,
            "table_name_prefix": payload_table_prefix, "enabled": True
        }
    }
}
resp = requests.patch(
    f"{host}/api/2.0/serving-endpoints/{da.endpoint_name}/ai-gateway",
    headers=headers, json=gateway_config
)
if resp.status_code in [200, 201]:
    print(f"AI Gateway enabled → {da.catalog}.{da.schema}.{payload_table_prefix}_inference_table")
else:
    print(f"AI Gateway (may retry after READY): {resp.status_code} — {resp.text[:200]}")

# COMMAND ----------
# MAGIC %md ## 4. Poll Until Endpoint is READY (30 min, non-fatal, max 3 recreates)

# COMMAND ----------
print(f"Waiting for endpoint '{da.endpoint_name}' to reach READY…")
deadline       = time.time() + 1800  # 30 minutes
endpoint_ready = False
recreate_count = 0
MAX_RECREATES  = 3

while time.time() < deadline:
    try:
        ep      = w.serving_endpoints.get(da.endpoint_name)
        state   = ep.state.ready if ep.state else None
        cfg_str = str(ep.state.config_update) if ep.state else ""
        ep_str  = str(ep)
        print(f"  [{time.strftime('%H:%M:%S')}] ready={state}  config={cfg_str}")

        if state == EndpointStateReady.READY:
            endpoint_ready = True
            print("Endpoint is READY ✓")
            break

        if ("UPDATE_FAILED" in cfg_str or "FAILED" in cfg_str) and "IN_PROGRESS" not in cfg_str:
            if recreate_count < MAX_RECREATES:
                recreate_count += 1
                print(f"Failure detected — recreating (attempt {recreate_count}/{MAX_RECREATES})")
                _delete_endpoint()
                _create_endpoint()
            else:
                print(f"Max recreate attempts ({MAX_RECREATES}) reached — giving up on endpoint")
                break

    except Exception as poll_err:
        print(f"  Poll error (continuing): {poll_err}")
    time.sleep(30)

if not endpoint_ready:
    print(f"WARNING: Endpoint {da.endpoint_name} not READY within 30 min — continuing anyway")
    print("(Batch inference does not require the serving endpoint)")

# COMMAND ----------
# MAGIC %md ## 5. Query Live Endpoint (non-fatal)

# COMMAND ----------
latency_ms = 0
try:
    if endpoint_ready:
        sample = spark.table(da.feature_table).select(*DA.FEATURES).limit(3).toPandas()
        payload = {"dataframe_records": sample.to_dict(orient="records")}
        t0   = time.time()
        resp = requests.post(
            f"{host}/serving-endpoints/{da.endpoint_name}/invocations",
            headers=headers, json=payload, timeout=30
        )
        latency_ms = (time.time() - t0) * 1000
        if resp.status_code == 200:
            print(f"Inference response ({latency_ms:.0f} ms): {json.dumps(resp.json(), indent=2)[:500]}")
        else:
            print(f"Inference HTTP {resp.status_code}: {resp.text[:300]}")
    else:
        print("Skipping live inference test — endpoint not READY")
except Exception as infer_err:
    print(f"Inference test skipped: {infer_err}")

# COMMAND ----------
# MAGIC %md ## 6. Summary

# COMMAND ----------
print("=" * 60)
print("Real-time serving deployment complete.")
print(f"  Endpoint:         {da.endpoint_name}")
print(f"  Serving model:    v{serving_version}")
print(f"  Endpoint ready:   {endpoint_ready}")
print(f"  Round-trip:       {latency_ms:.0f} ms")
print(f"  Next step:        05-Batch-Inference.py")
print("=" * 60)
