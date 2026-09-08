# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install mlflow

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import mlflow, requests, json, time
from mlflow import MlflowClient
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = f"{FULL_SCHEMA}.abmb_recommendation_model"

# COMMAND ----------
# MAGIC %md
# MAGIC # Deploy Real-Time Serving Endpoint
# MAGIC Endpoint reads features from Lakebase (synced from customer_features) for low-latency inference.

# COMMAND ----------
# Promote @dev to @champion
client = MlflowClient()
dev_versions = client.get_model_version_by_alias(MODEL_NAME, "dev")
client.set_registered_model_alias(MODEL_NAME, "champion", dev_versions.version)
print(f"✅ Version {dev_versions.version} promoted to @champion")

# COMMAND ----------
ctx      = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_api_url = ctx.apiUrl().get()
host     = _api_url if _api_url.startswith("https://") else f"https://{_api_url}"
token    = ctx.apiToken().get()
username = ctx.userName().get().replace("@", "_").replace(".", "_")
ENDPOINT_NAME = f"abmb-product-recommendation-{username[:20]}"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------
# Create serving endpoint with AI Gateway inference logging
endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_models": [{
            "name": "champion",
            "model_name": MODEL_NAME,
            "model_version": dev_versions.version,
            "workload_size": "Small",
            "scale_to_zero_enabled": True,
            "environment_vars": {}
        }]
    },
    "ai_gateway": {
        "inference_table_config": {
            "catalog_name": "fevm_master_classic_marcus_catalog",
            "schema_name":  "rfp_presentation",
            "table_name_prefix": "endpoint_payload",
            "enabled": True
        },
        "rate_limits": [{"calls": 60, "renewal_period": "minute", "key": "user"}]
    }
}

resp = requests.post(f"{host}/api/2.0/serving-endpoints",
                     headers=headers, json=endpoint_config)
if resp.status_code in [200, 201]:
    print(f"✅ Endpoint created: {ENDPOINT_NAME}")
elif resp.status_code == 400 and "already exists" in resp.text:
    print(f"ℹ️  Endpoint {ENDPOINT_NAME} already exists")
else:
    print(f"⚠️  Response: {resp.status_code} — {resp.text[:300]}")

dbutils.notebook.exit(ENDPOINT_NAME)

# COMMAND ----------
# MAGIC %md ## Wait for endpoint readiness, then test

# COMMAND ----------
for _ in range(20):
    status = requests.get(f"{host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}", headers=headers).json()
    state  = status.get("state", {}).get("ready", "")
    print(f"State: {state}")
    if state == "READY":
        break
    time.sleep(30)

FEATURES = ["tenure_years","total_deposit_balance_myr","num_accounts","annual_income_amount",
            "net_worth_band_encoded","ctos_score","ccris_status_encoded","payment_conduct_score",
            "age","number_of_dependents","employment_status_encoded","monthly_loan_commitment_myr",
            "num_loan_facilities","txn_count_30d","avg_txn_amount_myr","has_credit_card",
            "digital_maturity_score","telco_arpu_myr","is_shariah_preferred_int",
            "last_product_category_viewed_encoded"]

sample_party    = spark.table(f"{FULL_SCHEMA}.customer_features").select("party_id").first()["party_id"]
sample_features = spark.table(f"{FULL_SCHEMA}.customer_features").filter(f"party_id = '{sample_party}'").toPandas()
payload = {"inputs": sample_features[FEATURES].to_dict(orient="list")}
resp = requests.post(f"{host}/serving-endpoints/{ENDPOINT_NAME}/invocations",
                     headers=headers, json=payload)
print(f"✅ Test inference: {resp.status_code}")
print(json.dumps(resp.json(), indent=2))
