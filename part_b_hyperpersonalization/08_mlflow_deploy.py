# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # MLflow Deployment Pipeline
# MAGIC ## Evaluate → Approve → Deploy
# MAGIC
# MAGIC This notebook implements a 3-stage deployment gate:
# MAGIC 1. **Evaluate:** Compare challenger vs champion on held-out test set
# MAGIC 2. **Approve:** Auto-approve if challenger macro-F1 >= champion macro-F1 - 0.02
# MAGIC 3. **Deploy:** Update endpoint to serve challenger as new champion

# COMMAND ----------
import mlflow, requests
from mlflow import MlflowClient
from sklearn.metrics import f1_score
from sklearn.preprocessing import LabelEncoder

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
MODEL_NAME = f"{FULL_SCHEMA}.product_recommendation_model"

FEATURES = ["tenure_years","total_deposit_balance_myr","num_accounts","annual_income_amount",
            "net_worth_band_encoded","ctos_score","ccris_status_encoded","payment_conduct_score",
            "age","number_of_dependents","employment_status_encoded","monthly_loan_commitment_myr",
            "num_loan_facilities","txn_count_30d","avg_txn_amount_myr","has_credit_card",
            "digital_maturity_score","telco_arpu_myr","is_shariah_preferred_int",
            "last_product_category_viewed_encoded"]

# COMMAND ----------
# Stage 1: Evaluate
champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
dev      = client.get_model_version_by_alias(MODEL_NAME, "dev")

test_df  = spark.table(f"{FULL_SCHEMA}.customer_features").sample(0.2, seed=99).toPandas()
le = LabelEncoder()
y_true = le.fit_transform(test_df["next_best_product"])

champion_model   = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@champion")
challenger_model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@dev")

f1_champion   = f1_score(y_true, champion_model.predict(test_df[FEATURES].values),   average="macro")
f1_challenger = f1_score(y_true, challenger_model.predict(test_df[FEATURES].values), average="macro")

print(f"Champion   (v{champion.version}) Macro-F1:   {f1_champion:.4f}")
print(f"Challenger (v{dev.version})     Macro-F1: {f1_challenger:.4f}")

# COMMAND ----------
# Stage 2: Approve gate
THRESHOLD = -0.02  # challenger must not degrade more than 2%
approved  = (f1_challenger - f1_champion) >= THRESHOLD
print(f"{'✅ APPROVED' if approved else '❌ REJECTED'}: challenger {'meets' if approved else 'fails'} approval threshold")

if not approved:
    dbutils.notebook.exit("REJECTED")

# COMMAND ----------
# Stage 3: Deploy
client.set_registered_model_alias(MODEL_NAME, "champion", dev.version)
print(f"✅ Deployed: version {dev.version} is now @champion")

ctx      = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_api_url = ctx.apiUrl().get()
host     = _api_url if _api_url.startswith("https://") else f"https://{_api_url}"
token    = ctx.apiToken().get()
headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
username = ctx.userName().get().replace("@","_").replace(".","_")
ENDPOINT_NAME = f"dbx-product-recommendation-{username[:20]}"

update_payload = {
    "served_models": [{
        "name": "champion", "model_name": MODEL_NAME,
        "model_version": dev.version,
        "workload_size": "Small", "scale_to_zero_enabled": True
    }]
}
resp = requests.put(f"{host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config",
                    headers=headers, json=update_payload)
print(f"✅ Endpoint updated: {resp.status_code}")
