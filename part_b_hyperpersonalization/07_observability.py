# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install shap xgboost databricks-sdk

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import mlflow, shap, matplotlib.pyplot as plt
from mlflow import MlflowClient
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = f"{FULL_SCHEMA}.abmb_recommendation_model"

# COMMAND ----------
# MAGIC %md ## Section A: Compare MLflow Runs

# COMMAND ----------
ctx      = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
username = ctx.userName().get()
runs = mlflow.search_runs(
    experiment_names=[f"/Users/{username}/dbx-product-recommendation-training"],
    order_by=["metrics.test_f1_macro DESC"]
)
display(spark.createDataFrame(runs[[
    "run_id","metrics.test_f1_macro",
    "metrics.f1_CREDIT_CARD","metrics.f1_HOME_LOAN",
    "metrics.f1_INVESTMENT","metrics.f1_INSURANCE",
    "metrics.f1_PERSONAL_LOAN","metrics.f1_NO_ACTION"
]].fillna(0)))

# COMMAND ----------
# MAGIC %md ## Section B: SHAP Waterfall for Hero Customer

# COMMAND ----------
FEATURES = ["tenure_years","total_deposit_balance_myr","num_accounts","annual_income_amount",
            "net_worth_band_encoded","ctos_score","ccris_status_encoded","payment_conduct_score",
            "age","number_of_dependents","employment_status_encoded","monthly_loan_commitment_myr",
            "num_loan_facilities","txn_count_30d","avg_txn_amount_myr","has_credit_card",
            "digital_maturity_score","telco_arpu_myr","is_shariah_preferred_int",
            "last_product_category_viewed_encoded"]

loaded_model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@champion")
hero = spark.table(f"{FULL_SCHEMA}.customer_features").limit(1).toPandas()
try:
    explainer   = shap.TreeExplainer(loaded_model)
    shap_values = explainer.shap_values(hero[FEATURES])
    shap.waterfall_plot(shap.Explanation(
        values=shap_values[0][0],
        base_values=explainer.expected_value[0],
        data=hero[FEATURES].iloc[0],
        feature_names=FEATURES
    ))
    display(plt.gcf())
except Exception as e:
    print(f"⚠️  SHAP waterfall unavailable (XGBoost 2.x): {e}")
    # Fallback: native XGBoost importance plot
    import xgboost as xgb
    xgb.plot_importance(loaded_model, max_num_features=20, title="Feature Importance (XGBoost)")
    display(plt.gcf())

# COMMAND ----------
# MAGIC %md ## Section C: Lakehouse Monitor on Inference Payload

# COMMAND ----------
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
PAYLOAD_TABLE = f"{FULL_SCHEMA}.endpoint_payload_inference_table"

try:
    monitor = w.quality_monitors.create(
        table_name=PAYLOAD_TABLE,
        assets_dir=f"/Volumes/{CATALOG}/{SCHEMA}/monitor_assets",
        output_schema_name=FULL_SCHEMA,
        inference_log={
            "model_id_col":    "request_metadata.model_name",
            "prediction_col":  "response",
            "timestamp_col":   "timestamp_ms",
            "problem_type":    "classification"
        }
    )
    print(f"✅ Monitor created for {PAYLOAD_TABLE}")
except Exception as e:
    print(f"ℹ️  Monitor setup: {e}")
