# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Setup & Configuration
# MAGIC This notebook is run by every other notebook via `%run ./00-Setup`.
# MAGIC It defines the `DA` namespace class with all catalog, schema, model, and feature config.

# COMMAND ----------
import re, os
import mlflow
from databricks.sdk.runtime import dbutils

class DA:
    # ── Workspace identity ─────────────────────────────────────────────────────
    username  = spark.sql("SELECT current_user()").collect()[0][0]
    initials  = "".join(n[0].upper() for n in username.split("@")[0].replace(".", " ").split()[:2])

    # ── Unity Catalog ──────────────────────────────────────────────────────────
    catalog     = "fevm_master_classic_marcus_catalog"
    schema      = "rfp_presentation"
    full_schema = f"{catalog}.{schema}"
    volume_data = f"/Volumes/{catalog}/{schema}/raw_data"
    volume_pdfs = f"/Volumes/{catalog}/{schema}/product_pdfs"

    # ── ML assets ─────────────────────────────────────────────────────────────
    model_name      = f"{full_schema}.product_recommendation_model"
    endpoint_name   = f"dbx-product-rec-{re.sub(r'[^a-z0-9]', '-', username.split('@')[0])[:20]}"
    gateway_service = f"dbx-glm-gateway-{re.sub(r'[^a-z0-9]', '-', username.split('@')[0])[:20]}"

    # ── Tables ─────────────────────────────────────────────────────────────────
    feature_table        = f"{full_schema}.customer_features"
    inference_log_table  = f"{full_schema}.product_recommendation_inference_log"
    eval_log_table       = f"{full_schema}.product_recommendation_eval_log"

    # ── MLflow ─────────────────────────────────────────────────────────────────
    experiment_name = f"/Users/{username}/product_recommendation_experiment"

    # ── Model ──────────────────────────────────────────────────────────────────
    LABEL_CLASSES = ["CREDIT_CARD", "HOME_LOAN", "INSURANCE", "INVESTMENT", "NO_ACTION", "PERSONAL_LOAN"]
    F1_THRESHOLD  = 0.30   # minimum macro F1 required to approve deployment

    FEATURES = [
        "tenure_years", "total_deposit_balance_myr", "num_accounts",
        "annual_income_amount", "net_worth_band_encoded",
        "ctos_score", "ccris_status_encoded", "payment_conduct_score",
        "age", "number_of_dependents", "employment_status_encoded",
        "monthly_loan_commitment_myr", "num_loan_facilities",
        "txn_count_30d", "avg_txn_amount_myr", "has_credit_card",
        "digital_maturity_score", "telco_arpu_myr",
        "is_shariah_preferred_int", "last_product_category_viewed_encoded"
    ]

    @classmethod
    def print_config(cls):
        print(f"Username:      {cls.username}")
        print(f"Catalog:       {cls.catalog}")
        print(f"Schema:        {cls.schema}")
        print(f"Model:         {cls.model_name}")
        print(f"Endpoint:      {cls.endpoint_name}")
        print(f"Feature table: {cls.feature_table}")
        print(f"Experiment:    {cls.experiment_name}")

da = DA()

# ── Configure MLflow ──────────────────────────────────────────────────────────
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(da.experiment_name)

DA.print_config()
