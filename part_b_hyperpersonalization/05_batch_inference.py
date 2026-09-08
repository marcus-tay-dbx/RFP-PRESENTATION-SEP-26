# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install xgboost mlflow scikit-learn databricks-feature-engineering

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import mlflow
import mlflow.xgboost
import pandas as pd
import numpy as np
from pyspark.sql.functions import current_timestamp

mlflow.set_registry_uri("databricks-uc")

# ── Deployed resource names (UC) ──────────────────────────────────────────────
MODEL_NAME    = f"{FULL_SCHEMA}.product_recommendation_model"   # UC-registered model
FEATURE_TABLE = f"{FULL_SCHEMA}.customer_features"

FEATURES = [
    "tenure_years", "total_deposit_balance_myr", "num_accounts", "annual_income_amount",
    "net_worth_band_encoded", "ctos_score", "ccris_status_encoded", "payment_conduct_score",
    "age", "number_of_dependents", "employment_status_encoded", "monthly_loan_commitment_myr",
    "num_loan_facilities", "txn_count_30d", "avg_txn_amount_myr", "has_credit_card",
    "digital_maturity_score", "telco_arpu_myr", "is_shariah_preferred_int",
    "last_product_category_viewed_encoded"
]
CLASSES = ["CREDIT_CARD", "HOME_LOAN", "INSURANCE", "INVESTMENT", "NO_ACTION", "PERSONAL_LOAN"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Load customer features directly from Unity Catalog

# COMMAND ----------
features_df  = spark.table(FEATURE_TABLE)
customers_df = (spark.table(f"{FULL_SCHEMA}.gold_customer_360")
                .select("party_id", "legal_name", "lifestyle_segment"))

data_df = customers_df.join(
    features_df.select(["party_id"] + FEATURES),
    on="party_id",
    how="inner"
)
pdf = data_df.toPandas()
print(f"✅ Loaded {len(pdf)} customers for batch scoring")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Load XGBoost @champion model and score

# COMMAND ----------
# Load the registered XGBoost model directly (avoids Spark UDF / Photon array issues)
model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@champion")
X = pdf[FEATURES].values.astype(float)

# predict_proba returns shape (n_customers, n_classes) — one probability per class per customer
proba = model.predict_proba(X)
print(f"✅ Model scored {len(pdf)} customers  |  output shape: {proba.shape}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Build top-2 product recommendations per customer

# COMMAND ----------
for i, cls in enumerate(CLASSES):
    pdf[f"prob_{cls}"] = proba[:, i]

def top2(row):
    probs = [(cls, row[f"prob_{cls}"]) for cls in CLASSES]
    probs.sort(key=lambda x: x[1], reverse=True)
    return (
        probs[0][0], round(float(probs[0][1]) * 100, 1),
        probs[1][0], round(float(probs[1][1]) * 100, 1),
    )

results = [top2(row) for _, row in pdf.iterrows()]
pdf["recommendation_1"] = [r[0] for r in results]
pdf["confidence_1"]     = [r[1] for r in results]
pdf["recommendation_2"] = [r[2] for r in results]
pdf["confidence_2"]     = [r[3] for r in results]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Write gold_product_recommendations

# COMMAND ----------
out_cols = [
    "party_id", "legal_name", "lifestyle_segment",
    "recommendation_1", "confidence_1",
    "recommendation_2", "confidence_2"
]
result_df = (spark.createDataFrame(pdf[out_cols])
             .withColumn("scored_at", current_timestamp()))

(result_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{FULL_SCHEMA}.gold_product_recommendations"))

print(f"✅ Scored {len(pdf)} customers → gold_product_recommendations")
display(result_df.select(*out_cols).limit(10))
