# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering scikit-learn xgboost

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # MLflow Evaluate — CI/CD Job Task
# MAGIC
# MAGIC This notebook is the **first task** in the Continuous Deployment pipeline.
# MAGIC
# MAGIC It:
# MAGIC 1. Loads the latest `@dev` model version from the registry
# MAGIC 2. Scores the feature table to produce predictions
# MAGIC 3. Computes macro F1 on the full labelled set
# MAGIC 4. Tags the model version with `eval_f1_macro`
# MAGIC 5. Emits `model_version` and `eval_f1_macro` as job task values

# COMMAND ----------
import mlflow
from mlflow import MlflowClient
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from sklearn.metrics import f1_score
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np

fe     = FeatureEngineeringClient()
client = MlflowClient()

# COMMAND ----------
# Get @dev model version
dev     = client.get_model_version_by_alias(da.model_name, "dev")
version = dev.version
print(f"Evaluating @dev: version {version}")

# COMMAND ----------
# Load feature table for eval
labels_df = spark.table(da.feature_table).select("party_id", "next_best_product")

training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name=da.feature_table,
            lookup_key="party_id",
            feature_names=DA.FEATURES
        )
    ],
    label="next_best_product",
    exclude_columns=["party_id"]
)

eval_pdf = training_set.load_df().toPandas()
X_eval   = eval_pdf[DA.FEATURES]
y_true   = eval_pdf["next_best_product"].values

print(f"Eval set: {len(eval_pdf):,} rows")

# COMMAND ----------
# Score using @dev model
spark.conf.set("spark.databricks.photon.enabled", "false")
model_uri = f"models:/{da.model_name}/@dev"
loaded    = mlflow.pyfunc.load_model(model_uri)
preds     = loaded.predict(X_eval)
spark.conf.set("spark.databricks.photon.enabled", "true")

# Decode if numeric
le = LabelEncoder()
le.fit(DA.LABEL_CLASSES)

if hasattr(preds, "dtype") and np.issubdtype(preds.dtype, np.integer):
    pred_labels = le.inverse_transform(preds)
elif hasattr(preds, "values"):
    pred_labels = preds.values.astype(str)
else:
    pred_labels = np.array(preds).astype(str)

eval_f1 = f1_score(y_true, pred_labels, average="macro", labels=DA.LABEL_CLASSES, zero_division=0)
print(f"Eval macro F1: {eval_f1:.4f}")

# COMMAND ----------
# Tag model version
client.set_model_version_tag(da.model_name, version, "eval_f1_macro", str(round(eval_f1, 4)))
client.set_model_version_tag(da.model_name, version, "eval_status",   "evaluated")
print(f"Tagged model version {version} with eval_f1_macro={eval_f1:.4f}")

# COMMAND ----------
# Emit task values
dbutils.jobs.taskValues.set("model_version",   version)
dbutils.jobs.taskValues.set("eval_f1_macro",   str(round(eval_f1, 4)))

print(f"\nTask values:")
print(f"  model_version  = {version}")
print(f"  eval_f1_macro  = {eval_f1:.4f}")
