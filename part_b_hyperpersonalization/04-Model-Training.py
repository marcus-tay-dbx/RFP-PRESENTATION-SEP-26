# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install xgboost scikit-learn databricks-feature-engineering shap

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Training — RF vs XGBoost Competition
# MAGIC
# MAGIC Alliance Bank's recommendation engine competes two algorithms:
# MAGIC - **RandomForestClassifier** — interpretable baseline (200 trees, class_weight="balanced")
# MAGIC - **XGBoostClassifier** — gradient boosting (multi:softprob, 300 trees)
# MAGIC
# MAGIC Both are logged to MLflow with macro F1, per-class F1, confusion matrix,
# MAGIC and SHAP feature importance. The **winner by macro F1** is registered to
# MAGIC Unity Catalog with the `@dev` alias.

# COMMAND ----------
import mlflow
import mlflow.sklearn
import mlflow.xgboost
from mlflow.models.signature import infer_signature
from mlflow import MlflowClient
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, f1_score, confusion_matrix

import xgboost as xgb
import pandas as pd
import numpy as np
import shap
import json

fe     = FeatureEngineeringClient()
client = MlflowClient()

# COMMAND ----------
# MAGIC %md ## 1. Build Training Set via Feature Lookups

# COMMAND ----------
# Load label column only — features come from Feature Store
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

pdf = training_set.load_df().toPandas()
print(f"Training set: {len(pdf):,} rows × {len(DA.FEATURES)} features")

# COMMAND ----------
# Encode labels using the canonical class list so num_class is always 6
le = LabelEncoder()
le.fit(DA.LABEL_CLASSES)

X = pdf[DA.FEATURES].values
y = le.transform(pdf["next_best_product"])

# Guard: ensure all 6 class indices are present (add synthetic rows if any are missing)
present  = set(np.unique(y))
expected = set(range(len(DA.LABEL_CLASSES)))
missing  = expected - present

if missing:
    for idx in sorted(missing):
        cls = DA.LABEL_CLASSES[idx]
        print(f"WARNING: Class '{cls}' (index {idx}) has 0 samples — adding 1 synthetic row")
        X = np.vstack([X, np.median(X, axis=0, keepdims=True)])
        y = np.append(y, idx)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Class distribution: { {DA.LABEL_CLASSES[i]: int((y==i).sum()) for i in range(len(DA.LABEL_CLASSES))} }")

# COMMAND ----------
# MAGIC %md ## 2. Train RandomForest Baseline

# COMMAND ----------
with mlflow.start_run(run_name="rf_baseline") as rf_run:
    rf_params = {
        "n_estimators":  200,
        "max_depth":     None,
        "class_weight":  "balanced",
        "random_state":  42,
        "n_jobs":        -1,
    }
    mlflow.log_params(rf_params)
    mlflow.log_param("algorithm", "RandomForest")
    mlflow.log_param("features",  json.dumps(DA.FEATURES))
    mlflow.log_param("n_train",   len(X_train))
    mlflow.log_param("n_test",    len(X_test))

    rf = RandomForestClassifier(**rf_params)
    rf.fit(X_train, y_train)

    y_pred_rf  = rf.predict(X_test)
    y_proba_rf = rf.predict_proba(X_test)
    rf_f1      = f1_score(y_test, y_pred_rf, average="macro")

    mlflow.log_metric("test_f1_macro", rf_f1)
    report_rf = classification_report(y_test, y_pred_rf, target_names=le.classes_, output_dict=True)
    for cls in le.classes_:
        mlflow.log_metric(f"f1_{cls}",        report_rf[cls]["f1-score"])
        mlflow.log_metric(f"precision_{cls}", report_rf[cls]["precision"])
        mlflow.log_metric(f"recall_{cls}",    report_rf[cls]["recall"])

    # Feature importance
    importance = dict(zip(DA.FEATURES, rf.feature_importances_))
    for feat, imp in importance.items():
        mlflow.log_metric(f"importance_{feat}", float(imp))

    # SHAP beeswarm (RF)
    try:
        explainer_rf = shap.TreeExplainer(rf)
        shap_vals_rf = explainer_rf.shap_values(X_test[:100])
        import matplotlib.pyplot as plt
        shap.summary_plot(shap_vals_rf, X_test[:100], feature_names=DA.FEATURES,
                          class_names=list(le.classes_), show=False)
        plt.savefig("/tmp/shap_rf.png", bbox_inches="tight")
        mlflow.log_artifact("/tmp/shap_rf.png")
        plt.close()
        print("SHAP plot logged for RF")
    except Exception as e:
        print(f"SHAP skipped for RF: {e}")

    # Confusion matrix
    cm_rf = confusion_matrix(y_test, y_pred_rf)
    np.savetxt("/tmp/confusion_matrix_rf.csv", cm_rf, delimiter=",", fmt="%d")
    mlflow.log_artifact("/tmp/confusion_matrix_rf.csv")

    mlflow.sklearn.log_model(
        rf,
        artifact_path="model",
        input_example=pd.DataFrame(X_test[:3], columns=DA.FEATURES),
        signature=infer_signature(pd.DataFrame(X_train, columns=DA.FEATURES), y_proba_rf),
    )

    rf_run_id = rf_run.info.run_id
    print(f"\nRandomForest  |  Macro F1: {rf_f1:.4f}  |  Run: {rf_run_id}")

# COMMAND ----------
# MAGIC %md ## 3. Train XGBoost Challenger

# COMMAND ----------
with mlflow.start_run(run_name="xgboost_challenger") as xgb_run:
    xgb_params = {
        "objective":         "multi:softprob",
        "num_class":         len(DA.LABEL_CLASSES),
        "n_estimators":      300,
        "max_depth":         6,
        "learning_rate":     0.05,
        "subsample":         0.8,
        "colsample_bytree":  0.8,
        "min_child_weight":  3,
        "gamma":             0.1,
        "eval_metric":       "mlogloss",
        "use_label_encoder": False,
        "random_state":      42,
        "n_jobs":            -1,
    }
    mlflow.log_params(xgb_params)
    mlflow.log_param("algorithm", "XGBoost")
    mlflow.log_param("features",  json.dumps(DA.FEATURES))
    mlflow.log_param("n_train",   len(X_train))
    mlflow.log_param("n_test",    len(X_test))

    xgb_model = xgb.XGBClassifier(**xgb_params, early_stopping_rounds=20)
    xgb_model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50
    )

    y_pred_xgb  = xgb_model.predict(X_test)
    y_proba_xgb = xgb_model.predict_proba(X_test)
    xgb_f1      = f1_score(y_test, y_pred_xgb, average="macro")

    mlflow.log_metric("test_f1_macro",    xgb_f1)
    mlflow.log_metric("best_iteration",   xgb_model.best_iteration)
    report_xgb = classification_report(y_test, y_pred_xgb, target_names=le.classes_, output_dict=True)
    for cls in le.classes_:
        mlflow.log_metric(f"f1_{cls}",        report_xgb[cls]["f1-score"])
        mlflow.log_metric(f"precision_{cls}", report_xgb[cls]["precision"])
        mlflow.log_metric(f"recall_{cls}",    report_xgb[cls]["recall"])

    try:
        explainer_xgb = shap.TreeExplainer(xgb_model)
        shap_vals_xgb = explainer_xgb.shap_values(X_test[:100])
        import matplotlib.pyplot as plt
        shap.summary_plot(shap_vals_xgb, X_test[:100], feature_names=DA.FEATURES,
                          class_names=list(le.classes_), show=False)
        plt.savefig("/tmp/shap_xgb.png", bbox_inches="tight")
        mlflow.log_artifact("/tmp/shap_xgb.png")
        plt.close()
    except Exception as e:
        print(f"SHAP skipped for XGBoost: {e}")
        import matplotlib.pyplot as plt
        xgb.plot_importance(xgb_model, max_num_features=20)
        plt.tight_layout()
        plt.savefig("/tmp/feature_importance_xgb.png", bbox_inches="tight")
        mlflow.log_artifact("/tmp/feature_importance_xgb.png")
        plt.close()

    cm_xgb = confusion_matrix(y_test, y_pred_xgb)
    np.savetxt("/tmp/confusion_matrix_xgb.csv", cm_xgb, delimiter=",", fmt="%d")
    mlflow.log_artifact("/tmp/confusion_matrix_xgb.csv")

    mlflow.xgboost.log_model(
        xgb_model,
        artifact_path="model",
        input_example=pd.DataFrame(X_test[:3], columns=DA.FEATURES),
        signature=infer_signature(pd.DataFrame(X_train, columns=DA.FEATURES), y_proba_xgb),
    )

    xgb_run_id = xgb_run.info.run_id
    print(f"\nXGBoost  |  Macro F1: {xgb_f1:.4f}  |  Run: {xgb_run_id}")

# COMMAND ----------
# MAGIC %md ## 4. Pick Winner and Register to Feature Store

# COMMAND ----------
print(f"\nModel competition results:")
print(f"  RandomForest  macro F1 = {rf_f1:.4f}")
print(f"  XGBoost       macro F1 = {xgb_f1:.4f}")

if xgb_f1 >= rf_f1:
    winner_run_id   = xgb_run_id
    winner_model    = xgb_model
    winner_flavor   = mlflow.xgboost
    winner_name     = "XGBoost"
    winner_f1       = xgb_f1
    winner_proba    = y_proba_xgb
else:
    winner_run_id   = rf_run_id
    winner_model    = rf
    winner_flavor   = mlflow.sklearn
    winner_name     = "RandomForest"
    winner_f1       = rf_f1
    winner_proba    = y_proba_rf

print(f"\nWinner: {winner_name} (macro F1 = {winner_f1:.4f})")

# COMMAND ----------
# Register the winner using fe.log_model (preserves feature lineage)
with mlflow.start_run(run_id=winner_run_id):
    fe.log_model(
        model=winner_model,
        artifact_path="feature_store_model",
        flavor=winner_flavor,
        training_set=training_set,
        registered_model_name=da.model_name,
        input_example=pd.DataFrame(X_test[:3], columns=DA.FEATURES),
        signature=infer_signature(pd.DataFrame(X_train, columns=DA.FEATURES), winner_proba),
    )
    print(f"Model registered: {da.model_name}")

# COMMAND ----------
# Tag the latest version as @dev
versions = client.search_model_versions(f"name='{da.model_name}'")
latest   = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
client.set_registered_model_alias(da.model_name, "dev", latest.version)

client.set_model_version_tag(da.model_name, latest.version, "winner_algorithm", winner_name)
client.set_model_version_tag(da.model_name, latest.version, "test_f1_macro", str(round(winner_f1, 4)))
client.set_model_version_tag(da.model_name, latest.version, "rf_f1",         str(round(rf_f1, 4)))
client.set_model_version_tag(da.model_name, latest.version, "xgb_f1",        str(round(xgb_f1, 4)))

print(f"Model version {latest.version} tagged @dev")
print(f"  Algorithm:   {winner_name}")
print(f"  Macro F1:    {winner_f1:.4f}")

# COMMAND ----------
# MAGIC %md ## 5. Classification Report

# COMMAND ----------
if winner_name == "XGBoost":
    y_pred_winner = y_pred_xgb
else:
    y_pred_winner = y_pred_rf

print(f"\nClassification Report — {winner_name} (Test Set):")
print(classification_report(y_test, y_pred_winner, target_names=le.classes_))

# COMMAND ----------
print("\nModel training complete.")
print(f"  Winner:        {winner_name}")
print(f"  Macro F1:      {winner_f1:.4f}")
print(f"  Model alias:   @dev")
print(f"  Registry name: {da.model_name}")
print(f"  Next step:     05-Batch-Inference.py")
