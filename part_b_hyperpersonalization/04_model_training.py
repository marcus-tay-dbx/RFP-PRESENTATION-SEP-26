# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install xgboost scikit-learn databricks-feature-engineering shap

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Model Training — XGBoost Multi-Class Product Recommendation
# MAGIC **Target:** next_best_product (6 classes)
# MAGIC **Algorithm:** XGBoost with multi:softprob objective
# MAGIC **Metric:** Macro F1-score

# COMMAND ----------
import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, f1_score, confusion_matrix
import pandas as pd
import numpy as np
import shap

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/dbx-product-recommendation-training")

FEATURE_TABLE = f"{FULL_SCHEMA}.customer_features"
MODEL_NAME    = f"{FULL_SCHEMA}.product_recommendation_model"
FEATURES = [
    "tenure_years","total_deposit_balance_myr","num_accounts","annual_income_amount",
    "net_worth_band_encoded","ctos_score","ccris_status_encoded","payment_conduct_score",
    "age","number_of_dependents","employment_status_encoded","monthly_loan_commitment_myr",
    "num_loan_facilities","txn_count_30d","avg_txn_amount_myr","has_credit_card",
    "digital_maturity_score","telco_arpu_myr","is_shariah_preferred_int",
    "last_product_category_viewed_encoded"
]
LABEL_CLASSES = ["CREDIT_CARD","HOME_LOAN","INSURANCE","INVESTMENT","NO_ACTION","PERSONAL_LOAN"]

# COMMAND ----------
# Load features and encode labels
pdf = spark.table(FEATURE_TABLE).toPandas()
X = pdf[FEATURES].values
le = LabelEncoder()
y = le.fit_transform(pdf["next_best_product"])
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"Training: {len(X_train)} | Test: {len(X_test)}")
print(f"Classes: {list(le.classes_)}")

# COMMAND ----------
with mlflow.start_run(run_name="xgboost_recommendation_v1") as run:
    params = {
        "objective":        "multi:softprob",
        "num_class":        len(le.classes_),
        "n_estimators":     300,
        "max_depth":        6,
        "learning_rate":    0.05,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 3,
        "gamma":            0.1,
        "scale_pos_weight": 1,
        "eval_metric":      "mlogloss",
        "use_label_encoder": False,
        "random_state":     42,
        "n_jobs":           -1,
    }
    mlflow.log_params(params)
    mlflow.log_param("features", FEATURES)
    mlflow.log_param("label_classes", list(le.classes_))
    mlflow.log_param("n_train", len(X_train))
    mlflow.log_param("n_test", len(X_test))

    model = xgb.XGBClassifier(**params, early_stopping_rounds=20)
    model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              verbose=50)

    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)

    f1_macro = f1_score(y_test, y_pred, average="macro")
    mlflow.log_metric("test_f1_macro", f1_macro)
    mlflow.log_metric("best_iteration", model.best_iteration)

    report = classification_report(y_test, y_pred, target_names=le.classes_, output_dict=True)
    for cls in le.classes_:
        mlflow.log_metric(f"f1_{cls}",        report[cls]["f1-score"])
        mlflow.log_metric(f"precision_{cls}", report[cls]["precision"])
        mlflow.log_metric(f"recall_{cls}",    report[cls]["recall"])

    # SHAP feature importance (XGBoost 2.x base_score format may not be SHAP-compatible — wrap safely)
    try:
        explainer  = shap.TreeExplainer(model)
        shap_vals  = explainer.shap_values(X_test[:100])
        shap.summary_plot(shap_vals, X_test[:100], feature_names=FEATURES,
                          class_names=list(le.classes_), show=False)
        import matplotlib.pyplot as plt
        plt.savefig("/tmp/shap_summary.png", bbox_inches="tight")
        mlflow.log_artifact("/tmp/shap_summary.png")
        print("✅ SHAP summary plot logged")
    except Exception as shap_err:
        print(f"⚠️  SHAP skipped (XGBoost 2.x compatibility): {shap_err}")
        # Log native XGBoost feature importance as fallback
        import matplotlib.pyplot as plt
        xgb.plot_importance(model, max_num_features=20)
        plt.tight_layout()
        plt.savefig("/tmp/feature_importance.png", bbox_inches="tight")
        mlflow.log_artifact("/tmp/feature_importance.png")

    importance = dict(zip(FEATURES, model.feature_importances_))
    for feat, imp in importance.items():
        mlflow.log_metric(f"importance_{feat}", float(imp))

    training_set = fe.create_training_set(
        df=spark.createDataFrame(pdf[["party_id","next_best_product"]]),
        feature_lookups=[FeatureLookup(table_name=FEATURE_TABLE, lookup_key="party_id",
                                        feature_names=FEATURES)],
        label="next_best_product",
        exclude_columns=["party_id"]
    )

    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.xgboost,
        training_set=training_set,
        registered_model_name=MODEL_NAME,
        input_example=pd.DataFrame(X_test[:3], columns=FEATURES),
        signature=infer_signature(pd.DataFrame(X_train, columns=FEATURES), y_proba),
    )

    # Also log the raw XGBoost model as a native artifact (artifact_path="xgboost_model").
    # fe.log_model registers only the pyfunc flavor; this lets batch_inference load
    # the model with mlflow.xgboost.load_model() to call predict_proba() directly.
    mlflow.xgboost.log_model(
        model,
        artifact_path="xgboost_model",
        input_example=pd.DataFrame(X_test[:3], columns=FEATURES),
        signature=infer_signature(pd.DataFrame(X_train, columns=FEATURES), y_proba),
    )

    print(f"\n✅ Run ID: {run.info.run_id}")
    print(f"✅ Macro F1: {f1_macro:.4f}")
    print(f"✅ Model registered: {MODEL_NAME} (feature-store pyfunc)")
    print(f"✅ Native XGBoost artifact: runs:/{run.info.run_id}/xgboost_model")

# COMMAND ----------
# Tag as @dev alias
from mlflow import MlflowClient
client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
client.set_registered_model_alias(MODEL_NAME, "dev", latest.version)
print(f"✅ Model version {latest.version} tagged as @dev")

# COMMAND ----------
print("\n📊 Classification Report:")
print(classification_report(y_test, y_pred, target_names=le.classes_))
