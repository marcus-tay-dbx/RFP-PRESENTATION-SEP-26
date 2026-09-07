# ABMB Part B: Hyperpersonalization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a product recommendation model + Databricks App for Alliance Bank that reads from Part A's gold_customer_360, trains an XGBoost multi-class recommendation model, deploys a real-time serving endpoint, governs it via Unity AI Gateway (GLM 5.2), and surfaces everything in a React app with two Alliance Bank–branded UI templates.

**Architecture:** Feature engineering from gold_customer_360 → Lakebase synced feature table → XGBoost multi-class (6 product classes) trained with MLflow → serving endpoint with AI Gateway governance → FastAPI backend + React frontend (2 templates) deployed as Databricks App. GLM 5.2 drafts personalised emails via the governed endpoint.

**Tech Stack:** Databricks Serverless ML, XGBoost, MLflow, Feature Store (FeatureLookup), Lakebase (Postgres 17), Databricks Model Serving, Unity AI Gateway, FastAPI, React 18, TypeScript, Vite, Tailwind CSS

**Spec:** `/Users/marcus.tay/Documents/GoodVibesOnly/ABMB_RFP/docs/superpowers/specs/2026-09-07-abmb-rfp-demo-design.md`

## Global Constraints
- Depends on Part A: `gold_customer_360` must exist before running any notebook
- All assets in `fevm_master_classic_marcus_catalog.abmb_rfp_presentation`
- Lakebase project: `ABMB-RFP-PRESENTATION` (already created, production branch active)
- Model endpoint name: `abmb-product-recommendation` (suffix with username if needed)
- GLM 5.2 FMAPI: `system.ai.databricks-glm-5-2` (Databricks-hosted, pay-per-token)
- Every notebook starts with `# MAGIC %run ../shared/config` (shared/config.py from Part A)
- All notebooks use Serverless compute (ML Environment)
- %pip installs at top cell, followed by dbutils.library.restartPython()
- MLflow experiment: `abmb-product-recommendation-training`
- Model registry: UC model `fevm_master_classic_marcus_catalog.abmb_rfp_presentation.abmb_recommendation_model`
- Model aliases: `dev` → `champion` after approval
- Target variable: `next_best_product` (6 classes: CREDIT_CARD, PERSONAL_LOAN, HOME_LOAN, INVESTMENT, INSURANCE, NO_ACTION)

---

### Task 1: Part B Setup + Dependency Validation

**Files to create:** `part_b_hyperpersonalization/00_setup.py`

- [ ] Create `part_b_hyperpersonalization/00_setup.py` with the following content:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Part B Setup — Hyperpersonalization
# MAGIC **Dependency check:** Validates that Part A's gold_customer_360 table exists and has data.

# COMMAND ----------
# Validate Part A dependency
try:
    cnt = spark.table(f"{FULL_SCHEMA}.gold_customer_360").count()
    assert cnt >= 900, f"Expected >= 900 customers, got {cnt}"
    print(f"✅ gold_customer_360: {cnt} rows — Part A dependency satisfied")
except Exception as e:
    raise RuntimeError(f"❌ Part A gold_customer_360 not found or insufficient data. Run Part A first.\n{e}")

# COMMAND ----------
# Create MLflow experiment
import mlflow
mlflow.set_registry_uri("databricks-uc")
experiment = mlflow.set_experiment(f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/abmb-product-recommendation-training")
print(f"✅ MLflow experiment: {experiment.experiment_id}")

# COMMAND ----------
# Confirm Lakebase connectivity (project must exist)
import subprocess, json
result = subprocess.run(
    ["databricks", "lakebase", "instances", "list", "--profile", "fevm-master-classic-marcus"],
    capture_output=True, text=True
)
print("✅ Lakebase accessible" if result.returncode == 0 else f"⚠️ Lakebase check: {result.stderr}")
print(f"✅ Setup complete. Catalog: {FULL_SCHEMA}")
```

---

### Task 2: Overview Notebook

**Files to create:** `part_b_hyperpersonalization/01_overview.py`

- [ ] Create `part_b_hyperpersonalization/01_overview.py` with the following content:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Part B — Overview: Hyperpersonalization with Databricks
# MAGIC
# MAGIC ## Business Problem
# MAGIC Alliance Bank Malaysia serves over 1.2 million retail customers across 5 segments (Mass Market,
# MAGIC Affluent, High Net Worth, Private Banking, Premier). Today, product recommendations are rule-based
# MAGIC and segment-level — every affluent customer gets the same pitch.
# MAGIC
# MAGIC With Databricks, we build a **next-best-product model** that scores each customer individually
# MAGIC using 20 features drawn from their 360 profile: creditworthiness, digital behaviour, transaction
# MAGIC patterns, telco signals, and product ownership.
# MAGIC
# MAGIC **6 product classes:** CREDIT_CARD · PERSONAL_LOAN · HOME_LOAN · INVESTMENT · INSURANCE · NO_ACTION
# MAGIC
# MAGIC **Result:** Real-time recommendations served via a governed Model Serving endpoint, surfaced in a
# MAGIC React app where relationship managers can view a customer's full 360, see model recommendations,
# MAGIC and trigger GLM 5.2 to draft a personalised outreach email — all in one click.

# COMMAND ----------
# MAGIC %md ## gold_customer_360 Schema

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED fevm_master_classic_marcus_catalog.abmb_rfp_presentation.gold_customer_360;

# COMMAND ----------
# MAGIC %md ## Segment Distribution

# COMMAND ----------
from pyspark.sql.functions import count, avg

display(spark.table(f"{FULL_SCHEMA}.gold_customer_360")
        .groupBy("lifestyle_segment")
        .agg(count("party_id").alias("customer_count"),
             avg("ctos_score").alias("avg_credit_score"),
             avg("total_deposit_balance_myr").alias("avg_balance"))
        .orderBy("customer_count", ascending=False))

# COMMAND ----------
# MAGIC %md ## Product Ownership by Segment

# COMMAND ----------
display(spark.table(f"{FULL_SCHEMA}.gold_customer_360")
        .groupBy("lifestyle_segment")
        .agg(
            count("party_id").alias("total_customers"),
            __import__('pyspark.sql.functions', fromlist=['sum']).sum("owns_credit_card").alias("credit_card_holders"),
            __import__('pyspark.sql.functions', fromlist=['sum']).sum("owns_home_loan").alias("home_loan_holders"),
            __import__('pyspark.sql.functions', fromlist=['sum']).sum("owns_personal_loan").alias("personal_loan_holders")
        )
        .orderBy("total_customers", ascending=False))

# COMMAND ----------
# MAGIC %md ## Hero Customer Profile — Highest Balance

# COMMAND ----------
from pyspark.sql.functions import col

hero = (spark.table(f"{FULL_SCHEMA}.gold_customer_360")
        .orderBy(col("total_deposit_balance_myr").desc())
        .limit(1))
display(hero)
print(f"Hero customer: {hero.select('legal_name').first()[0]} | "
      f"Balance: RM {hero.select('total_deposit_balance_myr').first()[0]:,.0f} | "
      f"Segment: {hero.select('lifestyle_segment').first()[0]}")
```

---

### Task 3: EDA Notebook

**Files to create:** `part_b_hyperpersonalization/02_eda.py`

- [ ] Create `part_b_hyperpersonalization/02_eda.py` with the following content:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install seaborn matplotlib pandas

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import *

df = spark.table(f"{FULL_SCHEMA}.gold_customer_360").toPandas()

# COMMAND ----------
# MAGIC %md ## Feature Distributions

# COMMAND ----------
fig, axes = plt.subplots(2, 3, figsize=(15, 8))
df['ctos_score'].hist(bins=30, ax=axes[0,0], color='#1B3A6B', edgecolor='white')
axes[0,0].set_title('CTOS Score Distribution')
df['annual_income_amount'].apply(lambda x: x/1000).hist(bins=30, ax=axes[0,1], color='#C8102E', edgecolor='white')
axes[0,1].set_title('Annual Income (RM thousands)')
df['age'].hist(bins=20, ax=axes[0,2], color='#1B3A6B', edgecolor='white')
axes[0,2].set_title('Age Distribution')
df['total_deposit_balance_myr'].apply(lambda x: x/1000).hist(bins=30, ax=axes[1,0], color='#C8102E', edgecolor='white')
axes[1,0].set_title('Total Balance (RM thousands)')
df['digital_activity_score'].hist(bins=10, ax=axes[1,1], color='#1B3A6B', edgecolor='white')
axes[1,1].set_title('Digital Activity Score')
df['txn_count_30d'].hist(bins=20, ax=axes[1,2], color='#C8102E', edgecolor='white')
axes[1,2].set_title('Transactions Last 30 Days')
plt.suptitle('Alliance Bank Customer Feature Distributions', fontsize=14, fontweight='bold')
plt.tight_layout()
display(fig)

# COMMAND ----------
# MAGIC %md ## Correlation Matrix (features for recommendation model)

# COMMAND ----------
feature_cols = ['age','annual_income_amount','total_deposit_balance_myr','ctos_score',
                'txn_count_30d','digital_activity_score','telco_arpu_myr',
                'num_accounts','num_loan_facilities','monthly_loan_commitment_myr']
corr_df = df[feature_cols].corr()
fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr_df, annot=True, fmt='.2f', cmap='Blues', ax=ax,
            linewidths=0.5, square=True)
ax.set_title('Feature Correlation Matrix')
display(fig)

# COMMAND ----------
# MAGIC %md ## Label Distribution (next_best_product — rule-based proxy labels)

# COMMAND ----------
# Quick label preview using same rules as feature engineering
from pyspark.sql.functions import when, col

gold = spark.table(f"{FULL_SCHEMA}.gold_customer_360")
label_preview = (gold
    .withColumn("label",
        when((col("owns_credit_card") == 0) & (col("ctos_score") > 700) & (col("annual_income_amount") > 60000), "CREDIT_CARD")
        .when((col("owns_home_loan") == 0) & (col("age").between(28, 45)) & (col("marital_status") == "married") & (col("annual_income_amount") > 48000), "HOME_LOAN")
        .when((col("owns_personal_loan") == 0) & ((col("monthly_loan_commitment_myr") * 12 / col("annual_income_amount")) > 0.2), "PERSONAL_LOAN")
        .when(col("net_worth_band").isin("500k_to_1m","1m_to_5m","5m_to_10m","over_10m") & (col("has_fixed_deposit") == False), "INVESTMENT")
        .when((col("has_hire_purchase") == True) & (col("owns_credit_card") == 0), "INSURANCE")
        .otherwise("NO_ACTION"))
    .groupBy("label").count().orderBy("count", ascending=False))
display(label_preview)
```

---

### Task 4: Feature Engineering + Lakebase Synced Table

**Files to create:** `part_b_hyperpersonalization/03_feature_engineering.py`

- [ ] Create `part_b_hyperpersonalization/03_feature_engineering.py` with the following content:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Feature Engineering
# MAGIC Creates the feature table and syncs it to Lakebase for real-time lookup.

# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.functions import *
from pyspark.sql.types import *

fe = FeatureEngineeringClient()

# COMMAND ----------
# Build feature DataFrame from gold_customer_360
gold = spark.table(f"{FULL_SCHEMA}.gold_customer_360")

# Label generation (rule-based for simulation)
def assign_label(row):
    if row.owns_credit_card == 0 and row.ctos_score is not None and row.ctos_score > 700 and row.annual_income_amount > 60000:
        return "CREDIT_CARD"
    elif row.owns_home_loan == 0 and row.age is not None and 28 <= row.age <= 45 and row.marital_status == "married" and row.annual_income_amount > 48000:
        return "HOME_LOAN"
    elif row.monthly_loan_commitment_myr is not None and row.annual_income_amount > 0 and (row.monthly_loan_commitment_myr * 12 / row.annual_income_amount) > 0.2 and row.owns_personal_loan == 0:
        return "PERSONAL_LOAN"
    elif row.net_worth_band in ("500k_to_1m", "1m_to_5m", "5m_to_10m", "over_10m") and row.has_fixed_deposit == False:
        return "INVESTMENT"
    elif row.has_hire_purchase == True and row.owns_credit_card == 0:
        return "INSURANCE"
    else:
        return "NO_ACTION"

label_udf = udf(assign_label, StringType())

features_df = (gold
    .withColumn("next_best_product", label_udf(struct([gold[c] for c in gold.columns])))
    .withColumn("tenure_years", col("relationship_tenure_years").cast("double"))
    .withColumn("net_worth_band_encoded",
        when(col("net_worth_band") == "under_100k", 1)
        .when(col("net_worth_band") == "100k_to_500k", 2)
        .when(col("net_worth_band") == "500k_to_1m", 3)
        .when(col("net_worth_band") == "1m_to_5m", 4)
        .when(col("net_worth_band") == "5m_to_10m", 5)
        .when(col("net_worth_band") == "over_10m", 6)
        .otherwise(1).cast("double"))
    .withColumn("employment_status_encoded",
        when(col("employment_status") == "employed_full_time", 3)
        .when(col("employment_status") == "self_employed", 2)
        .when(col("employment_status").isin("employed_part_time","retired"), 1)
        .otherwise(0).cast("double"))
    .withColumn("ccris_status_encoded",
        when(col("ccris_status") == "clear", 3)
        .when(col("ccris_status") == "caution", 2)
        .otherwise(1).cast("double"))
    .withColumn("payment_conduct_score",
        when(col("payment_conduct_12m") == "clean", 4)
        .when(col("payment_conduct_12m") == "1_missed", 3)
        .when(col("payment_conduct_12m") == "2_missed", 2)
        .otherwise(1).cast("double"))
    .withColumn("last_product_category_viewed_encoded",
        when(col("last_product_category_viewed") == "CREDIT_CARD", 1)
        .when(col("last_product_category_viewed") == "PERSONAL_LOAN", 2)
        .when(col("last_product_category_viewed") == "HOME_LOAN", 3)
        .when(col("last_product_category_viewed") == "INVESTMENT", 4)
        .when(col("last_product_category_viewed") == "INSURANCE", 5)
        .otherwise(0).cast("double"))
    .withColumn("is_shariah_preferred_int", col("is_shariah_preferred").cast("int"))
    .select(
        "party_id",  # lookup key
        col("tenure_years"),
        col("total_deposit_balance_myr").cast("double"),
        col("num_accounts").cast("double"),
        col("annual_income_amount").cast("double"),
        col("net_worth_band_encoded"),
        col("ctos_score").cast("double"),
        col("ccris_status_encoded"),
        col("payment_conduct_score"),
        col("age").cast("double"),
        col("number_of_dependents").cast("double"),
        col("employment_status_encoded"),
        col("monthly_loan_commitment_myr").cast("double"),
        col("num_loan_facilities").cast("double"),
        col("txn_count_30d").cast("double"),
        col("avg_txn_amount_myr").cast("double"),
        col("has_credit_card").cast("int").cast("double"),
        col("digital_maturity_score").cast("double"),
        col("telco_arpu_myr").cast("double"),
        col("is_shariah_preferred_int").cast("double"),
        col("last_product_category_viewed_encoded"),
        col("next_best_product"),
        col("legal_name"),
        col("lifestyle_segment"),
        col("preferred_language_code"),
        col("primary_state"),
    )
    .fillna(0.0, subset=["tenure_years","total_deposit_balance_myr","num_accounts",
                          "annual_income_amount","net_worth_band_encoded","ctos_score",
                          "payment_conduct_score","age","number_of_dependents",
                          "monthly_loan_commitment_myr","num_loan_facilities",
                          "txn_count_30d","avg_txn_amount_myr","digital_maturity_score",
                          "telco_arpu_myr"])
)

# COMMAND ----------
# Create/overwrite Feature Store table
FEATURE_TABLE = f"{FULL_SCHEMA}.customer_features"
spark.sql(f"DROP TABLE IF EXISTS {FEATURE_TABLE}")

fe.create_table(
    name=FEATURE_TABLE,
    primary_keys=["party_id"],
    df=features_df,
    description="Customer features for product recommendation model. Key: party_id.",
    tags={"project": "abmb-rfp", "part": "B", "model": "recommendation"}
)
print(f"✅ Feature table created: {FEATURE_TABLE}")
print(f"   Rows: {spark.table(FEATURE_TABLE).count()}")
print(f"   Features: {len(features_df.columns) - 1}")

# COMMAND ----------
# MAGIC %md ## Sync to Lakebase for Real-Time Lookup
# MAGIC Enable Lakebase Synced Tables in the UI:
# MAGIC 1. Go to Catalog → fevm_master_classic_marcus_catalog → abmb_rfp_presentation → customer_features
# MAGIC 2. Click "Enable Lakebase Sync" → select project ABMB-RFP-PRESENTATION
# MAGIC 3. This creates a live Postgres replica for sub-millisecond feature lookup during serving
# MAGIC
# MAGIC Note: Lakebase synced table setup is UI-only (CLI does not support it yet).

# COMMAND ----------
# Validation
cnt = spark.table(FEATURE_TABLE).count()
assert cnt >= 900, f"Expected >= 900 rows, got {cnt}"
label_dist = spark.table(FEATURE_TABLE).groupBy("next_best_product").count().collect()
print("✅ Label distribution:")
for r in sorted(label_dist, key=lambda x: -x['count']):
    print(f"   {r['next_best_product']}: {r['count']} ({r['count']/cnt*100:.1f}%)")
```

---

### Task 5: Model Training

**Files to create:** `part_b_hyperpersonalization/04_model_training.py`

- [ ] Create `part_b_hyperpersonalization/04_model_training.py` with the following content:

```python
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
mlflow.set_experiment(f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/abmb-product-recommendation-training")

FEATURE_TABLE = f"{FULL_SCHEMA}.customer_features"
MODEL_NAME    = f"{FULL_SCHEMA}.abmb_recommendation_model"
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

    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              verbose=50,
              early_stopping_rounds=20)

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

    # SHAP feature importance
    explainer  = shap.TreeExplainer(model)
    shap_vals  = explainer.shap_values(X_test[:100])
    shap.summary_plot(shap_vals, X_test[:100], feature_names=FEATURES,
                      class_names=list(le.classes_), show=False)
    import matplotlib.pyplot as plt
    plt.savefig("/tmp/shap_summary.png", bbox_inches="tight")
    mlflow.log_artifact("/tmp/shap_summary.png")

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

    print(f"\n✅ Run ID: {run.info.run_id}")
    print(f"✅ Macro F1: {f1_macro:.4f}")
    print(f"✅ Model registered: {MODEL_NAME}")

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
```

---

### Task 6: Batch Inference

**Files to create:** `part_b_hyperpersonalization/05_batch_inference.py`

- [ ] Create `part_b_hyperpersonalization/05_batch_inference.py` with the following content:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
import mlflow
from pyspark.sql.functions import *

fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME    = f"{FULL_SCHEMA}.abmb_recommendation_model"
FEATURE_TABLE = f"{FULL_SCHEMA}.customer_features"

# COMMAND ----------
# Score all customers using feature store batch scoring
all_customers = spark.table(f"{FULL_SCHEMA}.gold_customer_360").select("party_id", "legal_name", "lifestyle_segment")

scored_df = fe.score_batch(
    model_uri=f"models:/{MODEL_NAME}@champion",
    df=all_customers,
    result_type="array<double>"
)

# scored_df has a `prediction` column with probabilities for each class
CLASSES = ["CREDIT_CARD","HOME_LOAN","INSURANCE","INVESTMENT","NO_ACTION","PERSONAL_LOAN"]

scored_final = scored_df
for i, cls in enumerate(CLASSES):
    scored_final = scored_final.withColumn(f"prob_{cls}", col("prediction")[i])

prob_cols = [struct(lit(cls).alias("product"), col(f"prob_{cls}").alias("probability"))
             for cls in CLASSES]
scored_final = (scored_final
    .withColumn("all_probs", sort_array(array(*prob_cols), asc=False))
    .withColumn("recommendation_1", col("all_probs")[0]["product"])
    .withColumn("confidence_1",     round(col("all_probs")[0]["probability"] * 100, 1))
    .withColumn("recommendation_2", col("all_probs")[1]["product"])
    .withColumn("confidence_2",     round(col("all_probs")[1]["probability"] * 100, 1))
    .withColumn("scored_at",        current_timestamp())
    .drop("prediction", "all_probs", *[f"prob_{c}" for c in CLASSES])
)

(scored_final.write.format("delta").mode("overwrite")
    .saveAsTable(f"{FULL_SCHEMA}.gold_product_recommendations"))

print(f"✅ Scored {scored_final.count()} customers → gold_product_recommendations")
display(scored_final.select("party_id","legal_name","lifestyle_segment",
                             "recommendation_1","confidence_1",
                             "recommendation_2","confidence_2").limit(10))
```

---

### Task 7: Real-Time Inference Endpoint

**Files to create:** `part_b_hyperpersonalization/06_realtime_inference.py`

- [ ] Create `part_b_hyperpersonalization/06_realtime_inference.py` with the following content:

```python
# Databricks notebook source
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
host     = f"https://{ctx.apiUrl().get()}"
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
            "schema_name":  "abmb_rfp_presentation",
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
```

---

### Task 8: Observability (MLflow, SHAP, Monitoring)

**Files to create:** `part_b_hyperpersonalization/07_observability.py`

- [ ] Create `part_b_hyperpersonalization/07_observability.py` with the following content:

```python
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
    experiment_names=[f"/Users/{username}/abmb-product-recommendation-training"],
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
explainer   = shap.TreeExplainer(loaded_model)
shap_values = explainer.shap_values(hero[FEATURES])

shap.waterfall_plot(shap.Explanation(
    values=shap_values[0][0],
    base_values=explainer.expected_value[0],
    data=hero[FEATURES].iloc[0],
    feature_names=FEATURES
))
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
```

---

### Task 9: MLflow Deploy Pipeline

**Files to create:** `part_b_hyperpersonalization/08_mlflow_deploy.py`

- [ ] Create `part_b_hyperpersonalization/08_mlflow_deploy.py` with the following content:

```python
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
MODEL_NAME = f"{FULL_SCHEMA}.abmb_recommendation_model"

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
host     = f"https://{ctx.apiUrl().get()}"
token    = ctx.apiToken().get()
headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
username = ctx.userName().get().replace("@","_").replace(".","_")
ENDPOINT_NAME = f"abmb-product-recommendation-{username[:20]}"

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
```

---

### Task 10: Unity AI Gateway Demo (GLM 5.2)

**Files to create:** `part_b_hyperpersonalization/09_unity_ai_gateway.py`

- [ ] Create `part_b_hyperpersonalization/09_unity_ai_gateway.py` with the following content:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway — Governing GLM 5.2 for Alliance Bank
# MAGIC
# MAGIC ## Business Story
# MAGIC Alliance Bank doesn't just deploy AI — they govern it. Every AI call that drafts a
# MAGIC customer email flows through Unity AI Gateway: rate-limited, guardrailed, and fully
# MAGIC auditable. The same governance model that controls data access now controls AI access.
# MAGIC
# MAGIC ## What we configure:
# MAGIC | Section | What | Why |
# MAGIC |---|---|---|
# MAGIC | A | GLM 5.2 model service | Foundation for email drafting |
# MAGIC | B | Inference table | Every call logged for compliance |
# MAGIC | C | Rate limits | Control token spend |
# MAGIC | D | Banking guardrails | No PII in responses, no specific financial advice |
# MAGIC | E | Traffic split | GLM 5.2 (quality) vs GLM 5.3 Flash (speed) |
# MAGIC | F | Telemetry query | system.ai_gateway.usage audit trail |

# COMMAND ----------
import requests, json, time
ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = f"https://{ctx.apiUrl().get()}"
TOKEN = ctx.apiToken().get()
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
username = ctx.userName().get().replace("@","_").replace(".","_")
SERVICE_NAME = f"abmb-glm-email-service-{username[:20]}"
CATALOG_NAME = "fevm_master_classic_marcus_catalog"
SCHEMA_NAME  = "abmb_rfp_presentation"

# COMMAND ----------
# MAGIC %md ## Section A: Create GLM 5.2 Model Service

# COMMAND ----------
service_payload = {
    "name": SERVICE_NAME,
    "config": {
        "served_models": [{
            "name": "glm52-primary",
            "model_name": "system.ai.databricks-glm-5-2",
            "scale_to_zero_enabled": True
        }]
    }
}
resp = requests.post(f"{HOST}/api/2.0/serving-endpoints", headers=H, json=service_payload)
print(f"Created model service: {resp.status_code} — {SERVICE_NAME}")
if resp.status_code not in [200, 201]:
    print(resp.text[:400])

# COMMAND ----------
# Quick test
test_prompt = {
    "messages": [{"role": "user", "content":
        "You are an Alliance Bank RM. Draft a 100-word email to Ahmad bin Ibrahim (affluent segment, "
        "5 years tenure) recommending he considers our WealthSmart Income Fund. Professional, Malay preferred."}]
}
resp = requests.post(f"{HOST}/serving-endpoints/{SERVICE_NAME}/invocations", headers=H, json=test_prompt)
print(resp.json()["choices"][0]["message"]["content"] if resp.status_code == 200 else resp.text[:300])

# COMMAND ----------
# MAGIC %md ## Section B: Enable Inference Table (Compliance Audit Log)
# MAGIC
# MAGIC Navigate to: AI Gateway → Models → {SERVICE_NAME} → Overview → Inference Table → Set up
# MAGIC
# MAGIC Configure:
# MAGIC - Catalog: fevm_master_classic_marcus_catalog
# MAGIC - Schema: abmb_rfp_presentation
# MAGIC - Table prefix: glm_email_payload
# MAGIC
# MAGIC This creates `glm_email_payload_inference_table` — every prompt and response logged.

# COMMAND ----------
update = {
    "ai_gateway": {
        "inference_table_config": {
            "catalog_name": CATALOG_NAME, "schema_name": SCHEMA_NAME,
            "table_name_prefix": "glm_email_payload", "enabled": True
        }
    }
}
resp = requests.patch(f"{HOST}/api/2.0/serving-endpoints/{SERVICE_NAME}/ai-gateway", headers=H, json=update)
print(f"Inference table configured: {resp.status_code}")

# COMMAND ----------
# MAGIC %md ## Section C: Rate Limits

# COMMAND ----------
rate_payload = {
    "ai_gateway": {
        "rate_limits": [
            {"calls": 100, "renewal_period": "minute", "key": "user"},
            {"calls": 5000, "renewal_period": "day",    "key": "endpoint"}
        ]
    }
}
resp = requests.patch(f"{HOST}/api/2.0/serving-endpoints/{SERVICE_NAME}/ai-gateway", headers=H, json=rate_payload)
print(f"Rate limits set: {resp.status_code} — 100 req/min per user, 5000 req/day total")

# COMMAND ----------
# MAGIC %md ## Section D: Banking Guardrails
# MAGIC
# MAGIC **Story:** A bank can't have AI giving specific investment advice or leaking PII in responses.
# MAGIC Unity AI Gateway's service policies enforce this automatically on every response.
# MAGIC
# MAGIC Configure in UI: AI Gateway → {SERVICE_NAME} → Overview → Service Policy → Add
# MAGIC - Policy type: Hallucination / Output content
# MAGIC - Block patterns: IC numbers (\\d{6}-\\d{2}-\\d{4}), account numbers (\\d{10,16})
# MAGIC - Block phrases: "you should invest", "guaranteed return", "I recommend you buy"

# COMMAND ----------
# Demonstrate guardrail in action
risky_prompt = {
    "messages": [{"role": "user", "content":
        "Tell customer Ahmad that he should invest RM 50,000 in unit trusts — guaranteed 8% return. "
        "His IC is 801215-14-1234. Sign off with his account number 1234567890."}]
}
resp = requests.post(f"{HOST}/serving-endpoints/{SERVICE_NAME}/invocations", headers=H, json=risky_prompt)
print("Guardrail response:", resp.status_code)
print(resp.json() if resp.status_code == 200 else resp.text[:500])

# COMMAND ----------
# MAGIC %md ## Section E: Traffic Splitting — GLM 5.2 (Quality) vs GLM 5.3 Flash (Speed/Cost)

# COMMAND ----------
split_payload = {
    "config": {
        "served_models": [
            {"name": "glm52-primary",        "model_name": "system.ai.databricks-glm-5-2",
             "scale_to_zero_enabled": True,  "traffic_percentage": 70},
            {"name": "glm53flash-secondary", "model_name": "system.ai.databricks-glm-5-3-flash",
             "scale_to_zero_enabled": True,  "traffic_percentage": 30}
        ]
    }
}
resp = requests.put(f"{HOST}/api/2.0/serving-endpoints/{SERVICE_NAME}/config", headers=H, json=split_payload)
print(f"Traffic split configured: {resp.status_code} — 70% GLM 5.2 / 30% GLM 5.3 Flash")

# COMMAND ----------
# MAGIC %md ## Section F: Query system.ai_gateway — Audit Trail

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Every AI call Alliance Bank makes is logged here — full audit trail
# MAGIC SELECT
# MAGIC   timestamp_ms / 1000 AS request_time,
# MAGIC   request_metadata.model_name,
# MAGIC   usage.prompt_tokens,
# MAGIC   usage.completion_tokens,
# MAGIC   usage.total_tokens,
# MAGIC   response_metadata.finish_reason
# MAGIC FROM system.ai_gateway.usage
# MAGIC WHERE endpoint_name LIKE '%abmb%'
# MAGIC ORDER BY timestamp_ms DESC
# MAGIC LIMIT 20;
# MAGIC -- Compliance story: "every AI call, who called it, what was asked, what was returned — all auditable"
```

---

### Task 11: App Backend (FastAPI)

**Files to create:**
- `part_b_hyperpersonalization/app/app.py`
- `part_b_hyperpersonalization/app/requirements.txt`

- [ ] Create `part_b_hyperpersonalization/app/requirements.txt`:

```
fastapi==0.111.0
uvicorn==0.30.1
databricks-sdk==0.28.0
pydantic==2.7.1
```

- [ ] Create `part_b_hyperpersonalization/app/app.py` with the following content:

```python
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
import os, re
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="ABMB Customer 360 + Recommendation API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

w = WorkspaceClient()

CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "abmb_rfp_presentation"
FULL    = f"{CATALOG}.{SCHEMA}"


def _get_endpoint_name():
    ctx   = os.environ.get("DATABRICKS_USERNAME", "demo")
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ctx)[:20]
    return f"abmb-product-recommendation-{clean}"


def _get_glm_service():
    ctx   = os.environ.get("DATABRICKS_USERNAME", "demo")
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ctx)[:20]
    return f"abmb-glm-email-service-{clean}"


def _get_warehouse():
    warehouses = list(w.warehouses.list())
    running    = [wh for wh in warehouses if wh.state and "RUNNING" in str(wh.state)]
    if running:
        return running[0].id
    return warehouses[0].id if warehouses else None


def _exec(sql: str):
    return w.statement_execution.execute_statement(
        warehouse_id=_get_warehouse(), statement=sql, wait_timeout="30s"
    )


# ── Health ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "ABMB Customer 360 API"}


# ── Customer search ──────────────────────────────────────────────────────────
@app.get("/api/customers/search")
def search_customers(q: str = "", limit: int = 10):
    sql = f"""
        SELECT party_id, cif_number, legal_name, lifestyle_segment,
               primary_state, relationship_tenure_years, ctos_score
        FROM {FULL}.gold_customer_360
        WHERE lifecycle_status = 'active'
          AND (legal_name ILIKE '%{q}%' OR cif_number ILIKE '%{q}%')
        ORDER BY total_deposit_balance_myr DESC
        LIMIT {limit}
    """
    result = _exec(sql)
    cols   = ["party_id","cif_number","legal_name","lifestyle_segment",
              "primary_state","relationship_tenure_years","ctos_score"]
    return [dict(zip(cols, r)) for r in (result.result.data_array or [])]


# ── Customer 360 ──────────────────────────────────────────────────────────────
@app.get("/api/customer/{party_id}")
def get_customer_360(party_id: str):
    result = _exec(f"SELECT * FROM {FULL}.gold_customer_360 WHERE party_id = '{party_id}'")
    if not result.result.data_array:
        raise HTTPException(status_code=404, detail="Customer not found")
    cols = [c.name for c in result.manifest.schema.columns]
    return dict(zip(cols, result.result.data_array[0]))


# ── Product recommendations ──────────────────────────────────────────────────
FEATURE_COLS = [
    "tenure_years","total_deposit_balance_myr","num_accounts","annual_income_amount",
    "net_worth_band_encoded","ctos_score","ccris_status_encoded","payment_conduct_score",
    "age","number_of_dependents","employment_status_encoded","monthly_loan_commitment_myr",
    "num_loan_facilities","txn_count_30d","avg_txn_amount_myr","has_credit_card",
    "digital_maturity_score","telco_arpu_myr","is_shariah_preferred_int",
    "last_product_category_viewed_encoded"
]
CLASSES = ["CREDIT_CARD","HOME_LOAN","INSURANCE","INVESTMENT","NO_ACTION","PERSONAL_LOAN"]


@app.get("/api/recommend/{party_id}")
def get_recommendations(party_id: str, live: bool = False):
    if live:
        result = _exec(f"SELECT * FROM {FULL}.customer_features WHERE party_id = '{party_id}'")
        if not result.result.data_array:
            raise HTTPException(status_code=404, detail="Customer features not found")
        cols     = [c.name for c in result.manifest.schema.columns]
        features = dict(zip(cols, result.result.data_array[0]))
        payload  = {"dataframe_split": {
            "columns": FEATURE_COLS,
            "data":    [[float(features.get(c, 0) or 0) for c in FEATURE_COLS]]
        }}
        resp   = w.serving_endpoints.query(name=_get_endpoint_name(), request=payload)
        probs  = resp.predictions[0] if resp.predictions else []
        ranked = sorted(zip(CLASSES, probs), key=lambda x: x[1], reverse=True)
        return {"party_id": party_id, "live": True,
                "recommendation_1": ranked[0][0], "confidence_1": round(ranked[0][1]*100, 1),
                "recommendation_2": ranked[1][0], "confidence_2": round(ranked[1][1]*100, 1)}
    else:
        sql    = f"""SELECT recommendation_1, confidence_1, recommendation_2, confidence_2
                     FROM {FULL}.gold_product_recommendations WHERE party_id = '{party_id}'"""
        result = _exec(sql)
        if not result.result.data_array:
            raise HTTPException(status_code=404, detail="Recommendations not found")
        cols = ["recommendation_1","confidence_1","recommendation_2","confidence_2"]
        return dict(zip(cols, result.result.data_array[0]))


# ── Products catalog ──────────────────────────────────────────────────────────
@app.get("/api/products")
def get_products():
    sql    = f"""SELECT product_code, product_name, product_category,
                        key_benefit_1, key_benefit_2, key_benefit_3,
                        min_amount_myr, max_amount_myr, shariah_compliant
                 FROM {FULL}.silver_product_catalog"""
    result = _exec(sql)
    cols   = [c.name for c in result.manifest.schema.columns]
    return [dict(zip(cols, r)) for r in (result.result.data_array or [])]


# ── Email drafting (GLM 5.2 via Unity AI Gateway) ────────────────────────────
class EmailRequest(BaseModel):
    party_id:       str
    recommendation: str
    confidence:     float
    language:       Optional[str] = "en"


PRODUCT_MAP = {
    "CREDIT_CARD":   "Alliance Visa Platinum Credit Card",
    "PERSONAL_LOAN": "Alliance CashFirst Personal Financing-i",
    "HOME_LOAN":     "Alliance HomeSmart Financing",
    "INVESTMENT":    "Alliance WealthSmart Income Fund",
    "INSURANCE":     "Alliance CarStar Takaful",
}


@app.post("/api/draft-email")
def draft_email(req: EmailRequest):
    customer     = get_customer_360(req.party_id)
    product_name = PRODUCT_MAP.get(req.recommendation, req.recommendation)
    lang_instr   = "in Bahasa Malaysia" if req.language == "ms" else "in English"

    prompt = f"""You are an Alliance Bank relationship manager writing to a valued customer.

Customer Profile:
- Name: {customer.get('legal_name', 'Valued Customer')}
- Segment: {customer.get('lifestyle_segment', 'retail').replace('_', ' ').title()}
- Tenure: {customer.get('relationship_tenure_years', 0)} years with Alliance Bank
- Recommended product: {product_name} (model confidence: {req.confidence:.0f}%)

Write a short, warm, professional email {lang_instr} (maximum 150 words).
- Address the customer by name
- Reference their loyalty/tenure naturally
- Briefly introduce the recommended product and 1-2 key benefits
- End with a friendly call to action (schedule a meeting or call)
- DO NOT include specific interest rates, specific financial advice, account numbers, or IC numbers
- Sign off as: Warm regards, Your Alliance Bank Relationship Team"""

    payload  = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 300, "temperature": 0.7}
    resp     = w.serving_endpoints.query(name=_get_glm_service(), request=payload)

    email_text = ""
    if hasattr(resp, "choices") and resp.choices:
        email_text = resp.choices[0].message.content
    elif isinstance(resp, dict):
        email_text = resp.get("choices", [{}])[0].get("message", {}).get("content", "")

    return {"party_id": req.party_id, "recommendation": req.recommendation,
            "product_name": product_name, "email_draft": email_text,
            "model": "system.ai.databricks-glm-5-2"}


# ── Serve React frontend ──────────────────────────────────────────────────────
if os.path.exists("frontend/dist"):
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="static")
```

---

### Task 12: App Frontend — Template A (Banker's Workstation)

**Files to create:**
- `part_b_hyperpersonalization/app/frontend/src/theme.ts`
- `part_b_hyperpersonalization/app/frontend/src/components/AllianceBankHeader.tsx`
- `part_b_hyperpersonalization/app/frontend/src/components/RecommendationCard.tsx`
- `part_b_hyperpersonalization/app/frontend/src/pages/Customer360ViewA.tsx`

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/theme.ts`:

```typescript
export const theme = {
  colors: {
    navy:      "#1B3A6B",
    navyDark:  "#122848",
    navyLight: "#2B5BA8",
    red:       "#C8102E",
    redLight:  "#E8394C",
    white:     "#FFFFFF",
    offWhite:  "#F8F9FB",
    grey50:    "#F0F2F5",
    grey200:   "#CBD5E1",
    grey600:   "#475569",
    grey900:   "#0F172A",
  },
  segments: {
    mass_market:     { color: "#64748B", label: "Mass Market" },
    affluent:        { color: "#1B3A6B", label: "Affluent" },
    high_net_worth:  { color: "#7C3AED", label: "High Net Worth" },
    private_banking: { color: "#B45309", label: "Private Banking" },
    premier:         { color: "#C8102E", label: "Premier" },
  },
  products: {
    CREDIT_CARD:   { icon: "💳", color: "#1B3A6B", label: "Credit Card" },
    PERSONAL_LOAN: { icon: "💰", color: "#0369A1", label: "Personal Loan" },
    HOME_LOAN:     { icon: "🏠", color: "#047857", label: "Home Financing" },
    INVESTMENT:    { icon: "📈", color: "#7C3AED", label: "Investment" },
    INSURANCE:     { icon: "🛡️", color: "#B45309", label: "Takaful" },
    NO_ACTION:     { icon: "✅", color: "#64748B", label: "No Action" },
  }
};
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/components/AllianceBankHeader.tsx`:

```tsx
import React from 'react';
import { theme } from '../theme';

export const AllianceBankHeader: React.FC<{ subtitle?: string }> = ({ subtitle }) => (
  <header style={{ background: theme.colors.navy, color: theme.colors.white,
                   padding: '12px 24px', display: 'flex', alignItems: 'center',
                   gap: '16px', boxShadow: '0 2px 8px rgba(0,0,0,0.3)' }}>
    <img src="/alliance_bank_logo.png" alt="Alliance Bank"
         style={{ height: '36px', filter: 'brightness(0) invert(1)' }} />
    <div>
      <div style={{ fontWeight: 700, fontSize: '16px', letterSpacing: '0.5px' }}>
        Customer Intelligence Platform
      </div>
      {subtitle && <div style={{ fontSize: '12px', opacity: 0.8 }}>{subtitle}</div>}
    </div>
    <div style={{ marginLeft: 'auto', background: theme.colors.red, padding: '4px 12px',
                  borderRadius: '4px', fontSize: '11px', fontWeight: 600 }}>
      POWERED BY DATABRICKS
    </div>
  </header>
);
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/components/RecommendationCard.tsx`:

```tsx
import React from 'react';
import { theme } from '../theme';

interface Props {
  rank:          1 | 2;
  product:       string;
  confidence:    number;
  onDraftEmail?: () => void;
}

export const RecommendationCard: React.FC<Props> = ({ rank, product, confidence, onDraftEmail }) => {
  const info = theme.products[product as keyof typeof theme.products] || theme.products.NO_ACTION;
  return (
    <div style={{ border: `2px solid ${rank === 1 ? theme.colors.navy : theme.colors.grey200}`,
                  borderRadius: '8px', padding: '16px',
                  background: rank === 1 ? '#EEF2FF' : theme.colors.white }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: '24px' }}>{info.icon}</span>
        <span style={{ background: rank === 1 ? theme.colors.navy : theme.colors.grey600,
                       color: 'white', borderRadius: '12px', padding: '2px 8px', fontSize: '11px' }}>
          #{rank} Pick
        </span>
      </div>
      <div style={{ fontWeight: 700, fontSize: '15px', marginTop: '8px', color: theme.colors.navy }}>
        {info.label}
      </div>
      <div style={{ margin: '8px 0' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between',
                      fontSize: '12px', marginBottom: '4px' }}>
          <span>Model Confidence</span>
          <span style={{ fontWeight: 700, color: info.color }}>{confidence}%</span>
        </div>
        <div style={{ background: theme.colors.grey200, borderRadius: '4px', height: '6px' }}>
          <div style={{ width: `${confidence}%`, background: info.color, borderRadius: '4px',
                        height: '6px', transition: 'width 0.8s ease' }} />
        </div>
      </div>
      {rank === 1 && onDraftEmail && (
        <button onClick={onDraftEmail}
          style={{ width: '100%', marginTop: '12px', padding: '8px',
                   background: theme.colors.navy, color: 'white', border: 'none',
                   borderRadius: '6px', cursor: 'pointer', fontWeight: 600, fontSize: '13px' }}>
          ✉️ Draft Personalised Email
        </button>
      )}
    </div>
  );
};
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/pages/Customer360ViewA.tsx`:

```tsx
import React, { useState } from 'react';
import { AllianceBankHeader } from '../components/AllianceBankHeader';
import { RecommendationCard }  from '../components/RecommendationCard';
import { theme } from '../theme';

const API  = '';
const TABS = ['Profile', 'Accounts', 'Transactions', 'Credit', 'Digital'];

export default function Customer360ViewA() {
  const [partyId, setPartyId]           = useState('');
  const [customer, setCustomer]         = useState<any>(null);
  const [recs, setRecs]                 = useState<any>(null);
  const [activeTab, setActiveTab]       = useState('Profile');
  const [emailDraft, setEmailDraft]     = useState('');
  const [draftLoading, setDraftLoading] = useState(false);
  const [searchResults, setSearchResults] = useState<any[]>([]);

  const search = async (q: string) => {
    if (!q) return;
    const res = await fetch(`${API}/api/customers/search?q=${q}&limit=8`);
    setSearchResults(await res.json());
  };

  const load = async (pid: string) => {
    setPartyId(pid); setSearchResults([]);
    const [c, r] = await Promise.all([
      fetch(`${API}/api/customer/${pid}`).then(r => r.json()),
      fetch(`${API}/api/recommend/${pid}`).then(r => r.json())
    ]);
    setCustomer(c); setRecs(r);
  };

  const draftEmail = async () => {
    if (!recs || !customer) return;
    setDraftLoading(true);
    const res = await fetch(`${API}/api/draft-email`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        party_id:       partyId,
        recommendation: recs.recommendation_1,
        confidence:     recs.confidence_1,
        language:       customer.preferred_language_code || 'en'
      })
    });
    const data = await res.json();
    setEmailDraft(data.email_draft);
    setDraftLoading(false);
  };

  const segInfo = customer
    ? theme.segments[customer.lifestyle_segment as keyof typeof theme.segments]
    : null;

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', minHeight: '100vh', background: theme.colors.offWhite }}>
      <AllianceBankHeader subtitle="Banker's Workstation" />
      <div style={{ display: 'flex', height: 'calc(100vh - 60px)' }}>

        {/* LEFT SIDEBAR */}
        <div style={{ width: '280px', background: theme.colors.navy, color: 'white',
                      padding: '16px', display: 'flex', flexDirection: 'column',
                      gap: '12px', overflow: 'auto' }}>
          <div style={{ fontWeight: 600, fontSize: '13px', opacity: 0.7, textTransform: 'uppercase' }}>
            Search Customer
          </div>
          <input placeholder="Name or CIF..."
            onChange={e => search(e.target.value)}
            style={{ width: '100%', padding: '8px 12px', borderRadius: '6px', border: 'none',
                     background: 'rgba(255,255,255,0.15)', color: 'white',
                     outline: 'none', fontSize: '14px' }} />
          {searchResults.map(c => (
            <div key={c.party_id} onClick={() => load(c.party_id)}
              style={{ padding: '10px', borderRadius: '6px', cursor: 'pointer',
                       background: partyId === c.party_id
                         ? 'rgba(255,255,255,0.2)' : 'rgba(255,255,255,0.08)',
                       transition: 'background 0.2s' }}>
              <div style={{ fontWeight: 600, fontSize: '13px' }}>{c.legal_name}</div>
              <div style={{ fontSize: '11px', opacity: 0.7 }}>
                {c.cif_number} · {c.lifestyle_segment?.replace('_', ' ')}
              </div>
            </div>
          ))}
        </div>

        {/* MAIN PANEL */}
        <div style={{ flex: 1, overflow: 'auto', padding: '24px' }}>
          {!customer ? (
            <div style={{ textAlign: 'center', marginTop: '80px', color: theme.colors.grey600 }}>
              <div style={{ fontSize: '48px' }}>🔍</div>
              <div style={{ fontSize: '18px', marginTop: '16px' }}>Search for a customer to begin</div>
            </div>
          ) : (
            <>
              {/* Customer header */}
              <div style={{ background: 'white', borderRadius: '12px', padding: '20px',
                            marginBottom: '16px', display: 'flex', gap: '20px', alignItems: 'center',
                            boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }}>
                <div style={{ width: '56px', height: '56px', borderRadius: '50%',
                               background: theme.colors.navy, color: 'white', display: 'flex',
                               alignItems: 'center', justifyContent: 'center',
                               fontSize: '22px', fontWeight: 700 }}>
                  {customer.legal_name?.[0]}
                </div>
                <div>
                  <div style={{ fontWeight: 700, fontSize: '20px' }}>{customer.legal_name}</div>
                  <div style={{ fontSize: '13px', color: theme.colors.grey600 }}>
                    {customer.cif_number} · {customer.primary_state} · {customer.relationship_tenure_years}yr tenure
                  </div>
                  {segInfo && (
                    <span style={{ background: segInfo.color, color: 'white', padding: '2px 8px',
                                   borderRadius: '12px', fontSize: '11px', fontWeight: 600 }}>
                      {segInfo.label}
                    </span>
                  )}
                </div>
                <div style={{ marginLeft: 'auto', display: 'flex', gap: '16px' }}>
                  {[
                    { label: 'Balance', value: `RM ${((customer.total_deposit_balance_myr||0)/1000).toFixed(0)}K` },
                    { label: 'CTOS',    value: customer.ctos_score || 'N/A' },
                    { label: 'NPS',     value: customer.nps_score  || 'N/A' },
                  ].map(kpi => (
                    <div key={kpi.label}
                      style={{ textAlign: 'center', padding: '8px 16px',
                               background: theme.colors.grey50, borderRadius: '8px' }}>
                      <div style={{ fontWeight: 700, fontSize: '18px', color: theme.colors.navy }}>
                        {kpi.value}
                      </div>
                      <div style={{ fontSize: '11px', color: theme.colors.grey600 }}>{kpi.label}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Tabs */}
              <div style={{ display: 'flex', gap: '4px', marginBottom: '16px' }}>
                {TABS.map(t => (
                  <button key={t} onClick={() => setActiveTab(t)}
                    style={{ padding: '8px 16px', borderRadius: '6px', border: 'none',
                             cursor: 'pointer', fontWeight: activeTab === t ? 700 : 400,
                             fontSize: '13px',
                             background: activeTab === t ? theme.colors.navy : theme.colors.grey50,
                             color:      activeTab === t ? 'white' : theme.colors.grey600 }}>
                    {t}
                  </button>
                ))}
              </div>

              {/* Tab content */}
              <div style={{ background: 'white', borderRadius: '12px', padding: '20px',
                            boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
                {activeTab === 'Profile' && (
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                    {[
                      ['Employment',      customer.employment_status?.replace(/_/g,' ')],
                      ['Annual Income',   `RM ${((customer.annual_income_amount||0)/1000).toFixed(0)}K`],
                      ['Marital Status',  customer.marital_status],
                      ['Dependents',      customer.number_of_dependents],
                      ['Net Worth Band',  customer.net_worth_band?.replace(/_/g,' ')],
                      ['Risk Rating',     customer.risk_rating?.toUpperCase()],
                      ['KYC Status',      customer.kyc_status],
                      ['Shariah Preferred', customer.is_shariah_preferred ? 'Yes' : 'No'],
                      ['Digital Enrolled',  customer.digital_banking_enrollment_flag ? 'Yes' : 'No'],
                      ['Mobile App User',   customer.mobile_app_user_flag ? 'Yes' : 'No'],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', background:theme.colors.grey50, borderRadius:'6px' }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v ?? '—'}</span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Accounts' && (
                  <div>
                    {[
                      ['Accounts',             customer.num_accounts],
                      ['Total Balance',        `RM ${((customer.total_deposit_balance_myr||0)/1000).toFixed(1)}K`],
                      ['Has Current Account',  customer.has_current_account ? 'Yes' : 'No'],
                      ['Has Savings Account',  customer.has_savings_account  ? 'Yes' : 'No'],
                      ['Has Fixed Deposit',    customer.has_fixed_deposit    ? 'Yes' : 'No'],
                      ['Loan Facilities',      customer.num_loan_facilities],
                      ['Total Loan Outstanding', `RM ${((customer.total_loan_outstanding_myr||0)/1000).toFixed(1)}K`],
                      ['Monthly Loan Commitment', `RM ${((customer.monthly_loan_commitment_myr||0)).toFixed(0)}`],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', borderBottom:`1px solid ${theme.colors.grey50}` }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v ?? '—'}</span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Credit' && (
                  <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:'12px' }}>
                    {[
                      ['CTOS Score',           customer.ctos_score],
                      ['CCRIS Status',         customer.ccris_status?.toUpperCase()],
                      ['Payment Conduct (12m)', customer.payment_conduct_12m],
                      ['Credit Facilities',    customer.total_credit_facilities],
                      ['Monthly KB Commitment', `RM ${((customer.kb_monthly_commitment_myr||0)).toFixed(0)}`],
                      ['Bureau Inquiries (6m)', customer.inquiry_count_last_6m],
                      ['Legal Cases',          customer.legal_cases_count],
                      ['Bankruptcy',           customer.bankruptcy_status],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', background:theme.colors.grey50, borderRadius:'6px' }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px',
                                       color: k === 'CTOS Score'
                                         ? (Number(v) > 700 ? '#059669' : Number(v) > 600 ? '#D97706' : '#DC2626')
                                         : 'inherit' }}>
                          {v ?? '—'}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Digital' && (
                  <div>
                    {[
                      ['Digital Activity Score', `${customer.digital_activity_score}/10`],
                      ['Digital Maturity',       `${customer.digital_maturity_score}/10`],
                      ['Mobile Sessions (30d)',  customer.mobile_sessions_30d],
                      ['Product Views (30d)',    customer.product_views_last_30d],
                      ['Last Product Viewed',    customer.last_product_category_viewed?.replace(/_/g,' ') || '—'],
                      ['Telco ARPU',             `RM ${customer.telco_arpu_myr || 0}`],
                      ['Data Consumption',       `${customer.telco_data_consumption_gb_monthly || 0} GB/mo`],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', borderBottom:`1px solid ${theme.colors.grey50}` }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v}</span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Transactions' && (
                  <div>
                    {[
                      ['Transactions (30d)',  customer.txn_count_30d],
                      ['Transactions (90d)',  customer.txn_count_90d],
                      ['Transactions (12m)',  customer.txn_count_12m],
                      ['Debit (30d)',         `RM ${((customer.total_debit_30d_myr||0)/1000).toFixed(1)}K`],
                      ['Credit (30d)',        `RM ${((customer.total_credit_30d_myr||0)/1000).toFixed(1)}K`],
                      ['Avg Transaction',    `RM ${((customer.avg_txn_amount_myr||0)).toFixed(0)}`],
                      ['Card Spend (30d)',    `RM ${((customer.card_spend_30d_myr||0)).toFixed(0)}`],
                      ['Overseas Transactions', customer.overseas_txn_flag ? 'Yes' : 'No'],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', borderBottom:`1px solid ${theme.colors.grey50}` }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          )}
        </div>

        {/* RIGHT RAIL — Recommendations */}
        {customer && recs && (
          <div style={{ width: '280px', padding: '24px 16px', display:'flex',
                        flexDirection:'column', gap:'12px',
                        borderLeft:`1px solid ${theme.colors.grey200}`,
                        background:'white', overflowY:'auto' }}>
            <div style={{ fontWeight: 700, fontSize: '14px', color: theme.colors.navy }}>
              🎯 Next Best Product
            </div>
            <RecommendationCard rank={1} product={recs.recommendation_1}
              confidence={recs.confidence_1} onDraftEmail={draftEmail} />
            <RecommendationCard rank={2} product={recs.recommendation_2}
              confidence={recs.confidence_2} />
            {emailDraft && (
              <div style={{ background:'#EEF2FF', borderRadius:'8px', padding:'12px', marginTop:'8px' }}>
                <div style={{ fontWeight:600, fontSize:'12px', color:theme.colors.navy, marginBottom:'8px' }}>
                  ✉️ AI-Drafted Email (GLM 5.2)
                </div>
                <textarea value={emailDraft} onChange={e => setEmailDraft(e.target.value)}
                  style={{ width:'100%', height:'180px', fontSize:'12px',
                           border:`1px solid ${theme.colors.grey200}`, borderRadius:'6px',
                           padding:'8px', fontFamily:'inherit', resize:'vertical' }} />
              </div>
            )}
            {draftLoading && (
              <div style={{ textAlign:'center', color:theme.colors.grey600, fontSize:'13px' }}>
                ✨ Drafting email...
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
```

---

### Task 13: App Frontend — Template B (Intelligence Hub)

**Files to create:**
- `part_b_hyperpersonalization/app/frontend/src/pages/Customer360ViewB.tsx`
- `part_b_hyperpersonalization/app/frontend/src/App.tsx`
- `part_b_hyperpersonalization/app/frontend/src/main.tsx`

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/pages/Customer360ViewB.tsx`:

```tsx
import React, { useState } from 'react';
import { AllianceBankHeader } from '../components/AllianceBankHeader';
import { RecommendationCard }  from '../components/RecommendationCard';
import { theme } from '../theme';

const API = '';

export default function Customer360ViewB() {
  const [query, setQuery]               = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [customer, setCustomer]         = useState<any>(null);
  const [recs, setRecs]                 = useState<any>(null);
  const [emailDraft, setEmailDraft]     = useState('');
  const [showEmailModal, setShowEmailModal] = useState(false);
  const [draftLoading, setDraftLoading] = useState(false);

  const search = async (q: string) => {
    setQuery(q);
    if (q.length < 2) { setSearchResults([]); return; }
    const res = await fetch(`${API}/api/customers/search?q=${encodeURIComponent(q)}&limit=5`);
    setSearchResults(await res.json());
  };

  const load = async (pid: string) => {
    setSearchResults([]); setQuery('');
    const [c, r] = await Promise.all([
      fetch(`${API}/api/customer/${pid}`).then(r => r.json()),
      fetch(`${API}/api/recommend/${pid}`).then(r => r.json())
    ]);
    setCustomer(c); setRecs(r);
  };

  const draftEmail = async () => {
    if (!recs || !customer) return;
    setDraftLoading(true); setShowEmailModal(true);
    const res = await fetch(`${API}/api/draft-email`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        party_id:       customer.party_id,
        recommendation: recs.recommendation_1,
        confidence:     recs.confidence_1,
        language:       customer.preferred_language_code
      })
    });
    const data = await res.json();
    setEmailDraft(data.email_draft); setDraftLoading(false);
  };

  const segInfo = customer
    ? (theme.segments[customer.lifestyle_segment as keyof typeof theme.segments] || theme.segments.mass_market)
    : null;

  const KPITile = ({ label, value, sub }: any) => (
    <div style={{ background:'white', borderRadius:'12px', padding:'16px 20px',
                  boxShadow:'0 2px 8px rgba(27,58,107,0.08)', flex:1, minWidth:'120px' }}>
      <div style={{ fontSize:'22px', fontWeight:800, color:theme.colors.navy }}>{value}</div>
      <div style={{ fontSize:'12px', color:theme.colors.grey600, marginTop:'2px' }}>{label}</div>
      {sub && <div style={{ fontSize:'11px', color:theme.colors.grey600, opacity:0.7 }}>{sub}</div>}
    </div>
  );

  return (
    <div style={{ fontFamily:'system-ui,sans-serif', minHeight:'100vh', background:'#F0F4FA' }}>
      <AllianceBankHeader subtitle="Intelligence Hub" />

      {/* Search bar */}
      <div style={{ background:theme.colors.navy, padding:'16px 32px', position:'relative' }}>
        <input value={query} onChange={e => search(e.target.value)}
          placeholder="Search customer by name or CIF..."
          style={{ width:'100%', maxWidth:'480px', padding:'10px 16px', borderRadius:'24px',
                   border:'none', fontSize:'14px', outline:'none',
                   boxShadow:'0 2px 8px rgba(0,0,0,0.2)' }} />
        {searchResults.length > 0 && (
          <div style={{ position:'absolute', top:'52px', left:'32px', width:'480px',
                        background:'white', borderRadius:'8px',
                        boxShadow:'0 8px 24px rgba(0,0,0,0.15)', zIndex:100, overflow:'hidden' }}>
            {searchResults.map(c => (
              <div key={c.party_id} onClick={() => load(c.party_id)}
                style={{ padding:'12px 16px', cursor:'pointer',
                         borderBottom:`1px solid ${theme.colors.grey50}`,
                         display:'flex', justifyContent:'space-between', alignItems:'center' }}
                onMouseEnter={e => (e.currentTarget.style.background = theme.colors.grey50)}
                onMouseLeave={e => (e.currentTarget.style.background = 'white')}>
                <div>
                  <div style={{ fontWeight:600 }}>{c.legal_name}</div>
                  <div style={{ fontSize:'12px', color:theme.colors.grey600 }}>{c.cif_number}</div>
                </div>
                <span style={{ fontSize:'11px', padding:'2px 8px', borderRadius:'12px',
                               background:theme.colors.grey50, color:theme.colors.grey600 }}>
                  {c.lifestyle_segment?.replace(/_/g,' ')}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {!customer ? (
        <div style={{ textAlign:'center', marginTop:'100px', color:theme.colors.grey600 }}>
          <div style={{ fontSize:'64px' }}>🏦</div>
          <div style={{ fontSize:'20px', fontWeight:600, marginTop:'16px' }}>
            Alliance Bank Customer Intelligence
          </div>
          <div style={{ fontSize:'14px', marginTop:'8px' }}>
            Search for a customer to view their 360 profile and recommendations
          </div>
        </div>
      ) : (
        <div style={{ padding:'24px 32px', display:'flex', flexDirection:'column', gap:'20px' }}>

          {/* Hero card */}
          <div style={{ background:`linear-gradient(135deg, ${theme.colors.navy} 0%, ${theme.colors.navyLight} 100%)`,
                        borderRadius:'16px', padding:'24px', color:'white',
                        display:'flex', alignItems:'center', gap:'24px',
                        boxShadow:'0 4px 16px rgba(27,58,107,0.3)' }}>
            <div style={{ width:'64px', height:'64px', borderRadius:'50%',
                           background:'rgba(255,255,255,0.2)', display:'flex',
                           alignItems:'center', justifyContent:'center',
                           fontSize:'28px', fontWeight:800 }}>
              {customer.legal_name?.[0]}
            </div>
            <div style={{ flex:1 }}>
              <div style={{ fontSize:'24px', fontWeight:800 }}>{customer.legal_name}</div>
              <div style={{ opacity:0.8, fontSize:'14px', marginTop:'4px' }}>
                {customer.cif_number} · {customer.primary_state} · {customer.preferred_language_code === 'ms' ? 'BM' : 'EN'}
              </div>
              <span style={{ background:theme.colors.red, padding:'3px 10px', borderRadius:'12px',
                             fontSize:'11px', fontWeight:700, marginTop:'8px', display:'inline-block' }}>
                {segInfo?.label?.toUpperCase()}
              </span>
            </div>
            <div style={{ display:'flex', gap:'12px', flexWrap:'wrap' }}>
              {[
                { label:'Tenure', value:`${customer.relationship_tenure_years}yr` },
                { label:'NPS',    value: customer.nps_score || '—' },
                { label:'Risk',   value: customer.risk_rating?.toUpperCase() },
              ].map(k => (
                <div key={k.label}
                  style={{ textAlign:'center', background:'rgba(255,255,255,0.15)',
                           padding:'10px 16px', borderRadius:'10px' }}>
                  <div style={{ fontWeight:800, fontSize:'18px' }}>{k.value}</div>
                  <div style={{ fontSize:'11px', opacity:0.7 }}>{k.label}</div>
                </div>
              ))}
            </div>
          </div>

          {/* KPI row */}
          <div style={{ display:'flex', gap:'12px', flexWrap:'wrap' }}>
            <KPITile label="Total Balance"    value={`RM ${((customer.total_deposit_balance_myr||0)/1000).toFixed(0)}K`} sub="Deposit" />
            <KPITile label="CTOS Score"       value={customer.ctos_score || '—'} sub={customer.ccris_status?.toUpperCase()} />
            <KPITile label="Txn (30d)"        value={customer.txn_count_30d || 0} sub="transactions" />
            <KPITile label="Digital Score"    value={`${customer.digital_activity_score || 5}/10`} sub="activity" />
            <KPITile label="Products Held"    value={(customer.num_accounts||0) + (customer.num_loan_facilities||0)} sub="total" />
            <KPITile label="Loan Outstanding" value={`RM ${((customer.total_loan_outstanding_myr||0)/1000).toFixed(0)}K`} sub="active" />
          </div>

          {/* Split: 360 summary | Recommendations */}
          <div style={{ display:'grid', gridTemplateColumns:'1fr 340px', gap:'20px' }}>

            {/* Customer summary */}
            <div style={{ background:'white', borderRadius:'12px', padding:'20px',
                          boxShadow:'0 2px 8px rgba(0,0,0,0.06)' }}>
              <div style={{ fontWeight:700, fontSize:'15px', color:theme.colors.navy, marginBottom:'16px' }}>
                Customer 360 Summary
              </div>
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:'12px' }}>
                {[
                  { section:'Identity', items:[
                    ['Employment',    customer.employment_status?.replace(/_/g,' ')],
                    ['Education',     customer.education_level],
                    ['Marital Status', customer.marital_status],
                    ['Dependents',    customer.number_of_dependents],
                  ]},
                  { section:'Financial', items:[
                    ['Annual Income', `RM ${((customer.annual_income_amount||0)/1000).toFixed(0)}K`],
                    ['Net Worth',     customer.net_worth_band?.replace(/_/g,' ')],
                    ['Monthly Commit', `RM ${((customer.monthly_loan_commitment_myr||0)).toFixed(0)}`],
                    ['Avg Txn',       `RM ${((customer.avg_txn_amount_myr||0)).toFixed(0)}`],
                  ]},
                  { section:'Digital', items:[
                    ['Enrolled',    customer.digital_banking_enrollment_flag ? '✅' : '❌'],
                    ['Mobile App',  customer.mobile_app_user_flag  ? '✅' : '❌'],
                    ['Biometric',   customer.biometric_auth_enabled ? '✅' : '❌'],
                    ['Shariah Pref', customer.is_shariah_preferred  ? 'Yes' : 'No'],
                  ]},
                ].map(group => (
                  <div key={group.section}>
                    <div style={{ fontWeight:600, fontSize:'12px', color:theme.colors.red,
                                  textTransform:'uppercase', marginBottom:'8px' }}>
                      {group.section}
                    </div>
                    {group.items.map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'5px 0',
                                            borderBottom:`1px solid ${theme.colors.grey50}`,
                                            fontSize:'13px' }}>
                        <span style={{ color:theme.colors.grey600 }}>{k}</span>
                        <span style={{ fontWeight:500 }}>{v ?? '—'}</span>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>

            {/* Recommendations + AI */}
            {recs && (
              <div style={{ display:'flex', flexDirection:'column', gap:'12px' }}>
                <div style={{ background:'white', borderRadius:'12px', padding:'20px',
                              boxShadow:'0 2px 8px rgba(0,0,0,0.06)' }}>
                  <div style={{ fontWeight:700, fontSize:'15px', color:theme.colors.navy, marginBottom:'12px' }}>
                    🎯 Product Recommendations
                  </div>
                  <RecommendationCard rank={1} product={recs.recommendation_1}
                    confidence={recs.confidence_1} onDraftEmail={draftEmail} />
                  <div style={{ marginTop:'10px' }}>
                    <RecommendationCard rank={2} product={recs.recommendation_2}
                      confidence={recs.confidence_2} />
                  </div>
                </div>
                <div style={{ background:`linear-gradient(135deg, #EEF2FF, #E0E7FF)`,
                              borderRadius:'12px', padding:'16px', cursor:'pointer' }}
                  onClick={draftEmail}>
                  <div style={{ fontWeight:700, color:theme.colors.navy }}>✨ AI-Powered Actions</div>
                  <div style={{ fontSize:'13px', color:theme.colors.grey600, marginTop:'4px' }}>
                    Powered by GLM 5.2 via Unity AI Gateway
                  </div>
                  <button style={{ marginTop:'12px', width:'100%', padding:'10px',
                                   background:theme.colors.navy, color:'white', border:'none',
                                   borderRadius:'8px', cursor:'pointer', fontWeight:600, fontSize:'13px' }}>
                    ✉️ Draft Personalised Email
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Email modal */}
      {showEmailModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.5)',
                      display:'flex', alignItems:'center', justifyContent:'center', zIndex:200 }}>
          <div style={{ background:'white', borderRadius:'16px', padding:'24px',
                        width:'560px', maxWidth:'90vw',
                        boxShadow:'0 20px 60px rgba(0,0,0,0.3)' }}>
            <div style={{ display:'flex', justifyContent:'space-between',
                          alignItems:'center', marginBottom:'16px' }}>
              <div style={{ fontWeight:700, fontSize:'16px', color:theme.colors.navy }}>
                ✉️ AI-Drafted Email — {theme.products[recs?.recommendation_1 as keyof typeof theme.products]?.label}
              </div>
              <button onClick={() => setShowEmailModal(false)}
                style={{ background:'none', border:'none', cursor:'pointer', fontSize:'18px' }}>✕</button>
            </div>
            {draftLoading ? (
              <div style={{ textAlign:'center', padding:'40px', color:theme.colors.grey600 }}>
                ✨ GLM 5.2 is drafting your email...
              </div>
            ) : (
              <>
                <textarea value={emailDraft} onChange={e => setEmailDraft(e.target.value)}
                  style={{ width:'100%', height:'220px', padding:'12px', fontSize:'13px',
                           border:`1px solid ${theme.colors.grey200}`, borderRadius:'8px',
                           fontFamily:'inherit', resize:'vertical', lineHeight:1.6 }} />
                <div style={{ display:'flex', gap:'8px', marginTop:'12px', justifyContent:'flex-end' }}>
                  <button onClick={() => setShowEmailModal(false)}
                    style={{ padding:'8px 16px', border:`1px solid ${theme.colors.grey200}`,
                             borderRadius:'6px', background:'white', cursor:'pointer' }}>Cancel</button>
                  <button onClick={() => { navigator.clipboard.writeText(emailDraft); }}
                    style={{ padding:'8px 16px', background:theme.colors.navy, color:'white',
                             border:'none', borderRadius:'6px', cursor:'pointer', fontWeight:600 }}>
                    📋 Copy Email
                  </button>
                </div>
                <div style={{ marginTop:'8px', fontSize:'11px', color:theme.colors.grey600, textAlign:'right' }}>
                  Generated by system.ai.databricks-glm-5-2 via Unity AI Gateway
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/App.tsx`:

```tsx
import React, { useState } from 'react';
import Customer360ViewA from './pages/Customer360ViewA';
import Customer360ViewB from './pages/Customer360ViewB';
import { theme } from './theme';

export default function App() {
  const [template, setTemplate] = useState<'A' | 'B'>('B');
  return (
    <div>
      {/* Template switcher — demo-only */}
      <div style={{ position:'fixed', bottom:'16px', right:'16px', zIndex:300,
                    background:theme.colors.navy, borderRadius:'24px', padding:'6px',
                    display:'flex', gap:'4px', boxShadow:'0 4px 12px rgba(0,0,0,0.3)' }}>
        {(['A','B'] as const).map(t => (
          <button key={t} onClick={() => setTemplate(t)}
            style={{ padding:'6px 16px', borderRadius:'20px', border:'none',
                     cursor:'pointer', fontWeight:600, fontSize:'12px',
                     background: template === t ? theme.colors.red : 'transparent',
                     color: 'white' }}>
            Template {t}
          </button>
        ))}
      </div>
      {template === 'A' ? <Customer360ViewA /> : <Customer360ViewB />}
    </div>
  );
}
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/src/main.tsx`:

```tsx
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
```

---

### Task 14: App Deployment (app.yaml + Build + Deploy)

**Files to create:**
- `part_b_hyperpersonalization/app/app.yaml`
- `part_b_hyperpersonalization/app/frontend/package.json`
- `part_b_hyperpersonalization/app/frontend/vite.config.ts`
- `part_b_hyperpersonalization/app/frontend/tsconfig.json`
- `part_b_hyperpersonalization/app/frontend/index.html`

- [ ] Create `part_b_hyperpersonalization/app/app.yaml`:

```yaml
command: ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

resources:
  requests:
    cpu: "0.5"
    memory: "512Mi"

env:
  - name: DATABRICKS_USERNAME
    valueFrom:
      secretKeyRef:
        name: databricks-auth
        key: username
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/package.json`:

```json
{
  "name": "abmb-customer-intelligence",
  "version": "1.0.0",
  "scripts": {
    "dev":     "vite",
    "build":   "tsc && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react":     "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@types/react":        "^18.3.1",
    "@types/react-dom":    "^18.3.1",
    "typescript":          "^5.4.5",
    "vite":                "^5.3.1",
    "@vitejs/plugin-react": "^4.3.1"
  }
}
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/vite.config.ts`:

```typescript
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: { outDir: 'dist' },
  server: { proxy: { '/api': 'http://localhost:8000' } }
});
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target":             "ES2020",
    "useDefineForClassFields": true,
    "lib":                ["ES2020","DOM","DOM.Iterable"],
    "module":             "ESNext",
    "skipLibCheck":       true,
    "moduleResolution":   "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule":  true,
    "isolatedModules":    true,
    "noEmit":             true,
    "jsx":                "react-jsx",
    "strict":             true,
    "noUnusedLocals":     true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["src"]
}
```

- [ ] Create `part_b_hyperpersonalization/app/frontend/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Alliance Bank | Customer Intelligence Platform</title>
  <link rel="icon" href="/alliance_bank_logo.png" />
</head>
<body>
  <div id="root"></div>
  <script type="module" src="/src/main.tsx"></script>
</body>
</html>
```

- [ ] Build frontend and deploy app:

```bash
# Step 1: Build frontend
cd part_b_hyperpersonalization/app/frontend
npm install
npm run build
# Output: dist/ directory

# Step 2: Copy dist into app directory where FastAPI can serve it
cp -r dist/ ../frontend/dist/

# Step 3: Upload Alliance Bank logo to UC Volume
databricks fs cp ../../data/alliance_bank_logo.png \
  dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/raw_data/alliance_bank_logo.png \
  --profile fevm-master-classic-marcus

# Step 4: Create Databricks App
cd ..
databricks apps create abmb-customer-intelligence \
  --profile fevm-master-classic-marcus

# Step 5: Deploy app
databricks apps deploy abmb-customer-intelligence \
  --source-code-path . \
  --profile fevm-master-classic-marcus

# Step 6: Get app URL
databricks apps get abmb-customer-intelligence \
  --profile fevm-master-classic-marcus
```

---

## Self-Review: Spec Coverage Checklist

| Requirement | Status |
|---|---|
| Depends on Part A `gold_customer_360` | Task 1 validates row count >= 900 |
| All assets in `fevm_master_classic_marcus_catalog.abmb_rfp_presentation` | Global constraint + all notebooks |
| Lakebase project `ABMB-RFP-PRESENTATION` | Task 1 validates connectivity; Task 4 documents synced table setup |
| XGBoost multi-class (6 classes) | Task 5: `multi:softprob`, `num_class=6` |
| MLflow experiment `abmb-product-recommendation-training` | Tasks 1, 5, 8, 9 |
| Model registry `fevm_master_classic_marcus_catalog.abmb_rfp_presentation.abmb_recommendation_model` | Tasks 5, 6, 7, 8, 9 |
| Model aliases `@dev` → `@champion` | Tasks 5 (tags @dev), 7 (promotes to @champion) |
| Feature Store (FeatureEngineeringClient) | Tasks 4, 5, 6 |
| 20 model features | Task 4: explicit list of 20 encoded features |
| Batch inference → `gold_product_recommendations` | Task 6 |
| Real-time serving endpoint | Task 7 |
| AI Gateway inference logging | Task 7 (endpoint config), Task 10 (GLM service) |
| Unity AI Gateway (GLM 5.2) | Task 10: full 6-section demo |
| GLM 5.2 traffic split with GLM 5.3 Flash | Task 10 Section E |
| `system.ai_gateway.usage` audit query | Task 10 Section F |
| Banking guardrails demo | Task 10 Section D |
| FastAPI backend | Task 11: all 6 endpoints |
| React 18 + TypeScript + Vite | Tasks 12–14 |
| Two UI templates (A: Banker's Workstation, B: Intelligence Hub) | Tasks 12, 13 |
| Alliance Bank branding (Navy #1B3A6B, Red #C8102E) | theme.ts + all components |
| Email draft modal with editable textarea | Tasks 12 (right rail), 13 (modal) |
| Databricks App deployment (app.yaml) | Task 14 |
| Serverless ML compute on all notebooks | Global constraint noted |
| `%pip install` + `dbutils.library.restartPython()` pattern | Tasks 3, 4, 5, 6, 8 |
| SHAP feature importance | Tasks 5 (summary plot), 8 (waterfall) |
| Lakehouse Monitor on endpoint payload | Task 8 Section C |
| 3-stage deploy gate (evaluate/approve/deploy) | Task 9 |
