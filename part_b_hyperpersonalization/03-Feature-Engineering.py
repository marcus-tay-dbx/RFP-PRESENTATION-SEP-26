# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Feature Engineering — Alliance Bank Product Recommendation
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Loads `gold_customer_360` from Part A
# MAGIC 2. Encodes categorical columns into numeric features
# MAGIC 3. Engineers tenure_years from relationship_start_date
# MAGIC 4. Generates the `next_best_product` label using business rules
# MAGIC 5. Publishes to Unity Catalog Feature Store (FeatureEngineeringClient)

# COMMAND ----------
# MAGIC %md ## 1. Load Source Data

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

raw = (
    spark.table(f"{da.full_schema}.gold_customer_360")
         .dropDuplicates(["party_id"])
)

print(f"Source rows (deduplicated on party_id): {raw.count():,}")
display(raw.limit(5))

# COMMAND ----------
# MAGIC %md ## 2. Encode Categorical Columns

# COMMAND ----------
# net_worth_band  → integer rank
net_worth_map = {
    "under_100k":    0,
    "100k_to_250k":  1,
    "250k_to_500k":  2,
    "500k_to_1m":    3,
    "1m_to_5m":      4,
    "over_5m":       5,
}

# ccris_status → integer
ccris_map = {
    "clean":        0,
    "performing":   1,
    "watchlist":    2,
    "non_performing": 3,
    "default":      4,
}

# employment_status → integer
employment_map = {
    "employed":          0,
    "self_employed":     1,
    "business_owner":    2,
    "retired":           3,
    "unemployed":        4,
}

# last_product_category_viewed → integer
category_map = {
    "savings":           0,
    "credit_card":       1,
    "home_loan":         2,
    "personal_loan":     3,
    "investment":        4,
    "insurance":         5,
    "hire_purchase":     6,
    "fixed_deposit":     7,
    "remittance":        8,
    "none":              9,
}

def map_col(col_name, mapping, default=0):
    """Map string column to integer using a dictionary."""
    expr = F.lit(default)
    for k, v in mapping.items():
        expr = F.when(F.lower(F.col(col_name)) == k.lower(), v).otherwise(expr)
    return expr

df = (
    raw
    # ── Tenure — use pre-computed column from gold_customer_360 ──────────────
    # gold_customer_360 already computes relationship_tenure_years; rename it
    .withColumn("tenure_years", F.col("relationship_tenure_years").cast(FloatType()))
    # ── Encodings ─────────────────────────────────────────────────────────────
    .withColumn("net_worth_band_encoded",           map_col("net_worth_band",                net_worth_map))
    .withColumn("ccris_status_encoded",             map_col("ccris_status",                  ccris_map))
    .withColumn("employment_status_encoded",        map_col("employment_status",              employment_map))
    .withColumn("last_product_category_viewed_encoded", map_col("last_product_category_viewed", category_map))
    # ── Boolean coercions ─────────────────────────────────────────────────────
    .withColumn("is_shariah_preferred_int",
                F.col("is_shariah_preferred").cast(IntegerType()))
    .withColumn("has_credit_card",
                F.col("has_credit_card").cast(IntegerType()))
    .withColumn("has_home_loan",
                F.col("has_home_loan").cast(IntegerType()))
    .withColumn("has_personal_loan",
                F.col("has_personal_loan").cast(IntegerType()))
    # ── Payment conduct score (synthetic if absent) ───────────────────────────
    .withColumn(
        "payment_conduct_score",
        F.when(F.col("nps_score").isNotNull(), F.col("nps_score")).otherwise(F.lit(50.0))
    )
    # ── Null guards ───────────────────────────────────────────────────────────
    .fillna({
        "ctos_score":                     600.0,
        "annual_income_amount":           36000.0,
        "total_deposit_balance_myr":      5000.0,
        "monthly_loan_commitment_myr":    0.0,
        "num_accounts":                   1,
        "num_loan_facilities":            0,
        "txn_count_30d":                  0,
        "avg_txn_amount_myr":             0.0,
        "digital_maturity_score":         50.0,
        "telco_arpu_myr":                 50.0,
        "number_of_dependents":           0,
        "tenure_years":                   1.0,
    })
)

print(f"After encoding: {df.count():,} rows | {len(df.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 3. Generate next_best_product Label (Business Rules)
# MAGIC
# MAGIC Rules applied in priority order — each customer gets **exactly one** label:
# MAGIC
# MAGIC | Priority | Label | Rule |
# MAGIC |---|---|---|
# MAGIC | 1 | CREDIT_CARD | ctos > 700, income > 60K, no existing credit card |
# MAGIC | 2 | HOME_LOAN | age 28-45, has dependents, no home loan, income > 48K |
# MAGIC | 3 | PERSONAL_LOAN | monthly_commitment / income > 20% (refinance signal) |
# MAGIC | 4 | INVESTMENT | net_worth in [500k–5m], income > 100K |
# MAGIC | 5 | INSURANCE | no insurance (proxied: has hire purchase, no specific insurance) |
# MAGIC | 6 | NO_ACTION | fallback |

# COMMAND ----------
from pyspark.sql.functions import when, col, lit

df = df.withColumn(
    "next_best_product",
    when(
        (col("ctos_score") > 700) &
        (col("annual_income_amount") > 60000) &
        (col("has_credit_card") == 0),
        lit("CREDIT_CARD")
    ).when(
        (col("age") >= 28) & (col("age") <= 45) &
        (col("number_of_dependents") > 0) &
        (col("has_home_loan") == 0) &
        (col("annual_income_amount") > 48000),
        lit("HOME_LOAN")
    ).when(
        (col("annual_income_amount") > 0) &
        (col("monthly_loan_commitment_myr") / col("annual_income_amount") * 12 > 0.20),
        lit("PERSONAL_LOAN")
    ).when(
        (col("net_worth_band_encoded") >= 3) &    # 500k_to_1m or above
        (col("annual_income_amount") > 100000),
        lit("INVESTMENT")
    ).when(
        (col("num_loan_facilities") > 0) &        # has some facility (proxy for hire purchase)
        (col("payment_conduct_score") >= 40),     # reasonable conduct — can afford insurance
        lit("INSURANCE")
    ).otherwise(lit("NO_ACTION"))
)

# COMMAND ----------
# MAGIC %md ## 4. Verify Label Distribution
# MAGIC *All 6 classes must be present for multi-class training to succeed*

# COMMAND ----------
from pyspark.sql.functions import count

dist = (
    df.groupBy("next_best_product")
      .agg(count("*").alias("n"))
      .orderBy("n", ascending=False)
)
display(dist)

# Validate all classes present
present_classes = set(r["next_best_product"] for r in dist.collect())
expected_classes = set(DA.LABEL_CLASSES)
missing = expected_classes - present_classes
if missing:
    print(f"WARNING: Missing classes: {missing}. Business rules may need tuning.")
else:
    print(f"All 6 label classes present: {sorted(present_classes)}")

# COMMAND ----------
# MAGIC %md ## 5. Select Feature Columns and Write to Feature Store

# COMMAND ----------
feature_cols = ["party_id", "next_best_product"] + DA.FEATURES

# Verify all feature columns exist
missing_cols = [c for c in feature_cols if c not in df.columns]
if missing_cols:
    raise ValueError(f"Missing columns in DataFrame: {missing_cols}")

feature_df = df.select(feature_cols)
print(f"Feature table shape: {feature_df.count():,} rows × {len(feature_cols)} columns")
display(feature_df.limit(5))

# COMMAND ----------
# MAGIC %md ## 6. Publish to Unity Catalog Feature Store

# COMMAND ----------
# Drop and recreate to ensure clean write
spark.sql(f"DROP TABLE IF EXISTS {da.feature_table}")

fe.create_table(
    name=da.feature_table,
    primary_keys=["party_id"],
    df=feature_df,
    description=(
        "Customer features for Alliance Bank next-best-product recommendation model. "
        "Engineered from gold_customer_360. Primary key: party_id. "
        "Label: next_best_product (6 classes)."
    ),
    tags={"team": "ml_platform", "domain": "retail_banking", "model": "product_recommendation"}
)

print(f"Feature table created: {da.feature_table}")
print(f"  Rows:    {spark.table(da.feature_table).count():,}")
print(f"  Columns: {len(spark.table(da.feature_table).columns)}")

# COMMAND ----------
# MAGIC %md ## 7. Verify Feature Store Entry

# COMMAND ----------
meta = fe.get_table(name=da.feature_table)
print(f"Name:         {meta.name}")
print(f"Primary keys: {meta.primary_keys}")
print(f"Description:  {meta.description}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Lakebase Synced Table (Reference Architecture)
# MAGIC
# MAGIC For production deployments, the feature table can be synced to a Lakebase
# MAGIC (managed Postgres) instance for sub-millisecond real-time lookups:
# MAGIC
# MAGIC ```python
# MAGIC # After creating a Lakebase project via:
# MAGIC #   databricks lakebase instances create --name alliance-bank-features
# MAGIC
# MAGIC # Sync the feature table to Lakebase for real-time serving:
# MAGIC # (Run in a notebook connected to your Lakebase project)
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC   CREATE OR REPLACE SYNCED TABLE lakebase.public.customer_features
# MAGIC   FROM {da.feature_table}
# MAGIC   SYNC KEYS (party_id)
# MAGIC """)
# MAGIC
# MAGIC # The feature server then looks up party_id → features in < 2ms P99
# MAGIC # instead of the ~50ms Delta scan path.
# MAGIC ```
# MAGIC
# MAGIC This pattern is documented in: `databricks lakebase --help`

# COMMAND ----------
print("Feature engineering complete.")
print(f"  Feature table: {da.feature_table}")
print(f"  Next step:     04-Model-Training.py")
