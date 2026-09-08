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

# Label generation using Spark when() — type-safe, no Python UDF serialisation issues
# Explicit .cast() ensures correct types regardless of how AutoLoader inferred the schema
label_expr = (
    when(
        (col("owns_credit_card").cast("int") == 0) &
        col("ctos_score").cast("double").isNotNull() &
        (col("ctos_score").cast("double") > 700) &
        (col("annual_income_amount").cast("double") > 60000),
        "CREDIT_CARD"
    ).when(
        (col("owns_home_loan").cast("int") == 0) &
        col("age").cast("double").isNotNull() &
        col("age").cast("double").between(28, 45) &
        (col("marital_status") == "married") &
        (col("annual_income_amount").cast("double") > 48000),
        "HOME_LOAN"
    ).when(
        (col("owns_personal_loan").cast("int") == 0) &
        col("monthly_loan_commitment_myr").cast("double").isNotNull() &
        (col("annual_income_amount").cast("double") > 0) &
        ((col("monthly_loan_commitment_myr").cast("double") * 12 /
          col("annual_income_amount").cast("double")) > 0.2),
        "PERSONAL_LOAN"
    ).when(
        col("net_worth_band").isin("500k_to_1m", "1m_to_5m", "5m_to_10m", "over_10m") &
        (col("has_fixed_deposit").cast("boolean") == False),
        "INVESTMENT"
    ).when(
        (col("has_hire_purchase").cast("boolean") == True) &
        (col("owns_credit_card").cast("int") == 0),
        "INSURANCE"
    ).otherwise("NO_ACTION")
)

features_df = (gold
    .withColumn("next_best_product", label_expr)
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
        col("total_deposit_balance_myr").cast("double").alias("total_deposit_balance_myr"),
        col("num_accounts").cast("double").alias("num_accounts"),
        col("annual_income_amount").cast("double").alias("annual_income_amount"),
        col("net_worth_band_encoded"),
        col("ctos_score").cast("double").alias("ctos_score"),
        col("ccris_status_encoded"),
        col("payment_conduct_score"),
        col("age").cast("double").alias("age"),
        col("number_of_dependents").cast("double").alias("number_of_dependents"),
        col("employment_status_encoded"),
        col("monthly_loan_commitment_myr").cast("double").alias("monthly_loan_commitment_myr"),
        col("num_loan_facilities").cast("double").alias("num_loan_facilities"),
        col("txn_count_30d").cast("double").alias("txn_count_30d"),
        col("avg_txn_amount_myr").cast("double").alias("avg_txn_amount_myr"),
        col("has_credit_card").cast("double").alias("has_credit_card"),  # bool→double directly
        col("digital_maturity_score").cast("double").alias("digital_maturity_score"),
        col("telco_arpu_myr").cast("double").alias("telco_arpu_myr"),
        col("is_shariah_preferred_int").cast("double").alias("is_shariah_preferred_int"),
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
)
print(f"✅ Feature table created: {FEATURE_TABLE}")
print(f"   Rows: {spark.table(FEATURE_TABLE).count()}")
print(f"   Features: {len(features_df.columns) - 1}")

# COMMAND ----------
# MAGIC %md ## Sync to Lakebase for Real-Time Lookup
# MAGIC Enable Lakebase Synced Tables in the UI:
# MAGIC 1. Go to Catalog → fevm_master_classic_marcus_catalog → abmb_rfp_presentation → customer_features
# MAGIC 2. Click "Enable Lakebase Sync" → select project DBX-RFP-PRESENTATION
# MAGIC 3. This creates a live Postgres replica for sub-millisecond feature lookup during serving
# MAGIC
# MAGIC Note: Lakebase synced table setup is UI-only (CLI does not support it yet).

# COMMAND ----------
# Validation
cnt = spark.table(FEATURE_TABLE).count()
assert cnt >= 800, f"Expected >= 800 rows, got {cnt}"  # ~88% lifecycle_status='active'
label_dist = spark.table(FEATURE_TABLE).groupBy("next_best_product").count().collect()
print("✅ Label distribution:")
for r in sorted(label_dist, key=lambda x: -x['count']):
    print(f"   {r['next_best_product']}: {r['count']} ({r['count']/cnt*100:.1f}%)")
