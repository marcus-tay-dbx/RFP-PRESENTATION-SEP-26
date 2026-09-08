# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Part B — Overview: Hyperpersonalization with Databricks
# MAGIC
# MAGIC ## Business Problem
# MAGIC DBX Bank Malaysia serves over 1.2 million retail customers across 5 segments (Mass Market,
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
