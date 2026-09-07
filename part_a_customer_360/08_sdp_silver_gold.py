# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold Pipeline (Spark Declarative Pipeline)
# MAGIC ## gold_customer_360 — Unified Customer View
# MAGIC
# MAGIC Assembles 1 row per active customer from 7 silver tables, producing ~65 features.
# MAGIC This is the certified data product consumed by Part B (AI/BI Dashboard) and
# MAGIC Part C (Next Best Product ML model).
# MAGIC
# MAGIC | Silver Table | Source Domain | Role in gold_customer_360 |
# MAGIC |---|---|---|
# MAGIC | silver_customers | customer | Identity, demographics, segmentation (base) |
# MAGIC | silver_deposit_accounts | account | Account holdings summary |
# MAGIC | silver_loan_accounts | loan | Loan facilities summary |
# MAGIC | silver_transactions | payment | Transaction behaviour (core banking) |
# MAGIC | silver_card_transactions | payment | Card spend behaviour |
# MAGIC | silver_kyc_compliance | compliance | CTOS/CCRIS credit bureau |
# MAGIC | silver_digital_activity | channel | Digital & telco engagement |
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC 7x silver tables
# MAGIC       │
# MAGIC       ├──► _agg_accounts      ─┐
# MAGIC       ├──► _agg_loans          ├──► gold_customer_360
# MAGIC       ├──► _agg_transactions   │         (1 row / active customer)
# MAGIC       └──► _agg_digital       ─┘
# MAGIC ```

# COMMAND ----------
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

FULL = "fevm_master_classic_marcus_catalog.abmb_rfp_presentation"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Intermediate Aggregations
# MAGIC
# MAGIC Four batch `@dlt.table` nodes pre-aggregate each domain before the final join.
# MAGIC This pattern avoids a single mega-join and makes lineage easy to inspect
# MAGIC in the pipeline DAG.

# COMMAND ----------
# MAGIC %md #### _agg_accounts — deposit account holdings per customer

# COMMAND ----------
@dlt.table(name="_agg_accounts", comment="Account aggregates per customer")
def _agg_accounts():
    return (spark.table(f"{FULL}.silver_deposit_accounts")
        .filter("__END_AT IS NULL")  # SCD2 current rows only
        .groupBy("party_id")
        .agg(
            count("deposit_account_id").alias("num_accounts"),
            sum(when(col("account_status") == "active", col("current_balance")).otherwise(0)).alias("total_deposit_balance_myr"),
            max(when(col("account_type") == "DDA",                    lit(True)).otherwise(lit(False))).alias("has_current_account"),
            max(when(col("account_type") == "savings",                lit(True)).otherwise(lit(False))).alias("has_savings_account"),
            max(when(col("account_type") == "certificate_of_deposit", lit(True)).otherwise(lit(False))).alias("has_fixed_deposit"),
            avg("current_balance").alias("avg_account_balance"),
        ))

# COMMAND ----------
# MAGIC %md #### _agg_loans — loan facility summary per customer

# COMMAND ----------
@dlt.table(name="_agg_loans", comment="Loan aggregates per customer")
def _agg_loans():
    return (spark.table(f"{FULL}.silver_loan_accounts")
        .filter("__END_AT IS NULL")  # SCD2 current rows only
        .groupBy("party_id")
        .agg(
            count("loan_account_id").alias("num_loan_facilities"),
            sum(when(col("loan_status") == "active", col("outstanding_principal_myr")).otherwise(0)).alias("total_loan_outstanding_myr"),
            sum(when(col("loan_status") == "active", col("monthly_installment_myr")).otherwise(0)).alias("monthly_loan_commitment_myr"),
            max(when(col("loan_type") == "home_loan",     lit(True)).otherwise(lit(False))).alias("has_home_loan"),
            max(when(col("loan_type") == "personal_loan", lit(True)).otherwise(lit(False))).alias("has_personal_loan"),
            max(when(col("loan_type") == "hire_purchase", lit(True)).otherwise(lit(False))).alias("has_hire_purchase"),
        ))

# COMMAND ----------
# MAGIC %md #### _agg_transactions — transaction behaviour per customer (core banking + card)

# COMMAND ----------
@dlt.table(name="_agg_transactions", comment="Transaction behaviour aggregates per customer")
def _agg_transactions():
    today = current_date()
    txn   = spark.table(f"{FULL}.silver_transactions")
    card  = spark.table(f"{FULL}.silver_card_transactions")

    txn_agg = (txn.groupBy("party_id").agg(
        count(when(datediff(today, col("posting_date")) <= 30,  1)).alias("txn_count_30d"),
        count(when(datediff(today, col("posting_date")) <= 90,  1)).alias("txn_count_90d"),
        count(when(datediff(today, col("posting_date")) <= 365, 1)).alias("txn_count_12m"),
        sum(when((col("txn_type") == "DEBIT")  & (datediff(today, col("posting_date")) <= 30), col("payment_amount")).otherwise(0)).alias("total_debit_30d_myr"),
        sum(when((col("txn_type") == "CREDIT") & (datediff(today, col("posting_date")) <= 30), col("payment_amount")).otherwise(0)).alias("total_credit_30d_myr"),
        avg("payment_amount").alias("avg_txn_amount_myr"),
    ))

    card_agg = (card.groupBy("party_id").agg(
        lit(True).alias("has_credit_card"),
        sum(when(datediff(today, col("posting_date")) <= 30, col("txn_amount")).otherwise(0)).alias("card_spend_30d_myr"),
        max(col("is_overseas").cast("boolean")).alias("overseas_txn_flag"),
    ))

    return txn_agg.join(card_agg, "party_id", "left")

# COMMAND ----------
# MAGIC %md #### _agg_digital — digital banking and telco engagement per customer

# COMMAND ----------
@dlt.table(name="_agg_digital", comment="Digital + telco activity aggregates per customer")
def _agg_digital():
    da    = spark.table(f"{FULL}.silver_digital_activity")
    today = current_date()

    digital = (da.filter(col("source") == "digital_banking")
        .groupBy("party_id")
        .agg(
            count(when(datediff(today, col("event_timestamp").cast("date")) <= 30, 1)).alias("mobile_sessions_30d"),
            count(when(col("event_type") == "view_product", 1)).alias("product_views_last_30d"),
            last(when(col("event_type") == "view_product", col("product_category_viewed"))).alias("last_product_category_viewed"),
        ))

    telco = (da.filter(col("source") == "telco")
        .groupBy("party_id")
        .agg(
            avg("digital_maturity_score").alias("digital_maturity_score"),
            avg("data_consumption_mb_monthly").alias("data_consumption_mb_monthly"),
            avg("arpu_myr").alias("telco_arpu_myr"),
        ))

    return (digital.join(telco, "party_id", "left")
        .withColumn("digital_activity_score",
                    round(coalesce(col("mobile_sessions_30d"),     lit(0))   * 0.4 +
                          coalesce(col("digital_maturity_score"),  lit(5.0)) * 0.6, 1))
        .withColumn("telco_data_consumption_gb_monthly",
                    round(coalesce(col("data_consumption_mb_monthly"), lit(0)) / 1024, 2)))

# COMMAND ----------
# MAGIC %md
# MAGIC ### gold_customer_360 — Final Assembly
# MAGIC
# MAGIC Joins all 7 silver tables (4 via intermediate aggregates, 3 direct) into a single
# MAGIC denormalised row per active customer. Data quality gates enforce:
# MAGIC - `cif_number IS NOT NULL` — every row must have a valid CIF (**drop** on failure)
# MAGIC - `lifecycle_status = 'active'` — inactive customers excluded (**drop** on failure)
# MAGIC - `legal_name IS NOT NULL` — warn only (logged to DQ metrics, row kept)

# COMMAND ----------
@dlt.expect_or_drop("gold_valid_cif",       "cif_number IS NOT NULL")
@dlt.expect_or_drop("gold_active_customer", "lifecycle_status = 'active'")
@dlt.expect("gold_has_name",                "legal_name IS NOT NULL")
@dlt.table(
    name="gold_customer_360",
    comment="Unified Customer 360 — 1 row per active customer, 65+ features. Certified data product.",
    table_properties={"quality": "gold", "certified": "true"}
)
def gold_customer_360():
    c    = spark.table(f"{FULL}.silver_customers").filter("__END_AT IS NULL")
    acct = dlt.read("_agg_accounts")
    loan = dlt.read("_agg_loans")
    txn  = dlt.read("_agg_transactions")
    kb   = spark.table(f"{FULL}.silver_kyc_compliance")
    da   = dlt.read("_agg_digital")

    return (c
        .join(acct, c.party_id == acct.party_id, "left").drop(acct.party_id)
        .join(loan, c.party_id == loan.party_id, "left").drop(loan.party_id)
        .join(txn,  c.party_id == txn.party_id,  "left").drop(txn.party_id)
        .join(kb,   c.cif_number == kb.cif_number, "left")
        .join(da,   c.party_id == da.party_id,   "left").drop(da.party_id)
        .select(
            # ── Identity ──────────────────────────────────────────────────────
            c.party_id,
            c.cif_number,
            c.legal_name,
            c.first_name,
            c.last_name,
            c.national_id_number,
            c.date_of_birth,
            (datediff(current_date(), c.date_of_birth.cast("date")) / 365).cast("int").alias("age"),
            c.gender,
            c.citizenship_country_code,
            c.residency_status,
            c.customer_segment,
            c.lifestyle_segment,
            c.employment_status,
            c.employer_name,
            c.annual_income_amount,
            c.marital_status,
            c.number_of_dependents,
            c.education_level,
            c.is_pep,
            c.risk_rating,
            c.kyc_status,
            c.lifecycle_status,
            (datediff(current_date(), c.relationship_start_date.cast("date")) / 365).cast("int").alias("relationship_tenure_years"),
            c.digital_banking_enrollment_flag,
            c.mobile_app_user_flag,
            c.marketing_consent_flag,
            c.net_worth_band,
            c.nps_score,
            c.is_shariah_preferred,
            c.preferred_language_code,
            c.preferred_contact_method,
            c.primary_state,
            # ── Accounts ──────────────────────────────────────────────────────
            coalesce(acct.num_accounts,              lit(0)).alias("num_accounts"),
            coalesce(acct.total_deposit_balance_myr, lit(0.0)).alias("total_deposit_balance_myr"),
            coalesce(acct.has_current_account,       lit(False)).alias("has_current_account"),
            coalesce(acct.has_savings_account,       lit(False)).alias("has_savings_account"),
            coalesce(acct.has_fixed_deposit,         lit(False)).alias("has_fixed_deposit"),
            # ── Loans ─────────────────────────────────────────────────────────
            coalesce(loan.num_loan_facilities,         lit(0)).alias("num_loan_facilities"),
            coalesce(loan.total_loan_outstanding_myr,  lit(0.0)).alias("total_loan_outstanding_myr"),
            coalesce(loan.monthly_loan_commitment_myr, lit(0.0)).alias("monthly_loan_commitment_myr"),
            coalesce(loan.has_home_loan,               lit(False)).alias("has_home_loan"),
            coalesce(loan.has_personal_loan,           lit(False)).alias("has_personal_loan"),
            coalesce(loan.has_hire_purchase,           lit(False)).alias("has_hire_purchase"),
            # ── Transactions ──────────────────────────────────────────────────
            coalesce(txn.txn_count_30d,        lit(0)).alias("txn_count_30d"),
            coalesce(txn.txn_count_90d,        lit(0)).alias("txn_count_90d"),
            coalesce(txn.txn_count_12m,        lit(0)).alias("txn_count_12m"),
            coalesce(txn.total_debit_30d_myr,  lit(0.0)).alias("total_debit_30d_myr"),
            coalesce(txn.total_credit_30d_myr, lit(0.0)).alias("total_credit_30d_myr"),
            coalesce(txn.avg_txn_amount_myr,   lit(0.0)).alias("avg_txn_amount_myr"),
            coalesce(txn.has_credit_card,      lit(False)).alias("has_credit_card"),
            coalesce(txn.card_spend_30d_myr,   lit(0.0)).alias("card_spend_30d_myr"),
            coalesce(txn.overseas_txn_flag,    lit(False)).alias("overseas_txn_flag"),
            # ── Credit Bureau (CTOS / CCRIS) ──────────────────────────────────
            kb.ctos_score,
            kb.ccris_status,
            coalesce(kb.total_credit_facilities,     lit(0)).alias("total_credit_facilities"),
            coalesce(kb.monthly_total_commitment_myr, lit(0.0)).alias("kb_monthly_commitment_myr"),
            kb.payment_conduct_12m,
            coalesce(kb.legal_cases_count,           lit(0)).alias("legal_cases_count"),
            kb.bankruptcy_status,
            coalesce(kb.credit_card_count,           lit(0)).alias("credit_card_bureau_count"),
            coalesce(kb.inquiry_count_last_6m,       lit(0)).alias("inquiry_count_last_6m"),
            # ── Digital ───────────────────────────────────────────────────────
            coalesce(da.digital_activity_score,              lit(5.0)).alias("digital_activity_score"),
            coalesce(da.mobile_sessions_30d,                 lit(0)).alias("mobile_sessions_30d"),
            coalesce(da.product_views_last_30d,              lit(0)).alias("product_views_last_30d"),
            da.last_product_category_viewed,
            coalesce(da.telco_data_consumption_gb_monthly,   lit(0.0)).alias("telco_data_consumption_gb_monthly"),
            coalesce(da.telco_arpu_myr,                      lit(0.0)).alias("telco_arpu_myr"),
            coalesce(da.digital_maturity_score,              lit(5.0)).alias("digital_maturity_score"),
            # ── Product ownership flags (integer, for ML model) ───────────────
            when(coalesce(txn.has_credit_card,   lit(False)), 1).otherwise(0).alias("owns_credit_card"),
            when(coalesce(loan.has_home_loan,    lit(False)), 1).otherwise(0).alias("owns_home_loan"),
            when(coalesce(loan.has_personal_loan, lit(False)), 1).otherwise(0).alias("owns_personal_loan"),
            # ── Audit ─────────────────────────────────────────────────────────
            current_timestamp().alias("gold_computed_at"),
        ))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Deploy
# MAGIC
# MAGIC 1. Pipelines → Create Pipeline
# MAGIC 2. Name: `ABMB-Silver-Gold`
# MAGIC 3. Source: this notebook (`08_sdp_silver_gold.py`)
# MAGIC 4. Target catalog: `fevm_master_classic_marcus_catalog`
# MAGIC 5. Target schema: `abmb_rfp_presentation`
# MAGIC 6. Serverless → Start
# MAGIC 7. Validate:
# MAGIC    ```sql
# MAGIC    SELECT COUNT(*) FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.gold_customer_360
# MAGIC    ```
# MAGIC    Expected: ~1000 rows
# MAGIC
# MAGIC ### Pipeline DAG (expected)
# MAGIC ```
# MAGIC silver_deposit_accounts ──► _agg_accounts ──┐
# MAGIC silver_loan_accounts ────► _agg_loans ───────┤
# MAGIC silver_transactions ─────┐                   ├──► gold_customer_360
# MAGIC silver_card_transactions ┴► _agg_transactions┤
# MAGIC silver_digital_activity ─► _agg_digital ─────┤
# MAGIC silver_customers ─────────────────────────────┤
# MAGIC silver_kyc_compliance ────────────────────────┘
# MAGIC ```
