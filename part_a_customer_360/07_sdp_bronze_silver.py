# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver Pipeline (Spark Declarative Pipeline)
# MAGIC
# MAGIC ## Enterprise Data Model Context
# MAGIC These silver tables follow the **Databricks Banking Minimum Viable Model (MVM)** —
# MAGIC a pre-built industry data model with **17 domains** and **227 tables** for banking.
# MAGIC We populate **6 of the 17 domains** with data from Alliance Bank's source systems.
# MAGIC
# MAGIC | Silver Table | MVM Domain | MVM Entity |
# MAGIC |---|---|---|
# MAGIC | silver_customers | customer | party + individual_profile |
# MAGIC | silver_deposit_accounts | account | deposit_account |
# MAGIC | silver_loan_accounts | loan | loan_account |
# MAGIC | silver_transactions | payment | payment_transaction |
# MAGIC | silver_card_transactions | payment | payment_instruction (card) |
# MAGIC | silver_kyc_compliance | compliance | kyc_review + CTOS/CCRIS |
# MAGIC | silver_digital_activity | channel | digital_channel events |
# MAGIC
# MAGIC Full Banking MVM DDL:
# MAGIC https://github.com/databricks-industry-solutions/lakehouse-industry-data-models/tree/main/data-models/banking/v1/mvm

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "abmb_rfp_presentation"
FULL    = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
# MAGIC %md
# MAGIC ### silver_customers (SCD Type 2 — MVM: customer.party + individual_profile)
# MAGIC
# MAGIC `dlt.apply_changes` manages full SCD Type 2 history automatically.
# MAGIC The pipeline tracks every change to a customer record using `cif_number` as the key
# MAGIC and `record_updated_timestamp` as the sequence column.
# MAGIC
# MAGIC > **Note on Data Quality for SCD Type 2 targets:** DLT does not support `@dlt.expect`
# MAGIC > decorators directly on `apply_changes` targets. Quality checks for SCD2 tables should
# MAGIC > be enforced upstream (in the bronze ingestion layer via `rescuedDataColumn`) or in a
# MAGIC > downstream gold-layer view that reads from the silver SCD2 table.

# COMMAND ----------
dp.create_streaming_table(
    name="silver_customers",
    comment="Banking MVM: customer domain — party + individual_profile. SCD Type 2.",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "true"}
)

dp.create_auto_cdc_flow(
    target="silver_customers",
    source=f"{FULL}.bronze_customer_master",
    keys=["cif_number"],
    sequence_by=col("record_updated_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_rescued_data", "_source_file", "_ingest_timestamp"]
)

# COMMAND ----------
# MAGIC %md ### silver_deposit_accounts (SCD Type 2 — MVM: account.deposit_account)

# COMMAND ----------
dp.create_streaming_table(
    name="silver_deposit_accounts",
    comment="Banking MVM: account domain — deposit_account. SCD Type 2.",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "true"}
)

dp.create_auto_cdc_flow(
    target="silver_deposit_accounts",
    source=f"{FULL}.bronze_core_banking_accounts",
    keys=["deposit_account_id"],
    sequence_by=col("last_modified_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_rescued_data", "_source_file"]
)

# COMMAND ----------
# MAGIC %md ### silver_loan_accounts (SCD Type 2 — MVM: loan.loan_account)

# COMMAND ----------
dp.create_streaming_table(
    name="silver_loan_accounts",
    comment="Banking MVM: loan domain — loan_account. SCD Type 2.",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "true"}
)

dp.create_auto_cdc_flow(
    target="silver_loan_accounts",
    source=f"{FULL}.bronze_loans",
    keys=["loan_account_id"],
    sequence_by=col("disbursement_date"),
    stored_as_scd_type=2,
    except_column_list=["_rescued_data"]
)

# COMMAND ----------
# MAGIC %md ### silver_transactions (append-only — MVM: payment.payment_transaction)

# COMMAND ----------
@dp.expect_or_drop("valid_txn_id",       "payment_transaction_id IS NOT NULL")
@dp.expect_or_drop("positive_amount",    "payment_amount > 0")
@dp.expect("valid_currency",             "currency_id IN ('MYR','USD','SGD','EUR','GBP')")
@dp.expect("valid_status",               "instruction_status IN ('settled','pending','rejected','cancelled','reversed')")
@dp.expect("valid_rail",                 "payment_rail_type IN ('FPX','IBG','DuitNow','SWIFT','ATM','branch','card','internal')")
@dp.expect("valid_sanctions",            "sanctions_screening_status IN ('CLEARED','FLAGGED','PENDING','BLOCKED')")
@dp.expect("posting_after_value",        "posting_date >= value_date")
@dp.expect("no_rescued_data",            "_rescued_data IS NULL")
@dp.table(name="silver_transactions",
          comment="Banking MVM: payment.payment_transaction. Append-only.",
          table_properties={"quality": "silver"})
def silver_transactions():
    return (spark.readStream.table(f"{FULL}.bronze_core_banking_txn")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md ### silver_card_transactions (append-only — MVM: payment.instruction card-type)

# COMMAND ----------
@dp.expect_or_drop("valid_card_txn_id",   "card_instruction_id IS NOT NULL")
@dp.expect_or_drop("positive_card_amount","txn_amount > 0")
@dp.expect("valid_card_status",           "instruction_status IN ('settled','pending','disputed','reversed')")
@dp.expect("no_rescued_data",             "_rescued_data IS NULL")
@dp.table(name="silver_card_transactions",
          comment="Banking MVM: payment.instruction (card). Append-only.",
          table_properties={"quality": "silver"})
def silver_card_transactions():
    return (spark.readStream.table(f"{FULL}.bronze_cards_txn")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md ### silver_kyc_compliance (MVM: compliance.kyc_review + CTOS/CCRIS extension)

# COMMAND ----------
@dp.expect_or_drop("valid_cif_kyc",              "cif_number IS NOT NULL")
@dp.expect("valid_ctos_score",                    "ctos_score IS NULL OR (ctos_score >= 300 AND ctos_score <= 850)")
@dp.expect("valid_payment_conduct",               "payment_conduct_12m IN ('clean','1_missed','2_missed','3+_missed')")
@dp.expect("no_negative_outstanding",             "total_outstanding_balance_myr IS NULL OR total_outstanding_balance_myr >= 0")
@dp.expect("valid_bankruptcy",                    "bankruptcy_status IN ('none','voluntary','involuntary')")
@dp.expect("no_rescued_data",                     "_rescued_data IS NULL")
@dp.table(name="silver_kyc_compliance",
          comment="Banking MVM: compliance.kyc_review + CTOS/CCRIS. Append-only.",
          table_properties={"quality": "silver"})
def silver_kyc_compliance():
    return (spark.readStream.table(f"{FULL}.bronze_credit_bureau")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md ### silver_digital_activity (MVM: channel domain — aggregated from telco + digital events)

# COMMAND ----------
@dp.expect_or_drop("valid_party_digital",  "party_id IS NOT NULL")
@dp.expect("valid_event_type",             "event_type IN ('login','view_product','apply','fund_transfer','bill_pay','investment','logout') OR event_type IS NULL")
@dp.expect("no_rescued_data",              "_rescued_data IS NULL")
@dp.table(name="silver_digital_activity",
          comment="Banking MVM: channel domain — digital + telco events. Append-only.",
          table_properties={"quality": "silver"})
def silver_digital_activity():
    digital = (spark.readStream.table(f"{FULL}.bronze_digital_events")
               .withColumn("source", lit("digital_banking"))
               .withColumn("_silver_timestamp", current_timestamp()))
    telco   = (spark.readStream.table(f"{FULL}.bronze_telco_events")
               .withColumn("event_type", lit(None).cast("string"))
               .withColumn("source", lit("telco"))
               .withColumn("_silver_timestamp", current_timestamp()))
    return digital.unionByName(telco, allowMissingColumns=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## How to Deploy This Pipeline
# MAGIC
# MAGIC 1. Go to Databricks → Pipelines → Create Pipeline
# MAGIC 2. Pipeline name: `ABMB-Bronze-Silver`
# MAGIC 3. Source: select this notebook (`07_sdp_bronze_silver.py`)
# MAGIC 4. Target catalog: `fevm_master_classic_marcus_catalog`
# MAGIC 5. Target schema: `abmb_rfp_presentation`
# MAGIC 6. Cluster: Serverless
# MAGIC 7. Click Start — the pipeline creates all 7 silver tables automatically
