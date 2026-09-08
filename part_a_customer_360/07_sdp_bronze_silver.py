# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 2 — Customer 360: Bronze (Batch) → Silver → Gold
# MAGIC
# MAGIC **Pipeline mode:** Triggered — run on-demand or scheduled. Processes all
# MAGIC accumulated data since the last run, then stops.
# MAGIC
# MAGIC ## DAG overview
# MAGIC
# MAGIC ```
# MAGIC AutoLoader(CSV) ──▶ bronze_customer_master   ──▶ silver_customers          ──┐
# MAGIC AutoLoader(CSV) ──▶ bronze_core_banking_accs ──▶ silver_deposit_accounts   ──┤
# MAGIC AutoLoader(CSV) ──▶ bronze_core_banking_txn  ──▶ silver_transactions        ──┤──▶ gold_customer_360
# MAGIC AutoLoader(CSV) ──▶ bronze_loans             ──▶ silver_loan_accounts       ──┤     (in 08_sdp_silver_gold)
# MAGIC AutoLoader(CSV) ──▶ bronze_cards_txn         ──▶ silver_card_transactions   ──┤
# MAGIC [RT Pipeline 1] ──▶ bronze_credit_bureau     ──▶ silver_kyc_compliance     ──┤
# MAGIC [RT Pipeline 1] ──▶ bronze_digital_events    ──▶ silver_digital_activity   ──┤
# MAGIC [RT Pipeline 1] ──▶ bronze_telco_events      ──▶  (feeds digital_activity) ──┘
# MAGIC ```
# MAGIC
# MAGIC Batch bronze tables are defined inline (AutoLoader from `batch_v1/`).
# MAGIC RT bronze tables are owned by Pipeline 1 (`DBX-RT-Bronze`) and referenced
# MAGIC here by their full UC path — DLT reads only new records since the last run.
# MAGIC
# MAGIC ## Banking MVM domains
# MAGIC | Silver Table | MVM Domain | MVM Entity |
# MAGIC |---|---|---|
# MAGIC | silver_customers | customer | party + individual_profile |
# MAGIC | silver_deposit_accounts | account | deposit_account |
# MAGIC | silver_loan_accounts | loan | loan_account |
# MAGIC | silver_transactions | payment | payment_transaction |
# MAGIC | silver_card_transactions | payment | payment_instruction (card) |
# MAGIC | silver_kyc_compliance | compliance | kyc_review + CTOS/CCRIS |
# MAGIC | silver_digital_activity | channel | digital_channel events |

# COMMAND ----------
# DLT pipelines do not support %run — inline constants instead of shared/config
CATALOG     = "fevm_master_classic_marcus_catalog"
SCHEMA      = "rfp_presentation"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_DATA = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

FULL = FULL_SCHEMA

# ══════════════════════════════════════════════════════════════════════════════
# PART 1 — BATCH BRONZE SOURCES (AutoLoader, triggered)
# Owned by this pipeline. AutoLoader checkpoints ensure only new files are
# processed on each pipeline run (availableNow semantics in triggered mode).
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md ### bronze_customer_master — Core Banking CIF (CSV, batch)

# COMMAND ----------
@dp.table(
    name="bronze_customer_master",
    comment="Raw CIF records from Core Banking system. AutoLoader batch ingest from batch_v1/customer_master/.",
    table_properties={"quality": "bronze"}
)
def bronze_customer_master():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "csv")
            .option("cloudFiles.inferColumnTypes",    "true")
            .option("header",                          "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles","true")
            .option("rescuedDataColumn",              "_rescued_data")
            .load(f"{VOLUME_DATA}/batch_v1/customer_master/")
        .withColumn("_source_file",      input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md ### bronze_core_banking_accounts — Deposit Accounts (CSV, batch)

# COMMAND ----------
@dp.table(
    name="bronze_core_banking_accounts",
    comment="Raw deposit account records from Core Banking System (CBS). AutoLoader batch.",
    table_properties={"quality": "bronze"}
)
def bronze_core_banking_accounts():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "csv")
            .option("cloudFiles.inferColumnTypes",    "true")
            .option("header",                          "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles","true")
            .option("rescuedDataColumn",              "_rescued_data")
            .load(f"{VOLUME_DATA}/batch_v1/accounts/")
        .withColumn("_source_file",      input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md ### bronze_core_banking_txn — Transactions (CSV, batch)

# COMMAND ----------
@dp.table(
    name="bronze_core_banking_txn",
    comment="Raw payment transactions from Transaction Engine. AutoLoader batch.",
    table_properties={"quality": "bronze"}
)
def bronze_core_banking_txn():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "csv")
            .option("cloudFiles.inferColumnTypes",    "true")
            .option("header",                          "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles","true")
            .option("rescuedDataColumn",              "_rescued_data")
            .load(f"{VOLUME_DATA}/batch_v1/transactions/")
        .withColumn("_source_file",      input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md ### bronze_loans — Loan Facilities (CSV, batch)

# COMMAND ----------
@dp.table(
    name="bronze_loans",
    comment="Raw loan facility records from Loan Origination System. AutoLoader batch.",
    table_properties={"quality": "bronze"}
)
def bronze_loans():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "csv")
            .option("cloudFiles.inferColumnTypes",    "true")
            .option("header",                          "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles","true")
            .option("rescuedDataColumn",              "_rescued_data")
            .load(f"{VOLUME_DATA}/batch_v1/loans/")
        .withColumn("_source_file",      input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md ### bronze_cards_txn — Card Transactions (CSV, batch)

# COMMAND ----------
@dp.table(
    name="bronze_cards_txn",
    comment="Raw card transactions from Card Engine. AutoLoader batch.",
    table_properties={"quality": "bronze"}
)
def bronze_cards_txn():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "csv")
            .option("cloudFiles.inferColumnTypes",    "true")
            .option("header",                          "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles","true")
            .option("rescuedDataColumn",              "_rescued_data")
            .load(f"{VOLUME_DATA}/batch_v1/cards/")
        .withColumn("_source_file",      input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
    )

# ══════════════════════════════════════════════════════════════════════════════
# PART 2 — SILVER TABLES (SCD Type 2 via apply_changes + append-only)
# Reads from bronze tables defined above (batch, in this pipeline) and from
# RT bronze tables owned by Pipeline 1 (cross-pipeline UC table reads).
# DLT checkpoints ensure only new bronze records are processed each run.
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ### silver_customers (SCD Type 2 — MVM: customer.party + individual_profile)
# MAGIC
# MAGIC `apply_changes` manages full SCD Type 2 history: `cif_number` is the business key,
# MAGIC `record_updated_timestamp` sequences changes. Every version is preserved.

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
# MAGIC %md
# MAGIC ### silver_kyc_compliance (MVM: compliance.kyc_review + CTOS/CCRIS)
# MAGIC **Source: `bronze_credit_bureau` — owned by Pipeline 1 (DBX-RT-Bronze)**
# MAGIC Cross-pipeline read via full UC table path. DLT processes only new records
# MAGIC written since the last Pipeline 2 trigger.

# COMMAND ----------
@dp.expect_or_drop("valid_cif_kyc",              "cif_number IS NOT NULL")
@dp.expect("valid_ctos_score",                    "ctos_score IS NULL OR (ctos_score >= 300 AND ctos_score <= 850)")
@dp.expect("valid_payment_conduct",               "payment_conduct_12m IN ('clean','1_missed','2_missed','3+_missed')")
@dp.expect("no_negative_outstanding",             "total_outstanding_balance_myr IS NULL OR total_outstanding_balance_myr >= 0")
@dp.expect("valid_bankruptcy",                    "bankruptcy_status IN ('none','voluntary','involuntary')")
@dp.table(name="silver_kyc_compliance",
          comment="Banking MVM: compliance.kyc_review + CTOS/CCRIS. "
                  "Source: bronze_credit_bureau (Pipeline 1 — RT Bronze). Append-only.",
          table_properties={"quality": "silver"})
def silver_kyc_compliance():
    # Cross-pipeline read: bronze_credit_bureau is owned by DBX-RT-Bronze (Pipeline 1).
    # spark.readStream.table() reads from the UC-registered streaming table directly.
    return (spark.readStream.table(f"{FULL}.bronze_credit_bureau")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md
# MAGIC ### silver_digital_activity (MVM: channel domain — digital events + telco signals)
# MAGIC **Sources: `bronze_digital_events` + `bronze_telco_events` — both owned by Pipeline 1**
# MAGIC Union of digital app events (real-time) and telco partner signals (near-RT).

# COMMAND ----------
@dp.expect_or_drop("valid_party_digital",  "party_id IS NOT NULL")
@dp.expect("valid_event_type",             "event_type IN ('login','view_product','apply','fund_transfer','bill_pay','investment','logout') OR event_type IS NULL")
@dp.table(name="silver_digital_activity",
          comment="Banking MVM: channel domain — digital events + telco signals. "
                  "Sources: bronze_digital_events + bronze_telco_events (Pipeline 1). Append-only.",
          table_properties={"quality": "silver"})
def silver_digital_activity():
    # Cross-pipeline reads from Pipeline 1 (DBX-RT-Bronze)
    digital = (spark.readStream.table(f"{FULL}.bronze_digital_events")
               .withColumn("source", lit("digital_banking"))
               .withColumn("_silver_timestamp", current_timestamp()))

    telco   = (spark.readStream.table(f"{FULL}.bronze_telco_events")
               .withColumn("event_type", lit(None).cast("string"))
               .withColumn("source", lit("telco"))
               .withColumn("_silver_timestamp", current_timestamp()))

    return digital.unionByName(telco, allowMissingColumns=True)
