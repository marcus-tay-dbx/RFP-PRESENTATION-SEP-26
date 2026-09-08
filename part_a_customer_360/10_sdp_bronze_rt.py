# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 1: RT Bronze Ingestion (Continuous)
# MAGIC
# MAGIC **Pipeline mode:** Continuous — always-on, scales to near-zero when idle.
# MAGIC
# MAGIC | Bronze Table | Source | Latency | Story |
# MAGIC |---|---|---|---|
# MAGIC | `bronze_digital_events` | Mobile app / web events | **Real-time** (~10s) | Customer logins, product views, transfers |
# MAGIC | `bronze_credit_bureau` | CTOS / CCRIS API push | **Near-RT micro-batch** (~60s) | Credit score updates, CCRIS status changes |
# MAGIC | `bronze_telco_events` | Celcom / Maxis / Digi signals | **Near-RT micro-batch** (~45s) | SIM activity, data usage, roaming flags |
# MAGIC
# MAGIC All three are AutoLoader streaming tables — DLT picks up new JSON files the moment
# MAGIC they land in the UC Volume. The event generator job (`DBX-Events-Generator`) writes
# MAGIC new files continuously so the record counter ticks up live during the demo.

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ── Schema hints — prevents AutoLoader schema inference on every new file ──────
# These match exactly what the event generator produces.

DIGITAL_EVENTS_HINTS = (
    "event_id STRING, party_id STRING, cif_number STRING, session_id STRING, "
    "event_timestamp TIMESTAMP, event_date DATE, event_type STRING, "
    "product_category_viewed STRING, channel STRING, device_type STRING, "
    "page_url STRING, session_duration_sec INT, is_authenticated BOOLEAN, "
    "source_system STRING"
)

CREDIT_BUREAU_HINTS = (
    "cif_number STRING, party_id STRING, report_date DATE, ctos_score INT, "
    "ccris_status STRING, total_credit_facilities INT, "
    "total_outstanding_balance_myr DOUBLE, monthly_total_commitment_myr DOUBLE, "
    "payment_conduct_12m STRING, legal_cases_count INT, bankruptcy_status STRING, "
    "credit_card_count INT, home_loan_count INT, personal_loan_count INT, "
    "hire_purchase_count INT, overdraft_count INT, "
    "inquiry_count_last_6m INT, inquiry_count_last_12m INT, "
    "oldest_facility_years INT, credit_utilisation_pct DOUBLE, "
    "debt_to_income_ratio DOUBLE, largest_single_facility_myr DOUBLE, "
    "secured_debt_pct DOUBLE, unsecured_debt_pct DOUBLE, "
    "bureau_pull_timestamp TIMESTAMP, bureau_provider STRING, "
    "consent_flag BOOLEAN, consent_date DATE, "
    "aml_flag BOOLEAN, pep_flag BOOLEAN, sanction_flag BOOLEAN, "
    "data_source STRING, _rescued_data STRING"
)

TELCO_EVENTS_HINTS = (
    "event_id STRING, cif_number STRING, party_id STRING, event_date DATE, "
    "telco_carrier STRING, data_plan STRING, "
    "data_consumption_mb_monthly DOUBLE, voice_minutes_monthly INT, "
    "sms_count_monthly INT, arpu_myr DOUBLE, device_class STRING, "
    "device_os STRING, digital_maturity_score DOUBLE, "
    "roaming_flag BOOLEAN, postpaid_flag BOOLEAN, "
    "tenure_months_with_carrier INT, churn_risk_score DOUBLE, "
    "financial_inclusion_score DOUBLE, app_usage_banking_mins DOUBLE, "
    "location_cluster STRING, consent_flag BOOLEAN, "
    "data_source STRING, _rescued_data STRING"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔴 Real-Time: `bronze_digital_events`
# MAGIC **AutoLoader picks up 1 file per micro-batch → sub-10-second latency**
# MAGIC
# MAGIC Event types: `login` · `view_product` · `apply` · `fund_transfer` · `bill_pay` · `investment` · `logout`

# COMMAND ----------
@dp.table(
    name="bronze_digital_events",
    comment="Real-time customer digital interactions — mobile app, internet banking, USSD. "
            "AutoLoader continuous: 1 file per trigger → ~10s end-to-end latency.",
    table_properties={
        "quality":                          "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)
def bronze_digital_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",             "json")
            .option("cloudFiles.schemaHints",         DIGITAL_EVENTS_HINTS)
            .option("cloudFiles.maxFilesPerTrigger",  1)      # 1 file = real-time feel
            .option("cloudFiles.includeExistingFiles","true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{VOLUME_DATA}/streaming/digital_events/")
        .withColumn("_source_file",       input_file_name())
        .withColumn("_ingest_timestamp",  current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🟡 Near-RT Micro-Batch: `bronze_credit_bureau`
# MAGIC **CTOS/CCRIS pushes score updates every ~60 seconds — processed in micro-batches of ≤5 files**
# MAGIC
# MAGIC Fields: CTOS score · CCRIS status · payment conduct · debt-to-income ratio · bankruptcy flag

# COMMAND ----------
@dp.table(
    name="bronze_credit_bureau",
    comment="Near-RT CTOS/CCRIS credit bureau score updates. "
            "AutoLoader continuous: up to 5 files per micro-batch → ~60s pulse rate.",
    table_properties={
        "quality":                          "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)
def bronze_credit_bureau():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",             "json")
            .option("cloudFiles.schemaHints",         CREDIT_BUREAU_HINTS)
            .option("cloudFiles.maxFilesPerTrigger",  5)      # micro-batch: ≤5 files
            .option("cloudFiles.includeExistingFiles","true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{VOLUME_DATA}/streaming/credit_bureau/")
        .withColumn("_source_file",       input_file_name())
        .withColumn("_ingest_timestamp",  current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🟡 Near-RT Micro-Batch: `bronze_telco_events`
# MAGIC **Celcom/Maxis/Digi push SIM & data signals every ~45 seconds — processed in micro-batches of ≤5 files**
# MAGIC
# MAGIC Fields: ARPU · data consumption · roaming flag · digital maturity score · churn risk

# COMMAND ----------
@dp.table(
    name="bronze_telco_events",
    comment="Near-RT telco operator signals — SIM activity, data usage, roaming. "
            "AutoLoader continuous: up to 5 files per micro-batch → ~45s pulse rate.",
    table_properties={
        "quality":                          "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)
def bronze_telco_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",             "json")
            .option("cloudFiles.schemaHints",         TELCO_EVENTS_HINTS)
            .option("cloudFiles.maxFilesPerTrigger",  5)      # micro-batch: ≤5 files
            .option("cloudFiles.includeExistingFiles","true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{VOLUME_DATA}/streaming/telco_events/")
        .withColumn("_source_file",       input_file_name())
        .withColumn("_ingest_timestamp",  current_timestamp())
    )
