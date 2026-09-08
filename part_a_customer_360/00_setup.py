# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # DBX RFP Demo — Setup
# MAGIC
# MAGIC Creates the Unity Catalog schema, volumes, and directory structure for Part A.
# MAGIC
# MAGIC ## Banking Minimum Viable Model (MVM) Overview
# MAGIC
# MAGIC The **Databricks Banking MVM** is a pre-built industry data model with:
# MAGIC - **17 domains** (customer, account, loan, payment, compliance, channel, product, risk, …)
# MAGIC - **227 tables** with standardised field names and business keys
# MAGIC - Designed to accelerate Lakehouse adoption in FSI — no schema design from scratch
# MAGIC
# MAGIC In this demo we populate **6 of the 17 domains**:
# MAGIC
# MAGIC | Domain | Tables Used | Source System |
# MAGIC |---|---|---|
# MAGIC | customer | party, individual_profile | Core Banking (CIF) |
# MAGIC | account | deposit_account | Core Banking (CBS) |
# MAGIC | loan | loan_account | Loan Origination System |
# MAGIC | payment | payment_transaction, payment_instruction | Transaction Engine |
# MAGIC | compliance | kyc_review | CTOS / CCRIS credit bureau |
# MAGIC | channel | digital_channel | Mobile app + Telco partner |

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FULL_SCHEMA}.raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FULL_SCHEMA}.product_pdfs")
print(f"✅ Schema and volumes created: {FULL_SCHEMA}")

# COMMAND ----------
# Create subdirectory structure inside the raw_data volume
for subdir in ["batch_v1/customer_master", "batch_v1/accounts", "batch_v1/loans",
               "batch_v1/transactions", "batch_v1/cards",
               "batch_v2/customer_master",
               "nearrt/credit_bureau", "nearrt/telco_events",
               "streaming/digital_events", "streaming/credit_bureau", "streaming/telco_events",
               "schema_evolution", "rescued_demo",
               "_schemas"]:
    dbutils.fs.mkdirs(f"{VOLUME_DATA}/{subdir}")
    print(f"  ✅ {VOLUME_DATA}/{subdir}")

# COMMAND ----------
# Validate
assert spark.catalog.databaseExists(FULL_SCHEMA), f"Schema {FULL_SCHEMA} not found!"
print(f"✅ Validation passed — schema exists: {FULL_SCHEMA}")
