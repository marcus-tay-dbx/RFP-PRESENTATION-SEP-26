# Databricks notebook source
# COMMAND ----------
CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "rfp_presentation"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_DATA = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOLUME_PDFS = f"/Volumes/{CATALOG}/{SCHEMA}/product_pdfs"
GLM_MODEL   = "system.ai.databricks-glm-5-2"
LAKEBASE_PROJECT = "DBX-RFP-PRESENTATION"
N_CUSTOMERS = 1000
BASE_DATE   = "2026-09-07"
