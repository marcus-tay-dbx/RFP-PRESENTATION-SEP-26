# Databricks notebook source
# COMMAND ----------
# ── PARTNER SETUP — change these two lines to match your workspace ─────────────
CATALOG = "fevm_master_classic_marcus_catalog"   # ← partners: change to your UC catalog name
SCHEMA  = "rfp_presentation"   # ← schema name (created by 00_setup.py — can leave as-is)
# ──────────────────────────────────────────────────────────────────────────────

FULL_SCHEMA  = f"{CATALOG}.{SCHEMA}"
VOLUME_DATA  = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOLUME_PDFS  = f"/Volumes/{CATALOG}/{SCHEMA}/product_pdfs"
GLM_MODEL    = "system.ai.databricks-glm-5-2"
LAKEBASE_PROJECT = "DBX-RFP-PRESENTATION"
N_CUSTOMERS  = 1000
BASE_DATE    = "2026-09-07"
