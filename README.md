# DBX Bank — Customer 360 + Hyperpersonalization Demo

A Databricks end-to-end demo for a retail banking RFP. Part A builds a Customer 360 data lakehouse on the Databricks Banking Minimum Viable Model (MVM); Part B trains an XGBoost product-recommendation model and serves it through a real-time endpoint and a Databricks App.

---

## Architecture Overview

```
Part A — Customer 360 (Data Lakehouse)
  Synthetic data gen  →  AutoLoader bronze ingest  →  DLT pipeline (Bronze→Silver SCD2→Gold)
  └─ gold_customer_360: unified view across 7 Banking MVM-conformed silver tables

Part B — Hyperpersonalization (ML/AI)
  gold_customer_360  →  Feature Store  →  XGBoost training  →  Model Serving endpoint
                                       →  Batch scoring  →  gold_product_recommendations
                                                         →  Databricks App (React UI)
```

**Part A** demonstrates the Databricks Lakehouse pattern for FSI: AutoLoader for batch and near-real-time ingestion, Spark Declarative Pipelines (DLT) with SCD Type 2 for governed silver tables, and a denormalised gold table as the single customer record.

**Part B** demonstrates the full ML lifecycle on top of that Customer 360: Databricks Feature Engineering, MLflow experiment tracking, Unity Catalog model registry with aliases (`@dev` → `@champion`), Model Serving with AI Gateway inference logging, and a React front-end served as a Databricks App.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Databricks workspace | Serverless compute enabled |
| Databricks CLI v0.200+ | `brew install databricks` or `pip install databricks-cli` |
| Bundle auth | OAuth or PAT configured for the target workspace |
| Unity Catalog | A catalog you own (default: `fevm_master_classic_marcus_catalog`) |

Authenticate the CLI before running any bundle commands:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

---

## Project Layout

```
ABMB_RFP/
├── databricks.yml                   # Bundle definition — catalog/schema variables, sync rules
├── shared/
│   └── config.py                    # Central constants: CATALOG, SCHEMA, volume paths, model name
├── resources/
│   ├── setup_ingest.job.yml         # Part A job definition (setup → ingest → DLT pipeline)
│   ├── part_b_training.job.yml      # Part B job definition (feature eng → train → deploy → score)
│   └── pipeline.yml                 # DLT pipeline definition (Bronze→Silver→Gold)
├── part_a_customer_360/
│   ├── 00_setup.py                  # Create UC schema, volumes, directory structure
│   ├── 01_data_generator.py         # Generate 1,000 synthetic Malaysian banking customers
│   ├── 02_ingest_batch.py           # AutoLoader batch ingest — 5 CSV sources → bronze tables
│   ├── 03_ingest_nearrt.py          # AutoLoader near-RT ingest — credit bureau + telco JSON
│   ├── 04_ingest_realtime.py        # Realtime ingest demonstration
│   ├── 05_schema_evolution_demo.py  # Schema evolution with AutoLoader rescuedDataColumn
│   ├── 06_rescued_data_demo.py      # Rescued data column demo
│   ├── 07_sdp_bronze_silver.py      # DLT: Bronze → 7 Silver tables (SCD Type 2)
│   ├── 08_sdp_silver_gold.py        # DLT: Silver → gold_customer_360
│   ├── 09_pdf_pipeline.py           # Unstructured PDF ingestion pipeline
│   └── data/                        # Local sample data files
└── part_b_hyperpersonalization/
    ├── 00_setup.py                  # Part B setup — Lakebase project, feature store init
    ├── 01_overview.py               # Demo narrative and architecture walkthrough
    ├── 02_eda.py                    # Exploratory data analysis on gold_customer_360
    ├── 03_feature_engineering.py    # Build customer_features table; sync to Lakebase
    ├── 04_model_training.py         # XGBoost multi-class training, MLflow logging, @dev alias
    ├── 05_batch_inference.py        # Score all customers → gold_product_recommendations
    ├── 06_realtime_inference.py     # Promote @dev → @champion; deploy Model Serving endpoint
    ├── 07_observability.py          # Inference logging, drift monitoring, AI Gateway metrics
    ├── 08_mlflow_deploy.py          # MLflow deployment utilities
    ├── 09_unity_ai_gateway.py       # AI Gateway configuration and rate limiting
    └── app/                         # Databricks App (FastAPI backend + React frontend)
        ├── app.py                   # FastAPI routes — calls serving endpoint, reads recommendations
        ├── app.yaml                 # Databricks App manifest
        ├── requirements.txt         # Python dependencies
        └── frontend/                # React UI (pre-built dist/ included for deployment)
```

---

## Part A — Customer 360 Pipeline

### What It Does

1. **Setup** — creates the Unity Catalog schema, two UC Volumes (`raw_data`, `product_pdfs`), and the directory tree that AutoLoader monitors.
2. **Data Generation** — generates 1,000 synthetic Malaysian retail banking customers with realistic profiles: CIF records, deposit/loan accounts, transaction history, credit bureau scores (CTOS/CCRIS), and mobile app events.
3. **Bronze Ingest** — AutoLoader ingests five CSV source feeds (customer master, accounts, loans, transactions, cards) into append-only bronze Delta tables. A parallel near-RT job ingests credit bureau and telco JSON.
4. **Bronze → Silver (DLT)** — a Spark Declarative Pipeline (DLT) applies SCD Type 2 CDC via `apply_changes` to produce seven Banking MVM-conformed silver tables.
5. **Silver → Gold (DLT)** — the same DLT pipeline joins and aggregates the silver tables into `gold_customer_360`, the unified customer record used by Part B.

### Notebooks

| Notebook | What It Does |
|---|---|
| `00_setup.py` | Create UC schema (`abmb_rfp_presentation`), volumes (`raw_data`, `product_pdfs`), and volume subdirectories |
| `01_data_generator.py` | Generate 1,000 synthetic customer records across all source-system feeds and write CSVs/JSON to the raw_data volume |
| `02_ingest_batch.py` | AutoLoader batch ingest of 5 CSV feeds into bronze Delta tables with `rescuedDataColumn` |
| `03_ingest_nearrt.py` | AutoLoader near-real-time ingest of credit bureau (CTOS/CCRIS) and telco event JSON using `trigger(availableNow=True)` |
| `04_ingest_realtime.py` | Continuous streaming ingest demonstration |
| `05_schema_evolution_demo.py` | Add a new column to the v2 customer feed; show AutoLoader schema evolution with `mergeSchema` |
| `06_rescued_data_demo.py` | Inject a malformed record; show `_rescued_data` column capturing it without pipeline failure |
| `07_sdp_bronze_silver.py` | DLT notebook: seven `create_streaming_table` + `create_auto_cdc_flow` definitions for SCD Type 2 silver tables |
| `08_sdp_silver_gold.py` | DLT notebook: join silver tables → materialised `gold_customer_360` view with 40+ features |
| `09_pdf_pipeline.py` | Ingest product PDF brochures into a vector index for AI-powered product Q&A |

### How to Run

Deploy the bundle and run the end-to-end Part A job with one command:

```bash
# Deploy notebooks and resources to the workspace
databricks bundle deploy

# Run Part A: setup → data gen → batch ingest + near-RT ingest → DLT pipeline
databricks bundle run abmb_setup_and_ingest
```

This single job runs the full sequence: `setup` → `generate_data` → (`ingest_batch` ∥ `ingest_nearrt`) → `run_pipeline` (DLT).

### Data Model

The pipeline populates **6 of the 17 Banking MVM domains**:

| Silver Table | MVM Domain | Source System |
|---|---|---|
| `silver_customers` | customer | Core Banking (CIF) — party + individual_profile |
| `silver_deposit_accounts` | account | Core Banking (CBS) — deposit_account |
| `silver_loan_accounts` | loan | Loan Origination System — loan_account |
| `silver_transactions` | payment | Transaction Engine — payment_transaction |
| `silver_card_transactions` | payment | Transaction Engine — payment_instruction (card) |
| `silver_kyc_compliance` | compliance | CTOS / CCRIS credit bureau — kyc_review |
| `silver_digital_activity` | channel | Mobile app + telco — digital_channel events |

All seven tables use **SCD Type 2** — full history is preserved automatically by DLT's `apply_changes`. The gold layer joins them into `gold_customer_360`.

---

## Part B — Hyperpersonalization Pipeline

### What It Does

1. **Feature Engineering** — reads `gold_customer_360`, derives label assignments (next-best product per customer), builds a `customer_features` Delta table, and registers it with Databricks Feature Engineering. Features are synced to a Lakebase Postgres instance for sub-10ms lookup at serve time.
2. **Model Training** — trains an XGBoost multi-class classifier (`multi:softprob`) to predict `next_best_product` across 6 product classes. Logs parameters, metrics, SHAP feature importances, and a confusion matrix to MLflow. Registers the model to Unity Catalog and sets the `@dev` alias.
3. **Endpoint Deployment** — promotes `@dev` to `@champion`, then creates a Model Serving endpoint backed by Lakebase for real-time feature lookup. AI Gateway inference logging is enabled.
4. **Batch Scoring** — scores all 1,000 customers using the `@champion` model and writes ranked product recommendations to `gold_product_recommendations`.
5. **Databricks App** — a FastAPI + React application reads recommendations and calls the serving endpoint live, giving a demo-ready UI for the hyperpersonalization story.

### Notebooks

| Notebook | What It Does |
|---|---|
| `00_setup.py` | Create Lakebase project (`ABMB-RFP-PRESENTATION`), initialise feature store schema |
| `01_overview.py` | Architecture walkthrough and demo narrative markdown |
| `02_eda.py` | Exploratory analysis: product ownership rates, income distribution, CTOS score bands |
| `03_feature_engineering.py` | Derive label column; build `customer_features` feature table; sync to Lakebase |
| `04_model_training.py` | XGBoost `multi:softprob` training; MLflow logging; register `@dev` to UC model registry |
| `05_batch_inference.py` | Batch score all customers using `@champion`; write `gold_product_recommendations` |
| `06_realtime_inference.py` | Promote `@dev` → `@champion`; deploy Model Serving endpoint with AI Gateway |
| `07_observability.py` | Query inference logs; compute drift metrics; display AI Gateway request volume |
| `08_mlflow_deploy.py` | MLflow deployment helpers and model validation utilities |
| `09_unity_ai_gateway.py` | Configure AI Gateway rate limits and usage policies on the serving endpoint |
| `app/` | Databricks App: FastAPI backend + React frontend for live demo |

### How to Run

```bash
# Run Part B: feature engineering → model training → endpoint deploy → batch inference
databricks bundle run abmb_part_b_training
```

Job task sequence: `feature_engineering` → `model_training` → `realtime_endpoint` → `batch_inference`.

> **Note:** Part A must complete successfully before running Part B — the feature engineering notebook reads from `gold_customer_360`.

### ML Model Details

| Property | Value |
|---|---|
| Algorithm | XGBoost (`multi:softprob`) |
| Target | `next_best_product` — 6 classes |
| Classes | `CREDIT_CARD`, `HOME_LOAN`, `PERSONAL_LOAN`, `UNIT_TRUST`, `FIXED_DEPOSIT`, `INSURANCE` |
| Primary metric | Macro F1-score |
| Explainability | SHAP feature importance values logged to MLflow |
| Feature source | `customer_features` Delta table (Feature Engineering client) |
| Real-time lookup | Lakebase Postgres (synced from `customer_features`) |
| Model registry | Unity Catalog — aliases `@dev`, `@champion` |

---

## Databricks Assets

All assets land in the configured catalog and schema (defaults below).

| Asset | Full Name | Type |
|---|---|---|
| UC schema | `fevm_master_classic_marcus_catalog.abmb_rfp_presentation` | Schema |
| Raw data volume | `.../abmb_rfp_presentation.raw_data` | UC Volume |
| Product PDFs volume | `.../abmb_rfp_presentation.product_pdfs` | UC Volume |
| Silver tables (×7) | `silver_customers`, `silver_deposit_accounts`, `silver_loan_accounts`, `silver_transactions`, `silver_card_transactions`, `silver_kyc_compliance`, `silver_digital_activity` | Delta (SCD2) |
| Gold Customer 360 | `gold_customer_360` | Delta (materialised) |
| Feature table | `customer_features` | Delta + Feature Store |
| Gold recommendations | `gold_product_recommendations` | Delta |
| ML model | `abmb_recommendation_model` | UC Registered Model |
| DLT pipeline | `ABMB-Customer-360-Pipeline` | Serverless DLT pipeline |
| Serving endpoint | `abmb-product-recommendation-<username>` | Model Serving |
| Lakebase project | `ABMB-RFP-PRESENTATION` | Lakebase Postgres |
| Databricks App | `app/` | Databricks App (FastAPI + React) |

---

## Bundle Commands

```bash
# Validate the bundle locally (no workspace changes)
databricks bundle validate

# Deploy notebooks and resource definitions to the workspace
databricks bundle deploy

# Run Part A end-to-end (setup → ingest → DLT pipeline)
databricks bundle run abmb_setup_and_ingest

# Run Part B end-to-end (feature eng → train → endpoint → batch score)
databricks bundle run abmb_part_b_training

# Check job run status
databricks bundle run --no-wait abmb_setup_and_ingest
databricks jobs list-runs --job-name ABMB-Part-A-Setup-and-Ingest

# Destroy all deployed resources (use with caution)
databricks bundle destroy
```

---

## Configuration

The catalog and schema are bundle variables defined in `databricks.yml`. Override them at deploy/run time without editing any files:

```bash
# Deploy to a different catalog or schema
databricks bundle deploy -v catalog=my_catalog -v schema=my_demo_schema

# Run Part A against the overridden schema
databricks bundle run abmb_setup_and_ingest -v catalog=my_catalog -v schema=my_demo_schema
```

To change the defaults permanently, edit the `variables` block in `databricks.yml`:

```yaml
variables:
  catalog:
    default: your_catalog_name
  schema:
    default: your_schema_name
```

The shared constants in `shared/config.py` derive all paths from `CATALOG` and `SCHEMA` — no other files need updating.
