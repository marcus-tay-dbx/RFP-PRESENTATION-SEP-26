# ABMB RFP Demo — Full Design Spec
**Date:** 2026-09-07  
**Author:** Marcus Tay  
**Status:** Ready for implementation

---

## 1. Overview & Goals

An end-to-end Customer 360 + Hyperpersonalization demo for Alliance Bank Malaysia Berhad (ABMB) presented as part of a two-vendor RFP. FPT presents Part A in the morning; SWO presents Part B in the afternoon. Both codebases are shared with both vendors, but each vendor focuses on their section.

### Success Criteria
- Part A stands alone: any partner can run it top-to-bottom with zero external dependencies
- Part B depends only on the gold table produced by Part A; it also runs top-to-bottom standalone after Part A
- Every demo step is repeatable (reset cells exist in schema evolution and rescued-data notebooks)
- All simulated data, models, app, and AI gateway are hosted within the single Databricks workspace
- The final Databricks App shows Alliance Bank branding and supports live product recommendations + AI-drafted emails

### RFP Scenarios Addressed
- **Scenario 1 (Onboarding):** New data source registered, mapped to Enterprise Data Model, governed via Unity Catalog, published as certified data product — demonstrated via schema evolution notebook + AutoLoader registration pattern
- **Scenario 2 (EOD Batch):** Core banking EOD files ingested, DQ validated, enriched via Banking MVM, certified gold table published — demonstrated via SDP pipeline with data quality expectations

---

## 2. Workspace Configuration

| Parameter | Value |
|---|---|
| Profile | `fevm-master-classic-marcus` |
| Workspace URL | `https://fevm-fevm-master-classic-marcus.cloud.databricks.com` |
| Workspace ID | `7474652083556195` |
| Catalog | `fevm_master_classic_marcus_catalog` |
| Schema | `abmb_rfp_presentation` |
| UC Volume (data) | `fevm_master_classic_marcus_catalog.abmb_rfp_presentation.raw_data` |
| UC Volume (PDFs) | `fevm_master_classic_marcus_catalog.abmb_rfp_presentation.product_pdfs` |
| Lakebase Project | `ABMB-RFP-PRESENTATION` (production branch, 8↔16 CU, Postgres 17, us-east-1) |
| FMAPI Model | `system.ai.databricks-glm-5-2` |
| Compute | Serverless (all notebooks) |

### Config Object (shared/config.py)
```python
CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "abmb_rfp_presentation"
VOLUME_DATA = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOLUME_PDFS = f"/Volumes/{CATALOG}/{SCHEMA}/product_pdfs"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
GLM_MODEL   = "system.ai.databricks-glm-5-2"
LAKEBASE_PROJECT = "ABMB-RFP-PRESENTATION"
```

---

## 3. Project File Structure

```
ABMB_RFP/
├── shared/
│   └── config.py                        # Single config — both parts source this
│
├── part_a_customer_360/
│   ├── 00_setup.py                      # Schema + volumes + Banking MVM explanation
│   ├── 01_data_generator.py            # Generate all 1,000-customer synthetic data
│   ├── 02_ingest_batch.py              # AutoLoader batch: CSV → bronze (Scenario 2)
│   ├── 03_ingest_nearrt.py             # AutoLoader micro-batch: JSON → bronze CDC sim
│   ├── 04_ingest_realtime.py           # Rate Source → Delta → continuous SDP
│   ├── 05_schema_evolution_demo.py     # Schema drift demo (repeatable reset)
│   ├── 06_rescued_data_demo.py         # Malformed data + _rescued_data demo
│   ├── 07_sdp_bronze_silver.py         # SDP SQL: bronze → silver (with DQ expectations)
│   ├── 08_sdp_silver_gold.py           # SDP SQL: silver → gold_customer_360
│   ├── 09_pdf_pipeline.py              # Lakeflow Designer SQL + notebook version
│   └── data/
│       ├── pdfs/                        # 5 Alliance Bank product catalog PDFs
│       ├── batch_v1/                    # CSV schema v1 (customer_master, accounts, txn, cards)
│       ├── batch_v2/                    # CSV schema v2 (new columns for evolution demo)
│       ├── nearrt/                      # JSON files (credit_bureau, telco_events)
│       └── streaming/                   # Seed JSON for rate-source simulation
│
├── part_b_hyperpersonalization/
│   ├── 00_setup.py
│   ├── 01_overview.py
│   ├── 02_eda.py
│   ├── 03_feature_engineering.py       # Lakebase synced tables + feature store
│   ├── 04_model_training.py            # XGBoost multi-class product recommendation
│   ├── 05_batch_inference.py
│   ├── 06_realtime_inference.py        # Model serving endpoint
│   ├── 07_observability.py             # MLflow, SHAP, drift monitoring
│   ├── 08_mlflow_deploy.py             # Evaluate → Approve → Deploy job
│   ├── 09_unity_ai_gateway.py          # GLM 5.2 governance (adapted UAIG demo)
│   └── app/
│       ├── app.yaml
│       ├── app.py                       # FastAPI backend
│       ├── requirements.txt
│       └── frontend/
│           ├── src/
│           │   ├── App.tsx
│           │   ├── theme.ts             # Alliance Bank: navy #1B3A6B, red #C8102E
│           │   ├── pages/
│           │   │   ├── CustomerSearch.tsx
│           │   │   ├── Customer360ViewA.tsx    # Template A: Banker's Workstation
│           │   │   ├── Customer360ViewB.tsx    # Template B: Intelligence Hub
│           │   │   ├── Recommendations.tsx
│           │   │   └── AIWorkflows.tsx         # GLM 5.2 email drafting
│           │   └── components/
│           │       ├── AllianceBankHeader.tsx
│           │       ├── CustomerKPIRow.tsx
│           │       ├── RecommendationCard.tsx
│           │       └── EmailDraftModal.tsx
│           └── public/
│               └── alliance_bank_logo.png
│
└── docs/
    └── superpowers/specs/
        └── 2026-09-07-abmb-rfp-demo-design.md  ← this file
```

---

## 4. Table Architecture

All tables: `fevm_master_classic_marcus_catalog.abmb_rfp_presentation.<table_name>`

### 4.1 Bronze Layer (7 tables — raw, schema-on-read)

| Table | Source System | Format | Frequency | Row Count |
|---|---|---|---|---|
| `bronze_customer_master` | CRM / Core Banking CIF | CSV | Batch daily | 1,000 |
| `bronze_core_banking_accounts` | IBM i/Db2 (T24) — deposit accounts | CSV | Batch EOD | ~1,200 |
| `bronze_loans` | IBM i/Db2 (T24) — loan facilities | CSV | Batch EOD | ~600 |
| `bronze_core_banking_txn` | IBM i/Db2 (T24) | CSV | Batch EOD | ~50,000 |
| `bronze_cards_txn` | Cards System | CSV | Batch EOD | ~10,000 |
| `bronze_credit_bureau` | CTOS/CCRIS API (CDC sim) | JSON | Near-RT micro-batch | 1,000 |
| `bronze_telco_events` | Telco Partner API (CDC sim) | JSON | Near-RT micro-batch | ~5,000 |
| `bronze_digital_events` | Digital Banking / Clickstream | JSON stream | Real-time continuous | ongoing |

All bronze tables use `_rescued_data` column (AutoLoader `rescuedDataColumn` option) and `_metadata` columns for ingestion tracking.

### 4.2 Silver Layer (8 tables — MVM-conformed)

These tables follow the **Databricks Banking Minimum Viable Model (MVM)** schema — 17 domains, 227 tables. We populate 6 of the 17 domains. Full Banking MVM DDL is explained in `07_sdp_bronze_silver.py`.

| Silver Table | MVM Domain | MVM Entity | Key Mapping |
|---|---|---|---|
| `silver_customers` | `customer` | `party` + `individual_profile` | CRM + core banking CIF |
| `silver_deposit_accounts` | `account` | `deposit_account` | Core banking accounts |
| `silver_loan_accounts` | `loan` | `loan_account` | Core banking loans |
| `silver_transactions` | `payment` | `payment_transaction` | All debit/credit movements |
| `silver_card_transactions` | `payment` | `payment_instruction` (card) | Cards system |
| `silver_kyc_compliance` | `compliance` | `kyc_review` + CTOS/CCRIS ext. | Credit bureau JSON |
| `silver_digital_activity` | `channel` | `digital_channel` + events | Telco + digital stream |
| `silver_product_catalog` | `reference` | `product_type` extended | PDF pipeline output |

### 4.3 Gold Layer (2 tables)

| Table | Description | Row Count |
|---|---|---|
| `gold_customer_360` | Wide denormalized join: 1 row per customer, ~60 features | 1,000 |
| `gold_product_recommendations` | Batch inference output from Part B ML model | 1,000 |

### 4.4 Lakebase Tables (Postgres — ABMB-RFP-PRESENTATION project)

| Table | Purpose |
|---|---|
| `customer_features` | Feature store synced table for real-time ML inference |
| `real_time_predictions` | Cached predictions for app response latency |

---

## 5. Part A: Customer 360 — Detailed Design

### 5.1 Data Simulation (01_data_generator.py)

Generates all synthetic data for 1,000 Malaysian banking customers. Uses Python Faker + custom Malaysian data patterns. Output: files written to UC Volumes.

#### Customer Profile Distribution
- 850 individual retail customers, 150 SME/corporate
- Age range: 22–65, normally distributed, mean 38
- Segments: mass_market (400), affluent (350), high_net_worth (150), private_banking (50), premier (50)
- Income range: RM 24,000–RM 2,400,000/yr
- Employment: employed_full_time (60%), self_employed (20%), retired (10%), other (10%)
- Shariah preference: ~40% (realistic for Malaysian Islamic banking market)
- Geographic distribution: Selangor (28%), KL (22%), Johor (12%), Penang (10%), others

#### bronze_customer_master fields (CSV)
Maps to Banking MVM `customer.party` + `customer.individual_profile`:
```
cif_number, legal_name, first_name, last_name,
citizenship_country_code,          -- MYS
national_id_number,                -- IC number format: YYMMDD-ST-NNNN
date_of_birth, gender,
customer_segment,                  -- retail|corporate|institutional
lifestyle_segment,                 -- mass_market|affluent|high_net_worth|ultra_high_net_worth|private_banking|premier
employment_status,                 -- employed_full_time|employed_part_time|self_employed|unemployed|retired
employer_name,
annual_income_amount,
marital_status,                    -- single|married|divorced|widowed
number_of_dependents,
education_level,                   -- high_school|bachelor|master|doctorate
occupation_code,                   -- NAICS-aligned
preferred_language_code,           -- ms|en
preferred_contact_method,          -- email|sms|phone|mobile_app
residency_status,                  -- resident|non_resident
is_pep,
pep_classification,
risk_rating,                       -- low|medium|high
kyc_status,                        -- verified|pending|expired
kyc_completion_date,
kyc_next_review_date,
lifecycle_status,                  -- active|dormant|closed
relationship_start_date,
digital_banking_enrollment_flag,
digital_banking_enrollment_date,
mobile_app_user_flag,
biometric_auth_enabled,
marketing_consent_flag,
credit_bureau_reporting_consent_flag,
net_worth_band,                    -- under_100k|100k_to_500k|500k_to_1m|1m_to_5m|5m_to_10m|over_10m
nps_score,
is_shariah_preferred,              -- custom extension
primary_state,                     -- Malaysian state
source_system_code,                -- T24
record_created_timestamp, record_updated_timestamp
```
*(45 fields — covers all key MVM party + individual_profile columns)*

#### bronze_core_banking_accounts fields (CSV)
Maps to Banking MVM `account.deposit_account`:
```
deposit_account_id, party_id (cif_number),
account_number,                    -- 10-digit
account_type,                      -- DDA|savings|money_market|certificate_of_deposit
account_status,                    -- active|dormant|closed|frozen
currency_id,                       -- MYR|USD|SGD
current_balance, available_balance,
opening_date, closing_date,
interest_rate,                     -- pa decimal
compounding_frequency,             -- daily|monthly|quarterly
last_activity_date,
overdraft_limit, overdraft_opt_in_status,
daily_withdrawal_limit, daily_transfer_limit, atm_withdrawal_limit,
ownership_type,                    -- individual|joint
statement_cycle,                   -- monthly|quarterly
iban,                              -- MY-format
aml_risk_rating,
dormancy_classification_date,
crs_reportable_flag, fatca_status,
regulatory_classification,         -- transaction_account|savings_deposit|time_deposit
created_timestamp, last_modified_timestamp,
source_system_code                 -- T24
```
*(30 fields — covers MVM deposit_account key columns)*

Distribution: avg 1.2 deposit accounts/customer → ~1,200 rows; 60% DDA, 30% savings, 10% CD/MM.
Note: loan facilities are in a separate `bronze_loans` CSV (see below) — this matches T24 real-world extract pattern (ACCOUNT.ARRANGEMENT vs LOAN.ARRANGEMENT are separate extracts).

#### bronze_loans fields (CSV)
Maps to Banking MVM `loan.loan_account`:
```
loan_account_id, party_id, deposit_account_id (linked settlement account),
loan_type,                         -- home_loan|personal_loan|hire_purchase|overdraft|term_loan
loan_status,                       -- active|settled|defaulted|written_off|restructured
original_principal_amount_myr,
outstanding_principal_myr,
monthly_installment_myr,
profit_rate_pa_pct,
rate_type,                         -- fixed|variable
disbursement_date, maturity_date,
tenure_months_original, tenure_months_remaining,
collateral_type,                   -- property|vehicle|none
collateral_value_myr,
arrears_amount_myr, arrears_days,
provision_amount_myr,
ecl_stage,                         -- Stage1|Stage2|Stage3 (MFRS 9)
last_payment_date, last_payment_amount_myr,
source_system_code
```
*(20 fields — covers MVM loan_account key columns)*

Distribution: ~0.6 loan facilities/customer → ~600 rows; 35% home loan, 30% personal loan, 20% hire purchase, 15% other

#### bronze_core_banking_txn fields (CSV)
Maps to Banking MVM `payment.payment_transaction`:
```
payment_transaction_id, party_id, deposit_account_id,
txn_type,                          -- DEBIT|CREDIT
payment_rail_type,                 -- FPX|IBG|DuitNow|SWIFT|ATM|branch|card|internal
payment_amount, currency_id,
balance_after,
value_date, posting_date,
instruction_status,                -- settled|pending|rejected|reversed
channel,                           -- mobile_app|web|branch|atm|api
merchant_name, merchant_category_code,
reference_number,
remittance_information,
aml_monitoring_flag,
sanctions_screening_status,        -- CLEARED|FLAGGED|PENDING
regulatory_reporting_flag,
settlement_method,                 -- RTGS|DNS|BATCH
created_timestamp
```
*(22 fields — covers MVM payment_transaction core columns)*

Distribution: ~50 txns/customer/year → ~50,000 rows; dates: last 18 months

#### bronze_cards_txn fields (CSV)
Maps to Banking MVM `payment.instruction` (card-type):
```
card_instruction_id, party_id, deposit_account_id,
card_last_four, card_type,         -- VISA|MASTERCARD
merchant_name, merchant_category_code, merchant_country,
txn_amount, currency_id,
fx_rate, base_currency_amount,
txn_timestamp, posting_date,
is_contactless, is_online,
is_overseas, authorization_code,
instruction_status,                -- settled|pending|disputed|reversed
dispute_flag, dispute_reason,
created_timestamp
```
*(21 fields)*

Distribution: ~400 cardholders × 25 txns/year → ~10,000 rows

#### bronze_credit_bureau fields (JSON)
Maps to Banking MVM `compliance.kyc_review` + custom CTOS/CCRIS extension:
```
ccris_subject_id, cif_number, inquiry_date,
credit_bureau_source,              -- CCRIS|CTOS
ctos_score,                        -- 300–850
ccris_status,                      -- clear|adverse|caution
total_credit_facilities,
total_outstanding_balance_myr,
secured_facilities_count, unsecured_facilities_count,
home_loan_count, personal_loan_count, hire_purchase_count,
credit_card_count, credit_card_outstanding_balance,
monthly_total_commitment_myr,
payment_conduct_12m,               -- clean|1_missed|2_missed|3+_missed
legal_cases_count, legal_cases_total_myr,
bankruptcy_status,                 -- none|voluntary|involuntary
inquiry_count_last_6m,
earliest_credit_date,
-- MVM kyc_review fields
kyc_review_id, kyc_status, kyc_review_date, kyc_next_review_date,
kyc_reviewer_id, risk_rating_assigned,
aml_pep_check_status, sanctions_check_status,
source_system, data_as_of_date, retrieved_timestamp
```
*(32 fields)*

#### bronze_telco_events fields (JSON)
Maps to Banking MVM `channel.digital_channel` (behavioral extension):
```
event_id, party_id, event_date,
telco_provider,                    -- Maxis|Celcom|Digi|U-Mobile|Yes
account_type,                      -- prepaid|postpaid
-- behavioral signals
data_consumption_mb_monthly,
voice_minutes_monthly,
sms_count_monthly,
roaming_flag, roaming_days,
device_type,                       -- smartphone|tablet|feature_phone
device_brand,                      -- Apple|Samsung|Huawei|Oppo|Vivo|Xiaomi
os_type,                           -- iOS|Android
app_category_top_1,                -- banking|ecommerce|social|gaming|news
app_category_top_2,
fintech_app_installed,             -- boolean
location_state,
-- derived signals
digital_maturity_score,            -- 1–10
arpu_myr,
tenure_months,
churn_risk_score,                  -- 0.0–1.0 (from telco model)
last_updated
```
*(22 fields)*

#### bronze_digital_events fields (JSON stream — Rate Source)
Maps to `channel.digital_channel` events:
```
event_id, party_id, session_id,
event_timestamp,
event_type,                        -- login|view_product|apply|fund_transfer|bill_pay|investment|logout
product_category_viewed,           -- CREDIT_CARD|PERSONAL_LOAN|HOME_LOAN|INVESTMENT|INSURANCE|null
page_name,
duration_seconds,
device_type,                       -- mobile|web|tablet
channel,                           -- mobile_app|web_banking
session_count_today,
ip_state,
is_authenticated
```
*(13 fields)*

### 5.2 Ingestion Notebooks

#### 02_ingest_batch.py — AutoLoader Batch
- Reads from `VOLUME_DATA/batch_v1/` using `cloudFiles` format
- `trigger(availableNow=True)` → writes to `bronze_customer_master`, `bronze_core_banking_accounts`, `bronze_loans`, `bronze_core_banking_txn`, `bronze_cards_txn`
- Options: `inferSchema=true`, `header=true`, `rescuedDataColumn="_rescued_data"`, `cloudFiles.schemaLocation`
- Databricks Job trigger: scheduled daily at 23:00 (simulates EOD)
- Clear demo narrative: "This is your Scenario 2 — every night, Core Banking pushes CSV extracts. AutoLoader detects new files, validates schema, loads to bronze."

#### 03_ingest_nearrt.py — AutoLoader Micro-Batch CDC Simulation
- Reads from `VOLUME_DATA/nearrt/` using `cloudFiles` JSON format
- `trigger(processingTime="30 seconds")` → writes to `bronze_credit_bureau`, `bronze_telco_events`
- The data generator writes new JSON files every 30 seconds to simulate CDC push
- Options: same as batch plus `multiLine=true`
- Demo narrative: "This simulates CDC from CTOS/CCRIS and your Telco partner API — near-real-time, every 30 seconds. In production this would be Lakeflow Connect from SQL Server."

#### 04_ingest_realtime.py — Rate Source Streaming
- Uses Spark `readStream` with format `rate`, rows-per-second=2
- Generates synthetic digital events in-memory → writes to `bronze_digital_events`
- SDP pipeline reads this with `CONTINUOUS` trigger
- Demo narrative: "This is true streaming — digital events from mobile app and web banking flowing in real-time. Watch the row count increment live."

### 5.3 Schema Evolution Demo (05_schema_evolution_demo.py)

Repeatable "demo reset" structure:

**Act 1 — Baseline schema (v1):**
- Generator writes `customer_master_v1.csv` to volume (no `occupation_code`, no `is_shariah_preferred`)
- AutoLoader infers schema, writes to bronze, schema logged to `_schema/`
- Show: `DESCRIBE bronze_customer_master` — 43 columns

**Act 2 — New source onboarded (Scenario 1):**
- Generator writes `customer_master_v2.csv` with 2 new columns
- AutoLoader detects new schema, `cloudFiles.schemaEvolutionMode=addNewColumns` merges schema
- Show: `DESCRIBE bronze_customer_master` — 45 columns; old rows have NULL for new columns
- Show Unity Catalog lineage: new columns traced back to source

**Reset cell:** Drops tables, clears schema checkpoint, re-runs Act 1 in one cell.

### 5.4 Rescued Data Demo (06_rescued_data_demo.py)

Repeatable:

**Step 1:** Normal ingestion — clean CSV file, all rows loaded successfully
**Step 2:** Inject malformed rows:
- `annual_income_amount = "TWO HUNDRED THOUSAND"` (string where decimal expected)
- `date_of_birth = "not-a-date"` 
- Extra unknown column `fraud_score` appears
**Step 3:** Show `_rescued_data` column — malformed values quarantined as JSON, rest of row preserved
**Step 4:** Show DQ expectation in SDP that flags rescued_data IS NOT NULL → quarantine table
**Reset cell:** Re-runs clean ingestion.

### 5.5 SDP Pipelines

#### 07_sdp_bronze_silver.py
Spark Declarative Pipeline (SQL mode). Notebook opens with markdown section explaining:
> "These silver tables follow the Databricks Banking Minimum Viable Model (MVM) — a pre-built industry data model covering 17 domains and 227 tables for banking. We populate 6 of the 17 domains. The MVM is Databricks' published FSI industry standard, giving ABMB a head start on the Enterprise Data Model rather than building from scratch."

Pipelines defined:

**silver_customers:**
```sql
-- DQ Expectations (bronze → silver)
CONSTRAINT valid_party_id EXPECT (cif_number IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_ic_format EXPECT (national_id_number RLIKE '^[0-9]{6}-[0-9]{2}-[0-9]{4}$') ON VIOLATION QUARANTINE
CONSTRAINT valid_lifecycle_status EXPECT (lifecycle_status IN ('active','dormant','closed','merged')) ON VIOLATION QUARANTINE
CONSTRAINT non_negative_income EXPECT (annual_income_amount >= 0) ON VIOLATION DROP ROW
CONSTRAINT valid_risk_rating EXPECT (risk_rating IN ('low','medium','high','prohibited')) ON VIOLATION QUARANTINE
CONSTRAINT valid_kyc_status EXPECT (kyc_status IN ('verified','pending','expired','rejected')) ON VIOLATION QUARANTINE
CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION QUARANTINE
-- Plus SCD Type 2 merge logic for customer changes
```

**silver_deposit_accounts:**
```sql
CONSTRAINT valid_account_id EXPECT (deposit_account_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_account_type EXPECT (account_type IN ('DDA','savings','money_market','NOW','certificate_of_deposit','custodial')) ON VIOLATION DROP ROW
CONSTRAINT non_negative_balance EXPECT (current_balance >= 0) ON VIOLATION QUARANTINE
CONSTRAINT valid_account_status EXPECT (account_status IN ('active','dormant','closed','frozen','restricted','pending_opening')) ON VIOLATION QUARANTINE
CONSTRAINT opening_before_closing EXPECT (closing_date IS NULL OR closing_date >= opening_date) ON VIOLATION DROP ROW
CONSTRAINT valid_currency EXPECT (currency_id IN ('MYR','USD','SGD','EUR','GBP')) ON VIOLATION QUARANTINE
```

**silver_transactions:**
```sql
CONSTRAINT valid_txn_id EXPECT (payment_transaction_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT positive_amount EXPECT (payment_amount > 0) ON VIOLATION DROP ROW
CONSTRAINT valid_currency EXPECT (currency_id IN ('MYR','USD','SGD','EUR','GBP')) ON VIOLATION QUARANTINE
CONSTRAINT valid_status EXPECT (instruction_status IN ('settled','pending','rejected','cancelled','reversed')) ON VIOLATION QUARANTINE
CONSTRAINT valid_rail EXPECT (payment_rail_type IN ('FPX','IBG','DuitNow','SWIFT','ATM','branch','card','internal')) ON VIOLATION QUARANTINE
CONSTRAINT valid_sanctions_status EXPECT (sanctions_screening_status IN ('CLEARED','FLAGGED','PENDING','BLOCKED')) ON VIOLATION QUARANTINE
CONSTRAINT posting_after_value EXPECT (posting_date >= value_date) ON VIOLATION QUARANTINE
```

**silver_kyc_compliance (CTOS/CCRIS):**
```sql
CONSTRAINT valid_cif EXPECT (cif_number IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_ctos_score EXPECT (ctos_score BETWEEN 300 AND 850) ON VIOLATION QUARANTINE
CONSTRAINT valid_payment_conduct EXPECT (payment_conduct_12m IN ('clean','1_missed','2_missed','3+_missed')) ON VIOLATION QUARANTINE
CONSTRAINT no_negative_balance EXPECT (total_outstanding_balance_myr >= 0) ON VIOLATION DROP ROW
CONSTRAINT valid_bankruptcy_status EXPECT (bankruptcy_status IN ('none','voluntary','involuntary')) ON VIOLATION QUARANTINE
```

**silver_digital_activity:**
```sql
CONSTRAINT valid_event_type EXPECT (event_type IN ('login','view_product','apply','fund_transfer','bill_pay','investment','logout')) ON VIOLATION DROP ROW
CONSTRAINT valid_party EXPECT (party_id IS NOT NULL) ON VIOLATION DROP ROW
```

**silver_loan_accounts:**
```sql
CONSTRAINT valid_loan_id EXPECT (loan_account_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT positive_principal EXPECT (original_principal_amount > 0) ON VIOLATION DROP ROW
CONSTRAINT valid_loan_status EXPECT (loan_status IN ('active','settled','defaulted','written_off','restructured')) ON VIOLATION QUARANTINE
```

All silver tables use `APPLY CHANGES INTO` (SCD Type 2) for customer + account entities.

#### 08_sdp_silver_gold.py
Assembles `gold_customer_360` — 1 row per customer, ~65 features:
```sql
CREATE OR REPLACE LIVE TABLE gold_customer_360 AS
SELECT
  -- Identity (from silver_customers)
  c.party_id, c.cif_number, c.legal_name, c.first_name, c.last_name,
  c.national_id_number, c.date_of_birth,
  DATEDIFF(YEAR, c.date_of_birth, CURRENT_DATE()) AS age,
  c.gender, c.citizenship_country_code, c.residency_status,
  c.customer_segment, c.lifestyle_segment,
  c.employment_status, c.employer_name, c.annual_income_amount,
  c.marital_status, c.number_of_dependents, c.education_level,
  c.is_pep, c.is_sanctioned, c.risk_rating, c.kyc_status,
  c.lifecycle_status,
  DATEDIFF(YEAR, c.relationship_start_date, CURRENT_DATE()) AS relationship_tenure_years,
  c.digital_banking_enrollment_flag, c.mobile_app_user_flag,
  c.biometric_auth_enabled, c.marketing_consent_flag,
  c.net_worth_band, c.nps_score, c.is_shariah_preferred,
  c.preferred_language_code, c.preferred_contact_method, c.primary_state,

  -- Account summary (from silver_deposit_accounts)
  acct.num_accounts, acct.total_deposit_balance_myr,
  acct.has_current_account, acct.has_savings_account,
  acct.has_fixed_deposit, acct.avg_account_balance,

  -- Loan summary (from silver_loan_accounts)
  loan.num_loan_facilities, loan.total_loan_outstanding_myr,
  loan.monthly_loan_commitment_myr, loan.has_home_loan,
  loan.has_personal_loan, loan.has_hire_purchase,

  -- Transaction behaviour (from silver_transactions + silver_card_transactions)
  txn.txn_count_30d, txn.txn_count_90d, txn.txn_count_12m,
  txn.total_debit_30d_myr, txn.total_credit_30d_myr,
  txn.avg_txn_amount_myr, txn.has_credit_card,
  txn.card_spend_30d_myr, txn.overseas_txn_flag,

  -- Credit bureau (from silver_kyc_compliance)
  kb.ctos_score, kb.ccris_status,
  kb.total_credit_facilities, kb.monthly_total_commitment_myr,
  kb.payment_conduct_12m, kb.legal_cases_count, kb.bankruptcy_status,
  kb.credit_card_count, kb.inquiry_count_last_6m,

  -- Digital + Telco (from silver_digital_activity)
  da.digital_activity_score, da.mobile_sessions_30d,
  da.product_views_last_30d, da.last_product_category_viewed,
  da.telco_data_consumption_gb_monthly, da.telco_arpu_myr,
  da.digital_maturity_score,

  -- Products held (derived flags for recommendation model)
  CASE WHEN txn.has_credit_card = TRUE THEN 1 ELSE 0 END AS owns_credit_card,
  CASE WHEN loan.has_home_loan = TRUE THEN 1 ELSE 0 END AS owns_home_loan,
  CASE WHEN loan.has_personal_loan = TRUE THEN 1 ELSE 0 END AS owns_personal_loan,
  -- investment and insurance flags from loan/account categories
  
  CURRENT_TIMESTAMP() AS gold_computed_at

FROM silver_customers c
LEFT JOIN (...account agg...) acct ON c.party_id = acct.party_id
LEFT JOIN (...loan agg...) loan ON c.party_id = loan.party_id
LEFT JOIN (...txn agg...) txn ON c.party_id = txn.party_id
LEFT JOIN silver_kyc_compliance kb ON c.cif_number = kb.cif_number
LEFT JOIN (...digital agg...) da ON c.party_id = da.party_id
```

### 5.6 PDF Pipeline (09_pdf_pipeline.py)

#### 5 Product Catalog PDFs to generate:
Each PDF is a realistic 2-page Alliance Bank product brochure (Alliance Bank navy/red branding). Generated using `reportlab` library with structured content.

| PDF Filename | Product | ai_classify Label |
|---|---|---|
| `alliance_visa_platinum.pdf` | Alliance Bank Visa Platinum Credit Card | `CREDIT_CARD` |
| `alliance_cashfirst_financing.pdf` | Alliance CashFirst Personal Financing-i | `PERSONAL_LOAN` |
| `alliance_homesmart_financing.pdf` | Alliance HomeSmart Financing | `HOME_LOAN` |
| `alliance_wealthsmart_fund.pdf` | Alliance WealthSmart Income Fund | `INVESTMENT` |
| `alliance_carstar_takaful.pdf` | Alliance CarStar Takaful | `INSURANCE` |

#### Complete ai_extract Schema (34 universal fields across all 5 products):
```sql
ai_extract(parsed_text, named_struct(
  -- Universal (all 5 products)
  'product_name',              'Full product name as printed on the document',
  'product_code',              'Internal product reference code or identifier',
  'min_amount_myr',            'Minimum financing, credit limit, or investment amount in MYR',
  'max_amount_myr',            'Maximum financing, credit limit, or investment amount in MYR',
  'min_rate_pct',              'Minimum interest, profit, management, or contribution rate as percentage',
  'max_rate_pct',              'Maximum interest, profit, management, or contribution rate as percentage',
  'min_tenure_months',         'Minimum product tenure or lock-in period in months',
  'max_tenure_months',         'Maximum product tenure or coverage period in months',
  'min_income_annual_myr',     'Minimum annual gross income required for eligibility in MYR',
  'shariah_compliant',         'Whether the product is Shariah-compliant: Yes or No',
  'annual_fee_myr',            'Annual fee, subscription, or contribution amount in MYR',
  'annual_fee_waiver',         'Condition under which the annual fee is waived, if applicable',
  'processing_fee',            'One-time processing, application, or origination fee',
  'early_termination_fee',     'Penalty or fee for early settlement, cancellation, or redemption',
  'key_benefit_1',             'First key feature or benefit headline from the product brochure',
  'key_benefit_2',             'Second key feature or benefit headline',
  'key_benefit_3',             'Third key feature or benefit headline',
  'fees_summary',              'Brief summary of all recurring and one-time fees and charges',
  'eligibility_age_min',       'Minimum applicant age in years',
  'eligibility_age_max',       'Maximum applicant age in years',
  'eligibility_nationality',   'Eligible nationality or residency status required',
  'eligibility_employment',    'Eligible employment types or income sources',
  'documents_required',        'List of documents required for application',
  -- Credit card specific
  'cashback_rate_pct',         'Cashback or rebate rate percentage for credit cards, null if not applicable',
  'reward_points_per_rm',      'Reward points or miles earned per RM1 spent, null if not applicable',
  'lounge_access_included',    'Whether complimentary airport lounge access is included: Yes, No, or null',
  -- Loan / financing specific
  'profit_rate_type',          'Whether rate is fixed or variable (floating), null if not a loan product',
  'collateral_required',       'Whether collateral or security is required: Yes, No, or null',
  'max_dsr_pct',               'Maximum debt service ratio percentage allowed, null if not applicable',
  -- Investment specific
  'fund_risk_rating',          'Risk rating of the fund: Low, Medium, High, or null if not investment',
  'capital_guaranteed',        'Whether capital is guaranteed: Yes, No, or null if not investment',
  'distribution_frequency',    'How often distributions or dividends are paid, null if not applicable',
  -- Insurance specific
  'sum_covered_max_myr',       'Maximum sum assured or covered in MYR, null if not insurance',
  'effective_date',            'Date this product brochure pricing or terms became effective'
))
```

#### Lakeflow Designer 3-Node Pipeline SQL (also runnable as notebook):
```sql
-- NODE 1: Parse PDFs
CREATE OR REPLACE TABLE silver_pdf_parsed AS
SELECT path AS file_path,
  regexp_extract(path, '[^/]+(?=\\.[Pp][Dd][Ff])') AS product_code,
  ai_parse_document(content, 'text') AS parsed_text,
  length(content) AS file_size_bytes,
  current_timestamp() AS parsed_at
FROM read_files(
  'dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs',
  format => 'binaryFile', pathGlobFilter => '*.pdf'
);

-- NODE 2: Classify
CREATE OR REPLACE TABLE silver_pdf_classified AS
SELECT *, ai_classify(parsed_text,
  ARRAY('CREDIT_CARD','PERSONAL_LOAN','HOME_LOAN','INVESTMENT','INSURANCE')
) AS product_category
FROM silver_pdf_parsed;

-- NODE 3: Extract
CREATE OR REPLACE TABLE silver_product_catalog AS
SELECT file_path, product_code, product_category, parsed_at,
  ai_extract(parsed_text, named_struct(
    ... [34 fields as above] ...
  )):product_name           AS product_name,
  ai_extract(...):min_amount_myr AS min_amount_myr,
  -- ... all 34 fields unpacked from the struct
  current_timestamp() AS extracted_at
FROM silver_pdf_classified;
```

---

## 6. Part B: Hyperpersonalization — Detailed Design

### 6.1 ML Model (notebooks 00–08, following sbc_churn_prediction structure)

**Problem framing:** Given a customer's full 360 profile, predict which 1–2 Alliance Bank products they are most likely to adopt next. Output: top-2 product recommendations with confidence scores.

**Target variable:** `next_best_product` — multi-class:
```
CREDIT_CARD | PERSONAL_LOAN | HOME_LOAN | INVESTMENT | INSURANCE | NO_ACTION
```

Label generation strategy (unsupervised simulation):
- Customers with `ctos_score > 700`, `annual_income > 60K`, no card → label `CREDIT_CARD`
- Customers with `age 28-45`, married, 1+ dependents, no home loan, income > 48K → label `HOME_LOAN`
- Customers with `monthly_commitment_myr/annual_income > 0.2` (debt consolidation need) → label `PERSONAL_LOAN`
- Customers with `net_worth_band in [500k_to_1m, 1m_to_5m]`, no investment → label `INVESTMENT`
- Customers with `has_hire_purchase = true` and no motor insurance → label `INSURANCE`
- Remaining → `NO_ACTION`

**20 Features for model:**
```
tenure_years, total_deposit_balance_myr, num_accounts,
annual_income_amount, net_worth_band_encoded,
ctos_score, ccris_status_encoded, payment_conduct_score,
age, num_dependents, employment_status_encoded,
monthly_loan_commitment_myr, num_loan_facilities,
txn_count_30d, avg_txn_amount_myr, has_credit_card,
digital_maturity_score, telco_arpu_myr,
is_shariah_preferred, last_product_category_viewed_encoded
```

**Algorithm:** XGBoost multi-class (`objective=multi:softprob`)  
**Metric:** Macro-F1 (balanced across 6 classes)  
**Output:** `predict_proba` → top-2 products by probability  
**MLflow:** UC Model Registry, aliases `dev` → `champion`

### 6.2 Feature Engineering (03_feature_engineering.py)

- Creates feature table `fevm_master_classic_marcus_catalog.abmb_rfp_presentation.customer_features`
- Lakebase synced table: `ABMB-RFP-PRESENTATION.customer_features` (Postgres)
- Feature lookup key: `party_id`
- FeatureLookup used for both training (batch) and real-time serving

### 6.3 Serving Endpoint (06_realtime_inference.py)

- Endpoint name: `abmb-product-recommendation-<username>`
- Input: `party_id` → feature lookup → model → top-2 products + probabilities
- AI Gateway inference logging enabled → `gold_endpoint_payload` table

### 6.4 Unity AI Gateway Demo (09_unity_ai_gateway.py)

Adapted from course notebook 02, repurposed for ABMB email drafting:

1. **Section A:** Create GLM 5.2 model service in Unity AI Gateway
   - Service name: `abmb-glm-email-service`
   - Destination: `system.ai.databricks-glm-5-2` (pay-per-token)
2. **Section B:** Inference table setup → every AI call logged for compliance audit
3. **Section C:** Rate limits → max 100 requests/minute per user
4. **Section D:** Banking guardrails:
   - Block responses containing specific financial advice ("you should invest in...")
   - Block PII in responses (IC numbers, account numbers in responses)
5. **Section E:** Traffic splitting → 70% GLM 5.2 (quality) / 30% GLM 5.3 Flash (speed/cost)
6. **Section F:** Query `system.ai_gateway.usage` — show audit trail ("every AI call, every response, all logged")

### 6.5 Databricks App

**Backend (app.py — FastAPI):**
- `GET /customer/{party_id}` → query `gold_customer_360` from catalog
- `POST /recommend/{party_id}` → call model serving endpoint
- `POST /draft-email` → call GLM 5.2 via Unity AI Gateway with recommendation context
- `GET /products` → query `silver_product_catalog`

**Frontend — 2 Templates:**

**Template A: Banker's Workstation** (professional, information-dense)
- Left sidebar: Customer search + recent customers list
- Main: Tabbed view — Profile | Accounts | Transactions | Credit | Digital Behaviour
- Right rail: Live Recommendations (top 2) + AI Actions (draft email, flag for review)
- Alliance Bank logo header, navy `#1B3A6B` sidebar, white main area

**Template B: Intelligence Hub** (visual, card-based)
- Hero card: customer photo placeholder, segment badge, NPS ring, tenure
- KPI tiles: Total Balance | Credit Score | Tenure | Digital Score
- Split: Customer 360 activity timeline (left) + Recommendation carousel (right)
- AI Panel: "Draft personalised email" button → GLM 5.2 generates email → editable preview

**GLM 5.2 Email Prompt Template:**
```
You are an Alliance Bank relationship manager. Based on this customer profile:
- Name: {name}, Segment: {segment}, Tenure: {tenure} years
- Recommended product: {product_name} (confidence: {confidence}%)
- Key reason: {reason}

Draft a short, professional, personalised email (max 150 words) in {preferred_language}.
Do NOT include specific rates or financial advice. Do NOT include any account numbers.
End with a call to action to schedule a meeting.
```

---

## 7. Data Quality Strategy

### DQ Expectations Summary
| Layer | Expectation Type | Action |
|---|---|---|
| Bronze | `_rescued_data IS NULL` | Quarantine to `quarantine_rescued` table |
| Bronze → Silver | PK NOT NULL | DROP ROW |
| Bronze → Silver | Enum values valid | QUARANTINE to `quarantine_invalid_values` |
| Bronze → Silver | Cross-field logic | QUARANTINE |
| Bronze → Silver | No negative amounts | QUARANTINE |
| Silver → Gold | Referential integrity (customer exists) | DROP ROW |
| Gold | Completeness: key fields not null | FAIL (halt pipeline) |

### Monitoring
- SDP pipeline metrics exposed via Pipeline UI (DQ expectations pass/fail per run)
- `system.lakeflow.*` system tables for pipeline observability

---

## 8. Demo Reset Instructions

For each live demo session, a reset is needed:

**Part A reset:**
1. Run `00_setup.py` → recreates schema and volumes (idempotent)
2. Run `01_data_generator.py` → regenerates fresh data files in volumes
3. Re-run each ingestion notebook in order

**Schema evolution reset:** Run the `RESET DEMO` cell in `05_schema_evolution_demo.py`  
**Rescued data reset:** Run the `RESET DEMO` cell in `06_rescued_data_demo.py`  
**SDP pipelines:** Delete and recreate pipeline in UI (or use `dbutils.notebook.run`)

---

## 9. Dependencies Between Parts

```
Part A outputs → Part B inputs
───────────────────────────────
gold_customer_360          → Part B: 03_feature_engineering, 04_model_training
silver_product_catalog     → Part B: app product display + email context
fevm_master_classic_marcus_catalog.abmb_rfp_presentation.* schema → Part B: assumes schema exists
```

Part B notebook `00_setup.py` validates that `gold_customer_360` exists before proceeding.

---

## 10. Out of Scope

- Genie Agent configuration (user will build and embed manually)
- Lakeview Dashboard creation (user will build and embed manually)
- Lakeflow Designer visual canvas wiring (user will wire 3-node PDF pipeline manually using `09_pdf_pipeline.py` as guide)
- CI/CD / DABs bundle (notebooks are standalone, self-contained)
- Production data from actual Alliance Bank systems
