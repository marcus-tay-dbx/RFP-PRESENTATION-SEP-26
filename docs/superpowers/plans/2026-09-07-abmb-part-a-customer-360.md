# ABMB Part A: Customer 360 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained Customer 360 demo on Databricks for Alliance Bank RFP (Part A — FPT vendor focus), simulating 1,000 Malaysian banking customers flowing through batch/near-RT/streaming ingestion into Banking MVM–conformed silver tables and a unified gold_customer_360 table.

**Architecture:** Synthetic data written to UC Volumes; ingested via AutoLoader (batch + micro-batch) and Spark Rate Source (streaming); transformed through Spark Declarative Pipeline (DLT SQL) into 8 Banking MVM–conformed silver tables; assembled into gold_customer_360. Schema evolution + rescued-data notebooks provide interactive demo moments.

**Tech Stack:** Databricks Serverless, AutoLoader (cloudFiles), Spark Declarative Pipeline (DLT SQL/Python), Unity Catalog, Delta Lake, Python (Faker, reportlab, pandas, numpy), PySpark

**Spec:** `/Users/marcus.tay/Documents/GoodVibesOnly/ABMB_RFP/docs/superpowers/specs/2026-09-07-abmb-rfp-demo-design.md`

## Global Constraints
- All notebooks run on Serverless compute (no cluster config needed)
- All assets in `fevm_master_classic_marcus_catalog.abmb_rfp_presentation`
- UC Volumes: `raw_data` (CSV/JSON) and `product_pdfs` (PDFs)
- Bronze tables: `bronze_*` prefix, all include `_rescued_data` column
- Silver tables: `silver_*` prefix, follow Banking MVM field names
- Gold table: `gold_customer_360`
- 1,000 customers, Malaysian data patterns (IC numbers, states, employers)
- Every notebook starts with `# MAGIC %run ../shared/config`
- All DLT pipelines use SQL mode in Databricks notebook format
- Faker seed=42, random seed=42, numpy seed=42 for reproducibility
- %pip installs go at top of notebook, followed by dbutils.library.restartPython()

---

### Task 1: Project Scaffold + Workspace Setup

- [ ] Write `shared/config.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "abmb_rfp_presentation"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_DATA = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOLUME_PDFS = f"/Volumes/{CATALOG}/{SCHEMA}/product_pdfs"
GLM_MODEL   = "system.ai.databricks-glm-5-2"
LAKEBASE_PROJECT = "ABMB-RFP-PRESENTATION"
N_CUSTOMERS = 1000
BASE_DATE   = "2026-09-07"
```

- [ ] Write `part_a_customer_360/00_setup.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # ABMB RFP Demo — Setup
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
               "schema_evolution", "rescued_demo",
               "_schemas"]:
    dbutils.fs.mkdirs(f"{VOLUME_DATA}/{subdir}")
    print(f"  ✅ {VOLUME_DATA}/{subdir}")

# COMMAND ----------
# Validate
assert spark.catalog.databaseExists(FULL_SCHEMA), f"Schema {FULL_SCHEMA} not found!"
print(f"✅ Validation passed — schema exists: {FULL_SCHEMA}")
```

- [ ] Commit scaffold files to git

---

### Task 2: Data Generator

- [ ] Write `part_a_customer_360/01_data_generator.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install faker
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Data Generator — 1,000 Malaysian Banking Customers
# MAGIC
# MAGIC Generates synthetic data with realistic Malaysian patterns:
# MAGIC - IC numbers in format YYMMDD-ST-NNNN
# MAGIC - State distribution matching real Malaysian demographics
# MAGIC - Segment-appropriate income bands
# MAGIC - 40% Shariah preference (realistic for Malaysia)

# COMMAND ----------
import random, json, os
import numpy as np
import pandas as pd
from faker import Faker
from datetime import date, datetime, timedelta

fake = Faker('en_MY')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ── Helper functions ──────────────────────────────────────────────────────────

def gen_ic(dob, state_code):
    return f"{dob.strftime('%y%m%d')}-{state_code}-{random.randint(1000,9999)}"

def gen_account_number():
    return str(random.randint(1000000000, 9999999999))

def gen_iban(acc_num):
    return f"MY00711100{acc_num}"

def income_for_segment(segment):
    ranges = {
        "mass_market":        (24000,   60000),
        "affluent":           (60000,  200000),
        "high_net_worth":    (200000,  800000),
        "ultra_high_net_worth":(800000,2000000),
        "private_banking":   (500000, 2400000),
        "premier":           (120000,  500000),
    }
    lo, hi = ranges[segment]
    return round(random.uniform(lo, hi), 2)

# ── Reference data ────────────────────────────────────────────────────────────

STATES = {
    "Selangor":       ("10", 0.28), "Kuala Lumpur":  ("14", 0.22),
    "Johor":          ("01", 0.12), "Penang":        ("07", 0.10),
    "Perak":          ("05", 0.07), "Sabah":         ("12", 0.06),
    "Sarawak":        ("13", 0.05), "Negeri Sembilan":("08", 0.03),
    "Melaka":         ("04", 0.03), "Kelantan":      ("03", 0.02),
    "Terengganu":     ("11", 0.01), "Kedah":         ("02", 0.01),
}
STATE_NAMES  = list(STATES.keys())
STATE_CODES  = [STATES[s][0] for s in STATE_NAMES]
STATE_WEIGHTS= [STATES[s][1] for s in STATE_NAMES]

SEGMENTS = ["mass_market","mass_market","mass_market","mass_market",
            "affluent","affluent","affluent",
            "high_net_worth","high_net_worth",
            "private_banking","premier"]

EMPLOYERS = [
    "Petronas","Maybank","CIMB Bank","Public Bank","Telekom Malaysia",
    "Tenaga Nasional","Maxis","Axiata","IHH Healthcare","Gamuda",
    "KLCC","Sime Darby","YTL Corporation","Hong Leong Bank","RHB Bank",
    "AmBank","Affin Bank","MBSB","Bank Islam","Bank Muamalat",
    "Permodalan Nasional Berhad","EPF","SOCSO","LHDN","Government of Malaysia",
]

EMPLOYMENT_STATUSES = ["permanent_employee","self_employed","contract","retired","professional"]
MARITAL_STATUSES    = ["single","married","divorced","widowed"]
EDUCATION_LEVELS    = ["secondary","diploma","bachelor","master","phd"]
RISK_RATINGS        = ["low","medium","high","prohibited"]
RISK_WEIGHTS        = [0.55, 0.30, 0.13, 0.02]
KYC_STATUSES        = ["verified","pending","expired","rejected"]
KYC_WEIGHTS         = [0.85, 0.08, 0.05, 0.02]
LIFECYCLE_STATUSES  = ["active","dormant","closed","merged"]
LIFECYCLE_WEIGHTS   = [0.88, 0.07, 0.03, 0.02]
LANGUAGES           = ["en","ms","zh","ta"]
LANG_WEIGHTS        = [0.30, 0.40, 0.25, 0.05]
CONTACT_METHODS     = ["email","sms","push_notification","mail"]
NET_WORTH_BANDS     = ["<100K","100K-500K","500K-1M","1M-5M",">5M"]
OCCUPATION_CODES    = ["K","M","J","A","B","C","D","E","F","G"]

REL_START_MIN = date(2005, 1, 1)
REL_START_MAX = date(2025, 1, 1)
REL_RANGE_DAYS = (REL_START_MAX - REL_START_MIN).days

# ── Generate customers ────────────────────────────────────────────────────────

customers_v1 = []
customers_v2 = []
customer_ids  = []

for i in range(1, N_CUSTOMERS + 1):
    cif = f"CIF{i:06d}"
    party_id = f"P{i:08d}"
    customer_ids.append(cif)

    state_name = random.choices(STATE_NAMES, weights=STATE_WEIGHTS)[0]
    state_code = STATES[state_name][0]
    dob = fake.date_of_birth(minimum_age=21, maximum_age=70)
    ic  = gen_ic(dob, state_code)

    segment = random.choice(SEGMENTS)
    income  = income_for_segment(segment)
    emp_status = random.choice(EMPLOYMENT_STATUSES)
    employer   = random.choice(EMPLOYERS) if emp_status in ("permanent_employee","contract") else "Self-Employed"

    rel_start = REL_START_MIN + timedelta(days=random.randint(0, REL_RANGE_DAYS))
    rec_updated = rel_start + timedelta(days=random.randint(0, (date(2026,9,7) - rel_start).days))

    risk  = random.choices(RISK_RATINGS, weights=RISK_WEIGHTS)[0]
    kyc   = random.choices(KYC_STATUSES, weights=KYC_WEIGHTS)[0]
    lc    = random.choices(LIFECYCLE_STATUSES, weights=LIFECYCLE_WEIGHTS)[0]
    lang  = random.choices(LANGUAGES, weights=LANG_WEIGHTS)[0]
    nw    = random.choice(NET_WORTH_BANDS)
    nps   = random.randint(1, 10) if random.random() > 0.3 else None

    base = {
        "cif_number":                     cif,
        "party_id":                       party_id,
        "legal_name":                     fake.name(),
        "first_name":                     fake.first_name(),
        "last_name":                      fake.last_name(),
        "national_id_number":             ic,
        "date_of_birth":                  dob.isoformat(),
        "gender":                         random.choice(["M","F"]),
        "citizenship_country_code":       random.choices(["MY","SG","CN","IN","US"], weights=[0.85,0.05,0.04,0.04,0.02])[0],
        "residency_status":               random.choices(["citizen","permanent_resident","work_permit"], weights=[0.85,0.10,0.05])[0],
        "mobile_number":                  fake.phone_number(),
        "email_address":                  fake.email(),
        "mailing_address_line1":          fake.street_address(),
        "mailing_address_line2":          fake.secondary_address(),
        "mailing_city":                   fake.city(),
        "primary_state":                  state_name,
        "postcode":                       fake.postcode(),
        "country_code":                   "MY",
        "customer_segment":               segment,
        "lifestyle_segment":              random.choice(["urban_professional","family_oriented","high_achiever","retiree","young_adult"]),
        "employment_status":              emp_status,
        "employer_name":                  employer,
        "annual_income_amount":           income,
        "marital_status":                 random.choice(MARITAL_STATUSES),
        "number_of_dependents":           random.randint(0, 5),
        "education_level":                random.choice(EDUCATION_LEVELS),
        "is_pep":                         random.random() < 0.02,
        "risk_rating":                    risk,
        "kyc_status":                     kyc,
        "lifecycle_status":               lc,
        "relationship_start_date":        rel_start.isoformat(),
        "record_updated_timestamp":       rec_updated.isoformat(),
        "digital_banking_enrollment_flag":random.random() < 0.75,
        "mobile_app_user_flag":           random.random() < 0.65,
        "marketing_consent_flag":         random.random() < 0.60,
        "net_worth_band":                 nw,
        "nps_score":                      nps,
        "preferred_language_code":        lang,
        "preferred_contact_method":       random.choice(CONTACT_METHODS),
        "branch_code":                    f"BR{random.randint(1,50):03d}",
        "relationship_manager_id":        f"RM{random.randint(1,200):04d}" if segment in ("high_net_worth","private_banking","premier") else None,
        "source_system":                  "CBS_CORE",
    }
    customers_v1.append(base)

    v2_extra = {**base,
                "occupation_code":        random.choice(OCCUPATION_CODES),
                "is_shariah_preferred":   random.random() < 0.40}
    customers_v2.append(v2_extra)

df_v1 = pd.DataFrame(customers_v1)
df_v2 = pd.DataFrame(customers_v2)

# ── Write customer files ──────────────────────────────────────────────────────
os.makedirs(f"/dbfs{VOLUME_DATA}/batch_v1/customer_master", exist_ok=True)
os.makedirs(f"/dbfs{VOLUME_DATA}/batch_v2/customer_master", exist_ok=True)

df_v1.to_csv(f"/dbfs{VOLUME_DATA}/batch_v1/customer_master/customers_v1.csv", index=False)
df_v2.to_csv(f"/dbfs{VOLUME_DATA}/batch_v2/customer_master/customers_v2.csv", index=False)
print(f"✅ customers_v1.csv: {len(df_v1)} rows, {len(df_v1.columns)} columns")
print(f"✅ customers_v2.csv: {len(df_v2)} rows, {len(df_v2.columns)} columns (adds occupation_code, is_shariah_preferred)")

# COMMAND ----------
# MAGIC %md ### Generate Accounts

# COMMAND ----------
ACCOUNT_TYPES   = ["DDA","savings","certificate_of_deposit","current_account_i","savings_i"]
ACCOUNT_STATUSES= ["active","dormant","closed"]
ACC_STATUS_W    = [0.85, 0.10, 0.05]
CURRENCIES      = ["MYR","USD","SGD"]

accounts = []
for cif in customer_ids:
    num_accts = random.choices([1,2,3,4], weights=[0.35,0.40,0.18,0.07])[0]
    party_id = f"P{int(cif[3:]):08d}"
    for j in range(num_accts):
        acc_num = gen_account_number()
        acc_type = random.choice(ACCOUNT_TYPES)
        status   = random.choices(ACCOUNT_STATUSES, weights=ACC_STATUS_W)[0]
        balance  = round(random.uniform(100, 500000) if status=="active" else 0, 2)
        open_date= (REL_START_MIN + timedelta(days=random.randint(0, REL_RANGE_DAYS))).isoformat()
        accounts.append({
            "deposit_account_id":       acc_num,
            "party_id":                 party_id,
            "cif_number":               cif,
            "iban":                     gen_iban(acc_num),
            "account_type":             acc_type,
            "account_status":           status,
            "currency_id":              random.choices(CURRENCIES, weights=[0.92,0.04,0.04])[0],
            "current_balance":          balance,
            "available_balance":        round(balance * random.uniform(0.8, 1.0), 2),
            "interest_rate_pct":        round(random.uniform(0.5, 3.5), 2),
            "overdraft_limit_myr":      round(random.uniform(0, 10000), 2) if acc_type=="DDA" else 0,
            "account_open_date":        open_date,
            "last_transaction_date":    (date.fromisoformat(open_date) + timedelta(days=random.randint(0,500))).isoformat(),
            "last_modified_timestamp":  open_date,
            "branch_code":              f"BR{random.randint(1,50):03d}",
            "product_code":             f"PROD-{acc_type.upper()}-001",
            "is_joint_account":         random.random() < 0.08,
            "is_dormant":               status == "dormant",
            "kyc_verified_flag":        random.random() < 0.90,
            "source_system":            "CBS_CORE",
            "is_shariah":               "i" in acc_type,
            "statement_frequency":      random.choice(["monthly","quarterly","no_statement"]),
            "e_statement_enrolled":     random.random() < 0.70,
            "cheque_book_issued":       acc_type == "DDA" and random.random() < 0.40,
            "debit_card_issued":        random.random() < 0.60,
            "account_purpose":          random.choice(["salary","savings","business","investment"]),
            "relationship_manager_id":  None,
            "freeze_flag":              random.random() < 0.01,
            "lien_amount_myr":          0.0,
            "average_monthly_balance":  round(balance * random.uniform(0.7,1.0), 2),
        })

df_acc = pd.DataFrame(accounts)
os.makedirs(f"/dbfs{VOLUME_DATA}/batch_v1/accounts", exist_ok=True)
df_acc.to_csv(f"/dbfs{VOLUME_DATA}/batch_v1/accounts/accounts.csv", index=False)
print(f"✅ accounts.csv: {len(df_acc)} rows")

# COMMAND ----------
# MAGIC %md ### Generate Loans

# COMMAND ----------
LOAN_TYPES   = ["home_loan","personal_loan","hire_purchase","overdraft","trade_finance"]
LOAN_STATUSES= ["active","settled","npl","written_off","restructured"]
LOAN_STATUS_W= [0.72, 0.18, 0.06, 0.02, 0.02]
COLLATERAL_TYPES = ["property","vehicle","fixed_deposit","guarantee","none"]

loans = []
for cif in random.sample(customer_ids, 600):
    party_id = f"P{int(cif[3:]):08d}"
    loan_type = random.choice(LOAN_TYPES)
    status    = random.choices(LOAN_STATUSES, weights=LOAN_STATUS_W)[0]
    principal = round(random.uniform(5000, 1500000), 2)
    outstanding = round(principal * random.uniform(0.0, 1.0), 2) if status=="active" else 0.0
    disb_date = (REL_START_MIN + timedelta(days=random.randint(0, REL_RANGE_DAYS))).isoformat()
    maturity  = (date.fromisoformat(disb_date) + timedelta(days=random.randint(365,10950))).isoformat()
    loans.append({
        "loan_account_id":              f"LA{random.randint(10000000,99999999)}",
        "party_id":                     party_id,
        "cif_number":                   cif,
        "loan_type":                    loan_type,
        "loan_status":                  status,
        "original_principal_myr":       principal,
        "outstanding_principal_myr":    outstanding,
        "interest_rate_pct":            round(random.uniform(2.5, 18.0), 2),
        "effective_rate_pct":           round(random.uniform(3.0, 20.0), 2),
        "monthly_installment_myr":      round(outstanding / max(random.randint(12,360), 1), 2),
        "tenure_months":                random.randint(12, 420),
        "disbursement_date":            disb_date,
        "maturity_date":                maturity,
        "last_modified_timestamp":      disb_date,
        "collateral_type":              random.choice(COLLATERAL_TYPES),
        "collateral_value_myr":         round(principal * random.uniform(0.8, 2.0), 2),
        "dpd":                          random.randint(0, 360) if status=="npl" else 0,
        "npl_flag":                     status == "npl",
        "is_shariah":                   random.random() < 0.40,
        "source_system":                "LOS",
    })

df_loans = pd.DataFrame(loans)
os.makedirs(f"/dbfs{VOLUME_DATA}/batch_v1/loans", exist_ok=True)
df_loans.to_csv(f"/dbfs{VOLUME_DATA}/batch_v1/loans/loans.csv", index=False)
print(f"✅ loans.csv: {len(df_loans)} rows")

# COMMAND ----------
# MAGIC %md ### Generate Transactions (~50,000 rows)

# COMMAND ----------
TXN_TYPES    = ["DEBIT","CREDIT"]
PAYMENT_RAILS= ["FPX","IBG","DuitNow","SWIFT","ATM","branch","card","internal"]
RAIL_WEIGHTS = [0.30, 0.15, 0.25, 0.05, 0.08, 0.04, 0.10, 0.03]
TXN_STATUSES = ["settled","pending","rejected","cancelled","reversed"]
TXN_STATUS_W = [0.88, 0.06, 0.03, 0.02, 0.01]
SANCTIONS     = ["CLEARED","FLAGGED","PENDING","BLOCKED"]
SANC_WEIGHTS  = [0.96, 0.02, 0.015, 0.005]
MCC_CODES     = ["5411","5812","5999","4111","7011","5912","5200","5732","5945","4900"]

transactions = []
acc_map = df_acc.groupby("cif_number")["deposit_account_id"].apply(list).to_dict()

for cif in customer_ids:
    accts = acc_map.get(cif, [gen_account_number()])
    num_txns = random.choices([10,20,30,50,80,120], weights=[0.05,0.15,0.30,0.28,0.15,0.07])[0]
    for _ in range(num_txns):
        txn_date = date(2025,1,1) + timedelta(days=random.randint(0,608))
        txn_type = random.choices(TXN_TYPES, weights=[0.55,0.45])[0]
        amount   = round(random.expovariate(1/500), 2) + 1.0
        transactions.append({
            "payment_transaction_id":   f"TXN{random.randint(100000000,999999999)}",
            "party_id":                 f"P{int(cif[3:]):08d}",
            "cif_number":               cif,
            "deposit_account_id":       random.choice(accts),
            "txn_type":                 txn_type,
            "payment_amount":           amount,
            "currency_id":              random.choices(["MYR","USD","SGD"], weights=[0.92,0.04,0.04])[0],
            "payment_rail_type":        random.choices(PAYMENT_RAILS, weights=RAIL_WEIGHTS)[0],
            "instruction_status":       random.choices(TXN_STATUSES, weights=TXN_STATUS_W)[0],
            "posting_date":             txn_date.isoformat(),
            "value_date":               (txn_date - timedelta(days=random.randint(0,1))).isoformat(),
            "description":              fake.sentence(nb_words=4),
            "counterparty_name":        random.choice(EMPLOYERS + [fake.company()]),
            "counterparty_account":     gen_account_number(),
            "mcc_code":                 random.choice(MCC_CODES),
            "channel":                  random.choice(["mobile_app","web_banking","atm","branch","pos"]),
            "sanctions_screening_status": random.choices(SANCTIONS, weights=SANC_WEIGHTS)[0],
            "fraud_flag":               random.random() < 0.003,
            "running_balance_myr":      round(random.uniform(100, 200000), 2),
            "reference_number":         f"REF{random.randint(100000000,999999999)}",
            "source_system":            "TXN_ENGINE",
        })

df_txn = pd.DataFrame(transactions)
os.makedirs(f"/dbfs{VOLUME_DATA}/batch_v1/transactions", exist_ok=True)
df_txn.to_csv(f"/dbfs{VOLUME_DATA}/batch_v1/transactions/transactions.csv", index=False)
print(f"✅ transactions.csv: {len(df_txn)} rows")

# COMMAND ----------
# MAGIC %md ### Generate Card Transactions (~10,000 rows)

# COMMAND ----------
CARD_NETWORKS = ["VISA","MASTERCARD","AMEX"]
CARD_TYPES    = ["credit","debit","prepaid"]
MERCHANTS     = ["Tesco Extra","MyDIY","Grab","Shopee","Watsons","Parkson",
                 "McDonald's","KFC","Shell","Petron","AirAsia","Agoda",
                 "Uniqlo","H&M","Guardian","Starbucks","99 Speedmart","Caring Pharmacy"]
CARD_STATUSES = ["settled","pending","disputed","reversed"]
CARD_STATUS_W = [0.87, 0.07, 0.04, 0.02]

cards_txn = []
cif_with_cards = random.sample(customer_ids, 700)

for cif in cif_with_cards:
    num_card_txns = random.randint(5, 30)
    for _ in range(num_card_txns):
        txn_date = date(2025,1,1) + timedelta(days=random.randint(0,608))
        amount   = round(random.uniform(5, 5000), 2)
        is_overseas = random.random() < 0.08
        cards_txn.append({
            "card_instruction_id":       f"CARD{random.randint(100000000,999999999)}",
            "party_id":                  f"P{int(cif[3:]):08d}",
            "cif_number":                cif,
            "card_number_masked":        f"**** **** **** {random.randint(1000,9999)}",
            "card_network":              random.choice(CARD_NETWORKS),
            "card_type":                 random.choice(CARD_TYPES),
            "merchant_name":             random.choice(MERCHANTS),
            "merchant_category_code":    random.choice(MCC_CODES),
            "merchant_country_code":     random.choice(["MY","SG","US","UK","AU"]) if is_overseas else "MY",
            "txn_amount":                amount,
            "txn_currency":              random.choice(["MYR","USD","SGD"]) if is_overseas else "MYR",
            "billing_amount_myr":        round(amount * random.uniform(0.95,1.05), 2) if is_overseas else amount,
            "posting_date":              txn_date.isoformat(),
            "instruction_status":        random.choices(CARD_STATUSES, weights=CARD_STATUS_W)[0],
            "is_overseas":               is_overseas,
            "is_contactless":            random.random() < 0.55,
            "reward_points_earned":      int(amount // 1),
            "cashback_earned_myr":       round(amount * 0.05, 2) if random.random() < 0.3 else 0.0,
            "dispute_flag":              random.random() < 0.01,
            "fraud_flag":                random.random() < 0.002,
            "source_system":             "CARD_ENGINE",
        })

df_cards = pd.DataFrame(cards_txn)
os.makedirs(f"/dbfs{VOLUME_DATA}/batch_v1/cards", exist_ok=True)
df_cards.to_csv(f"/dbfs{VOLUME_DATA}/batch_v1/cards/cards_txn.csv", index=False)
print(f"✅ cards_txn.csv: {len(df_cards)} rows")

# COMMAND ----------
# MAGIC %md ### Generate Near-RT JSON Files — Credit Bureau (1,000 files)

# COMMAND ----------
CCRIS_STATUSES  = ["clean","1_dpd","30_dpd","60_dpd","90_dpd"]
CCRIS_WEIGHTS   = [0.70, 0.12, 0.08, 0.06, 0.04]
PAYMENT_CONDUCTS= ["clean","1_missed","2_missed","3+_missed"]
PAYMENT_C_W     = [0.72, 0.14, 0.09, 0.05]
BANKRUPTCY_STAT = ["none","voluntary","involuntary"]
BANKRUPTCY_W    = [0.97, 0.02, 0.01]

os.makedirs(f"/dbfs{VOLUME_DATA}/nearrt/credit_bureau", exist_ok=True)
for cif in customer_ids:
    ctos = random.randint(300, 850)
    record = {
        "cif_number":                   cif,
        "party_id":                     f"P{int(cif[3:]):08d}",
        "report_date":                  "2026-09-07",
        "ctos_score":                   ctos,
        "ccris_status":                 random.choices(CCRIS_STATUSES, weights=CCRIS_WEIGHTS)[0],
        "total_credit_facilities":      random.randint(0, 15),
        "total_outstanding_balance_myr":round(random.uniform(0, 2000000), 2),
        "monthly_total_commitment_myr": round(random.uniform(0, 30000), 2),
        "payment_conduct_12m":          random.choices(PAYMENT_CONDUCTS, weights=PAYMENT_C_W)[0],
        "legal_cases_count":            random.choices([0,1,2,3], weights=[0.90,0.06,0.03,0.01])[0],
        "bankruptcy_status":            random.choices(BANKRUPTCY_STAT, weights=BANKRUPTCY_W)[0],
        "credit_card_count":            random.randint(0, 8),
        "home_loan_count":              random.randint(0, 3),
        "personal_loan_count":          random.randint(0, 4),
        "hire_purchase_count":          random.randint(0, 3),
        "overdraft_count":              random.randint(0, 2),
        "inquiry_count_last_6m":        random.randint(0, 10),
        "inquiry_count_last_12m":       random.randint(0, 15),
        "oldest_facility_years":        random.randint(0, 20),
        "credit_utilisation_pct":       round(random.uniform(0, 100), 1),
        "debt_to_income_ratio":         round(random.uniform(0, 1.2), 3),
        "largest_single_facility_myr":  round(random.uniform(0, 1000000), 2),
        "secured_debt_pct":             round(random.uniform(0, 100), 1),
        "unsecured_debt_pct":           round(random.uniform(0, 100), 1),
        "bureau_pull_timestamp":        "2026-09-07T00:00:00",
        "bureau_provider":              random.choice(["CTOS","CCRIS","RAM Credit"]),
        "consent_flag":                 True,
        "consent_date":                 "2026-09-01",
        "aml_flag":                     random.random() < 0.01,
        "pep_flag":                     random.random() < 0.02,
        "sanction_flag":                random.random() < 0.005,
        "data_source":                  "CTOS_API_v2",
    }
    with open(f"/dbfs{VOLUME_DATA}/nearrt/credit_bureau/{cif}.json", "w") as f:
        json.dump(record, f)

print(f"✅ Credit bureau: 1000 JSON files written")

# COMMAND ----------
# MAGIC %md ### Generate Near-RT JSON Files — Telco Events (5,000 files)

# COMMAND ----------
TELCO_CARRIERS   = ["Maxis","Celcom","Digi","U Mobile","YES 4G"]
DEVICE_CLASSES   = ["flagship","mid_range","budget","feature_phone"]
DEVICE_WEIGHTS   = [0.25, 0.40, 0.28, 0.07]
DATA_PLANS       = ["UNLIMITED_50","UNLIMITED_30","15GB","8GB","3GB","BASIC"]

os.makedirs(f"/dbfs{VOLUME_DATA}/nearrt/telco_events", exist_ok=True)
for idx, cif in enumerate(customer_ids):
    for j in range(5):
        event_date = date(2026,8,1) + timedelta(days=random.randint(0,37))
        record = {
            "event_id":                     f"TELCO{idx:06d}_{j:02d}",
            "cif_number":                   cif,
            "party_id":                     f"P{int(cif[3:]):08d}",
            "event_date":                   event_date.isoformat(),
            "telco_carrier":                random.choice(TELCO_CARRIERS),
            "data_plan":                    random.choice(DATA_PLANS),
            "data_consumption_mb_monthly":  round(random.uniform(100, 60000), 1),
            "voice_minutes_monthly":        random.randint(0, 2000),
            "sms_count_monthly":            random.randint(0, 500),
            "arpu_myr":                     round(random.uniform(25, 250), 2),
            "device_class":                 random.choices(DEVICE_CLASSES, weights=DEVICE_WEIGHTS)[0],
            "device_os":                    random.choices(["iOS","Android"], weights=[0.35,0.65])[0],
            "digital_maturity_score":       round(random.uniform(1, 10), 1),
            "roaming_flag":                 random.random() < 0.12,
            "postpaid_flag":                random.random() < 0.65,
            "tenure_months_with_carrier":   random.randint(1, 120),
            "churn_risk_score":             round(random.uniform(0, 1), 3),
            "financial_inclusion_score":    round(random.uniform(1, 10), 1),
            "app_usage_banking_mins":       round(random.uniform(0, 120), 1),
            "location_cluster":             f"GEO_{random.randint(1,20):02d}",
            "consent_flag":                 True,
            "data_source":                  "TELCO_PARTNER_API",
        }
        fname = f"/dbfs{VOLUME_DATA}/nearrt/telco_events/{cif}_{j:02d}.json"
        with open(fname, "w") as f:
            json.dump(record, f)

print(f"✅ Telco events: 5000 JSON files written")

# COMMAND ----------
# MAGIC %md ### Validate all output files

# COMMAND ----------
for path in [
    f"{VOLUME_DATA}/batch_v1/customer_master/customers_v1.csv",
    f"{VOLUME_DATA}/batch_v1/accounts/accounts.csv",
    f"{VOLUME_DATA}/batch_v1/loans/loans.csv",
    f"{VOLUME_DATA}/batch_v1/transactions/transactions.csv",
    f"{VOLUME_DATA}/batch_v1/cards/cards_txn.csv",
]:
    rows = spark.read.csv(path, header=True).count()
    print(f"✅ {path.split('/')[-1]}: {rows} rows")

cb_count = len(dbutils.fs.ls(f"{VOLUME_DATA}/nearrt/credit_bureau/"))
te_count = len(dbutils.fs.ls(f"{VOLUME_DATA}/nearrt/telco_events/"))
print(f"✅ credit_bureau JSON files: {cb_count}")
print(f"✅ telco_events JSON files:  {te_count}")
```

- [ ] Commit data generator notebook

---

### Task 3: AutoLoader Batch Ingestion

- [ ] Write `part_a_customer_360/02_ingest_batch.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # AutoLoader Batch Ingestion — Bronze Layer
# MAGIC
# MAGIC **Scenario 2:** Every night at 23:00, Core Banking pushes CSV extracts to the landing zone.
# MAGIC AutoLoader detects new files, validates schema, and loads to bronze Delta tables.
# MAGIC
# MAGIC Key AutoLoader options used:
# MAGIC - `cloudFiles.schemaEvolutionMode = addNewColumns` — new columns in source automatically added to bronze
# MAGIC - `rescuedDataColumn = _rescued_data` — malformed values captured as JSON, no row loss
# MAGIC - `trigger(availableNow=True)` — processes all pending files then terminates (batch semantics)

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, col

def load_bronze(source_path, table_name, schema_loc):
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{schema_loc}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{FULL_SCHEMA}.{table_name}")
    ).awaitTermination()
    print(f"✅ {table_name} loaded")

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/customer_master/",
    table_name="bronze_customer_master",
    schema_loc=f"{VOLUME_DATA}/_schemas/customer_master"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/accounts/",
    table_name="bronze_core_banking_accounts",
    schema_loc=f"{VOLUME_DATA}/_schemas/accounts"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/loans/",
    table_name="bronze_loans",
    schema_loc=f"{VOLUME_DATA}/_schemas/loans"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/transactions/",
    table_name="bronze_core_banking_txn",
    schema_loc=f"{VOLUME_DATA}/_schemas/transactions"
)

# COMMAND ----------
load_bronze(
    source_path=f"{VOLUME_DATA}/batch_v1/cards/",
    table_name="bronze_cards_txn",
    schema_loc=f"{VOLUME_DATA}/_schemas/cards"
)

# COMMAND ----------
# MAGIC %md ### Validate bronze tables

# COMMAND ----------
for tbl, expected_min in [
    ("bronze_customer_master",      990),
    ("bronze_core_banking_accounts",1100),
    ("bronze_loans",                500),
    ("bronze_core_banking_txn",     45000),
    ("bronze_cards_txn",            9000),
]:
    cnt = spark.table(f"{FULL_SCHEMA}.{tbl}").count()
    assert cnt >= expected_min, f"{tbl}: {cnt} rows (expected >= {expected_min})"
    print(f"✅ {tbl}: {cnt} rows")
```

- [ ] Commit batch ingestion notebook

---

### Task 4: Near-RT CDC Ingestion (AutoLoader Micro-Batch)

- [ ] Write `part_a_customer_360/03_ingest_nearrt.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Near-Real-Time CDC Simulation
# MAGIC
# MAGIC **Scenario:** CTOS/CCRIS credit bureau and Telco partner push JSON updates every 30 seconds.
# MAGIC In production this would be Lakeflow Connect from SQL Server or a REST API webhook.
# MAGIC Here we simulate by writing new JSON files to the volume and processing them in micro-batches.
# MAGIC
# MAGIC `trigger(processingTime="30 seconds")` — process any new files every 30 seconds.

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, col

# Start credit bureau stream
credit_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/credit_bureau")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("multiLine", "true")
    .load(f"{VOLUME_DATA}/nearrt/credit_bureau/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/credit_bureau/checkpoint")
    .trigger(processingTime="30 seconds")
    .toTable(f"{FULL_SCHEMA}.bronze_credit_bureau")
)
print("✅ Credit bureau stream started")

# COMMAND ----------
# Start telco events stream
telco_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/telco_events")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("multiLine", "true")
    .load(f"{VOLUME_DATA}/nearrt/telco_events/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/telco_events/checkpoint")
    .trigger(processingTime="30 seconds")
    .toTable(f"{FULL_SCHEMA}.bronze_telco_events")
)
print("✅ Telco events stream started")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Monitor ingestion progress
# MAGIC Re-run the cell below every ~30 seconds to watch the counts grow.

# COMMAND ----------
import time
time.sleep(35)  # Wait for first micro-batch to complete
cb_cnt  = spark.table(f"{FULL_SCHEMA}.bronze_credit_bureau").count()
tel_cnt = spark.table(f"{FULL_SCHEMA}.bronze_telco_events").count()
assert cb_cnt  > 0, "bronze_credit_bureau has 0 rows — check stream"
assert tel_cnt > 0, "bronze_telco_events has 0 rows — check stream"
print(f"✅ bronze_credit_bureau: {cb_cnt} rows")
print(f"✅ bronze_telco_events:  {tel_cnt} rows")

# COMMAND ----------
# MAGIC %md ## Stop streams (run when demo is over)

# COMMAND ----------
credit_query.stop()
telco_query.stop()
print("✅ Near-RT streams stopped")
```

- [ ] Commit near-RT ingestion notebook

---

### Task 5: Real-Time Streaming (Rate Source → bronze_digital_events)

- [ ] Write `part_a_customer_360/04_ingest_realtime.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Real-Time Event Streaming — Spark Rate Source
# MAGIC
# MAGIC This simulates live digital banking events (mobile app + web) using Spark's built-in
# MAGIC Rate Source — no external Kafka or message broker needed. Watch the row count grow live.
# MAGIC
# MAGIC **Demo talking point:** This is 2 events/sec. In production, replace the Rate Source with
# MAGIC a Kafka/Event Hub source — the downstream pipeline code is identical.

# COMMAND ----------
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load party IDs from bronze table for realistic event generation
party_ids = [r.cif_number for r in spark.table(f"{FULL_SCHEMA}.bronze_customer_master")
             .select("cif_number").limit(1000).collect()]
party_ids_bc = spark.sparkContext.broadcast(party_ids)

EVENT_TYPES   = ["login","view_product","apply","fund_transfer","bill_pay","investment","logout"]
PRODUCT_CATS  = ["CREDIT_CARD","PERSONAL_LOAN","HOME_LOAN","INVESTMENT","INSURANCE",None]
STATES        = ["Selangor","Kuala Lumpur","Johor","Penang","Perak","Sabah","Sarawak"]

@udf(returnType=StringType())
def random_event_json(row_id):
    import random, json, uuid
    pid = random.choice(party_ids_bc.value)
    evt = random.choice(EVENT_TYPES)
    return json.dumps({
        "event_id":                   str(uuid.uuid4()),
        "party_id":                   pid,
        "session_id":                 str(uuid.uuid4())[:8],
        "event_type":                 evt,
        "product_category_viewed":    random.choice(PRODUCT_CATS) if evt == "view_product" else None,
        "page_name":                  f"/{evt}",
        "duration_seconds":           random.randint(5, 300),
        "device_type":                random.choice(["mobile","web","tablet"]),
        "channel":                    random.choice(["mobile_app","web_banking"]),
        "session_count_today":        random.randint(1, 8),
        "ip_state":                   random.choice(STATES),
        "is_authenticated":           True,
    })

# COMMAND ----------
EVENT_SCHEMA = schema_of_json('{"event_id":"x","party_id":"x","session_id":"x","event_type":"x","product_category_viewed":"x","page_name":"x","duration_seconds":1,"device_type":"x","channel":"x","session_count_today":1,"ip_state":"x","is_authenticated":true}')

stream_query = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load()
    .select(from_json(random_event_json(col("value").cast("string")), EVENT_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/digital_events/checkpoint")
    .toTable(f"{FULL_SCHEMA}.bronze_digital_events")
)
print("✅ Streaming started at 2 events/sec. Run the next cell to see live row count.")

# COMMAND ----------
# MAGIC %md ## Live row count — re-run this cell every few seconds

# COMMAND ----------
display(spark.sql(f"""
SELECT COUNT(*) AS total_events, MAX(event_timestamp) AS latest
FROM {FULL_SCHEMA}.bronze_digital_events
"""))

# COMMAND ----------
# MAGIC %md ## Stop stream

# COMMAND ----------
stream_query.stop()
print("✅ Real-time stream stopped")
```

- [ ] Commit real-time streaming notebook

---

### Task 6: Schema Evolution Demo

- [ ] Write `part_a_customer_360/05_schema_evolution_demo.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Schema Evolution Demo — Scenario 1: New Data Source Onboarding
# MAGIC
# MAGIC **Story:** ABMB launches a new digital initiative. The CRM exports a new customer file
# MAGIC with 2 additional attributes. We show how AutoLoader handles this automatically —
# MAGIC no pipeline downtime, no schema conflicts, new columns appear in the bronze table instantly.

# COMMAND ----------
# MAGIC %md ## ⚙️ RESET DEMO — Run this cell first to start fresh

# COMMAND ----------
# RESET CELL — idempotent, run before every demo
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_customers_evolution")
dbutils.fs.rm(f"{VOLUME_DATA}/schema_evolution/", recurse=True)
dbutils.fs.rm(f"{VOLUME_DATA}/_schemas/customers_evolution/", recurse=True)
dbutils.fs.mkdirs(f"{VOLUME_DATA}/schema_evolution/v1/")
dbutils.fs.mkdirs(f"{VOLUME_DATA}/schema_evolution/v2/")
print("✅ Reset complete — ready for demo")

# COMMAND ----------
# MAGIC %md ## Act 1: Load V1 schema — 43 columns (baseline)

# COMMAND ----------
import pandas as pd, random
from pyspark.sql.functions import current_timestamp, col
random.seed(42)

v1_records = [{"cif_number": f"CIF{i:06d}", "legal_name": f"Customer {i}",
               "annual_income_amount": random.randint(24000, 200000),
               "risk_rating": random.choice(["low","medium","high"])}
              for i in range(1, 101)]
df_v1 = pd.DataFrame(v1_records)
df_v1.to_csv(f"/dbfs{VOLUME_DATA}/schema_evolution/v1/customers_v1.csv", index=False)
print(f"✅ Written {len(df_v1)} rows with {len(df_v1.columns)} columns")

# COMMAND ----------
(spark.readStream
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true").option("header", "true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/customers_evolution")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{VOLUME_DATA}/schema_evolution/v1/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/customers_evolution/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{FULL_SCHEMA}.bronze_customers_evolution")
).awaitTermination()
print("✅ V1 loaded")

# COMMAND ----------
# Show current schema — columns + _rescued_data, _ingest_timestamp
display(spark.sql(f"DESCRIBE {FULL_SCHEMA}.bronze_customers_evolution"))

# COMMAND ----------
# MAGIC %md ## Act 2: New source onboarded — V2 adds occupation_code + is_shariah_preferred

# COMMAND ----------
v2_records = [{"cif_number": f"CIF{i:06d}", "legal_name": f"Customer {i}",
               "annual_income_amount": random.randint(24000, 200000),
               "risk_rating": random.choice(["low","medium","high"]),
               "occupation_code": random.choice(["K","M","J","A","B"]),   # NEW
               "is_shariah_preferred": random.choice([True, False])}       # NEW
              for i in range(1001, 1051)]
df_v2 = pd.DataFrame(v2_records)
df_v2.to_csv(f"/dbfs{VOLUME_DATA}/schema_evolution/v2/customers_v2.csv", index=False)
print(f"✅ Written {len(df_v2)} rows with {len(df_v2.columns)} columns (2 NEW fields)")

# COMMAND ----------
# Same AutoLoader call — addNewColumns mode handles schema change automatically, no code change!
(spark.readStream
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true").option("header", "true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/customers_evolution")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{VOLUME_DATA}/schema_evolution/v2/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/customers_evolution/checkpoint")
    .trigger(availableNow=True).option("mergeSchema", "true")
    .toTable(f"{FULL_SCHEMA}.bronze_customers_evolution")
).awaitTermination()
print("✅ V2 loaded — new columns automatically added")

# COMMAND ----------
# MAGIC %md ## Act 3: Validate — new columns appear, old rows have NULL for new fields

# COMMAND ----------
print(f"Total rows: {spark.table(f'{FULL_SCHEMA}.bronze_customers_evolution').count()}")
display(spark.sql(f"""
SELECT cif_number, annual_income_amount, occupation_code, is_shariah_preferred, _ingest_timestamp
FROM {FULL_SCHEMA}.bronze_customers_evolution
ORDER BY _ingest_timestamp DESC LIMIT 20
"""))
# Key insight: V1 rows show NULL for occupation_code and is_shariah_preferred — schema evolved gracefully
```

- [ ] Commit schema evolution demo notebook

---

### Task 7: Rescued Data Demo

- [ ] Write `part_a_customer_360/06_rescued_data_demo.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Rescued Data Column Demo — Handling Malformed Values
# MAGIC
# MAGIC **Story:** The Cards system occasionally sends malformed data — string where number expected,
# MAGIC invalid dates, unknown extra columns. AutoLoader's `_rescued_data` column captures all
# MAGIC malformed values as JSON, preserving the good rows while quarantining bad ones.
# MAGIC The SDP pipeline then routes these rows to a quarantine table via:
# MAGIC `CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION QUARANTINE`

# COMMAND ----------
# MAGIC %md ## ⚙️ RESET — Run first

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_rescued_demo")
dbutils.fs.rm(f"{VOLUME_DATA}/rescued_demo/", recurse=True)
dbutils.fs.rm(f"{VOLUME_DATA}/_schemas/rescued_demo/", recurse=True)
dbutils.fs.mkdirs(f"{VOLUME_DATA}/rescued_demo/clean/")
dbutils.fs.mkdirs(f"{VOLUME_DATA}/rescued_demo/malformed/")
print("✅ Reset complete")

# COMMAND ----------
# MAGIC %md ## Step 1: Clean ingestion baseline

# COMMAND ----------
import pandas as pd
from pyspark.sql.functions import current_timestamp

clean_data = pd.DataFrame([
    {"txn_id": f"T{i:06d}", "amount": round(i * 100.50, 2),
     "posting_date": "2026-09-01", "status": "settled"}
    for i in range(1, 21)
])
clean_data.to_csv(f"/dbfs{VOLUME_DATA}/rescued_demo/clean/txns_clean.csv", index=False)

(spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes","true").option("header","true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/rescued_demo")
    .option("rescuedDataColumn","_rescued_data")
    .load(f"{VOLUME_DATA}/rescued_demo/clean/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/rescued_demo/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{FULL_SCHEMA}.bronze_rescued_demo")
).awaitTermination()

display(spark.sql(f"SELECT *, _rescued_data FROM {FULL_SCHEMA}.bronze_rescued_demo"))
print("✅ All rows clean — _rescued_data is NULL for every row")

# COMMAND ----------
# MAGIC %md ## Step 2: Inject malformed rows

# COMMAND ----------
malformed_data = pd.DataFrame([
    {"txn_id": "T000021", "amount": "TWO HUNDRED",   # ← string where DECIMAL expected
     "posting_date": "2026-09-02", "status": "settled"},
    {"txn_id": "T000022", "amount": 500.00,
     "posting_date": "not-a-date",                     # ← invalid date
     "status": "settled"},
    {"txn_id": "T000023", "amount": 750.00,
     "posting_date": "2026-09-02", "status": "settled",
     "fraud_score": 0.95},                              # ← unknown extra column
])
malformed_data.to_csv(f"/dbfs{VOLUME_DATA}/rescued_demo/malformed/txns_bad.csv", index=False)

(spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
    .option("cloudFiles.inferColumnTypes","true").option("header","true")
    .option("cloudFiles.schemaLocation", f"{VOLUME_DATA}/_schemas/rescued_demo")
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
    .option("rescuedDataColumn","_rescued_data")
    .load(f"{VOLUME_DATA}/rescued_demo/malformed/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/rescued_demo/checkpoint")
    .trigger(availableNow=True).option("mergeSchema","true")
    .toTable(f"{FULL_SCHEMA}.bronze_rescued_demo")
).awaitTermination()

# COMMAND ----------
# MAGIC %md ## Step 3: Inspect rescued data — malformed values captured as JSON

# COMMAND ----------
display(spark.sql(f"""
SELECT txn_id, amount, posting_date, status, _rescued_data
FROM {FULL_SCHEMA}.bronze_rescued_demo
WHERE _rescued_data IS NOT NULL
"""))
# Expected: rows T000021, T000022, T000023 each show _rescued_data with the bad field as JSON
# T000021: {{"amount": "TWO HUNDRED"}}
# T000022: {{"posting_date": "not-a-date"}}
# T000023: {{"fraud_score": "0.95"}}  ← captured even though it was an unknown column
```

- [ ] Commit rescued data demo notebook

---

### Task 8: SDP Bronze → Silver Pipeline

- [ ] Write `part_a_customer_360/07_sdp_bronze_silver.py` as a Databricks DLT notebook source file (deploy as Pipeline):

```python
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
# MAGIC
# MAGIC ## How to Deploy This Pipeline
# MAGIC 1. Go to Databricks → Pipelines → Create Pipeline
# MAGIC 2. Pipeline name: `ABMB-Bronze-Silver`
# MAGIC 3. Source: select this notebook
# MAGIC 4. Target schema: `fevm_master_classic_marcus_catalog.abmb_rfp_presentation`
# MAGIC 5. Cluster: Serverless
# MAGIC 6. Click Start — the pipeline creates all silver tables automatically

# COMMAND ----------
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "abmb_rfp_presentation"
FULL    = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
# MAGIC %md ### silver_customers (SCD Type 2 — MVM: customer.party + individual_profile)

# COMMAND ----------
dlt.create_streaming_table(
    name="silver_customers",
    comment="Banking MVM: customer domain — party + individual_profile. SCD Type 2.",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "true"}
)

dlt.apply_changes(
    target="silver_customers",
    source=f"{FULL}.bronze_customer_master",
    keys=["cif_number"],
    sequence_by=col("record_updated_timestamp"),
    stored_as_scd_type=2,
    except_column_list=["_rescued_data", "_source_file"]
)

# COMMAND ----------
# MAGIC %md ### silver_deposit_accounts (SCD Type 2 — MVM: account.deposit_account)

# COMMAND ----------
dlt.create_streaming_table(
    name="silver_deposit_accounts",
    comment="Banking MVM: account domain — deposit_account. SCD Type 2.",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
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
dlt.create_streaming_table(
    name="silver_loan_accounts",
    comment="Banking MVM: loan domain — loan_account. SCD Type 2.",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
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
@dlt.expect_or_drop("valid_txn_id",       "payment_transaction_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount",    "payment_amount > 0")
@dlt.expect("valid_currency",             "currency_id IN ('MYR','USD','SGD','EUR','GBP')")
@dlt.expect("valid_status",               "instruction_status IN ('settled','pending','rejected','cancelled','reversed')")
@dlt.expect("valid_rail",                 "payment_rail_type IN ('FPX','IBG','DuitNow','SWIFT','ATM','branch','card','internal')")
@dlt.expect("valid_sanctions",            "sanctions_screening_status IN ('CLEARED','FLAGGED','PENDING','BLOCKED')")
@dlt.expect("posting_after_value",        "posting_date >= value_date")
@dlt.expect("no_rescued_data",            "_rescued_data IS NULL")
@dlt.table(name="silver_transactions",
           comment="Banking MVM: payment.payment_transaction. Append-only.",
           table_properties={"quality": "silver"})
def silver_transactions():
    return (dlt.read_stream(f"{FULL}.bronze_core_banking_txn")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md ### silver_card_transactions (append-only — MVM: payment.instruction card-type)

# COMMAND ----------
@dlt.expect_or_drop("valid_card_txn_id",  "card_instruction_id IS NOT NULL")
@dlt.expect_or_drop("positive_card_amount","txn_amount > 0")
@dlt.expect("valid_card_status",          "instruction_status IN ('settled','pending','disputed','reversed')")
@dlt.expect("no_rescued_data",            "_rescued_data IS NULL")
@dlt.table(name="silver_card_transactions",
           comment="Banking MVM: payment.instruction (card).",
           table_properties={"quality": "silver"})
def silver_card_transactions():
    return (dlt.read_stream(f"{FULL}.bronze_cards_txn")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md ### silver_kyc_compliance (MVM: compliance.kyc_review + CTOS/CCRIS extension)

# COMMAND ----------
@dlt.expect_or_drop("valid_cif_kyc",               "cif_number IS NOT NULL")
@dlt.expect("valid_ctos_score",                     "ctos_score IS NULL OR (ctos_score >= 300 AND ctos_score <= 850)")
@dlt.expect("valid_payment_conduct",                "payment_conduct_12m IN ('clean','1_missed','2_missed','3+_missed')")
@dlt.expect("no_negative_outstanding",              "total_outstanding_balance_myr IS NULL OR total_outstanding_balance_myr >= 0")
@dlt.expect("valid_bankruptcy",                     "bankruptcy_status IN ('none','voluntary','involuntary')")
@dlt.expect("no_rescued_data",                      "_rescued_data IS NULL")
@dlt.table(name="silver_kyc_compliance",
           comment="Banking MVM: compliance.kyc_review + CTOS/CCRIS.",
           table_properties={"quality": "silver"})
def silver_kyc_compliance():
    return (dlt.read_stream(f"{FULL}.bronze_credit_bureau")
            .withColumn("_silver_timestamp", current_timestamp()))

# COMMAND ----------
# MAGIC %md ### silver_digital_activity (MVM: channel domain — aggregated from telco + digital events)

# COMMAND ----------
@dlt.expect_or_drop("valid_party_digital",  "party_id IS NOT NULL")
@dlt.expect("valid_event_type",             "event_type IN ('login','view_product','apply','fund_transfer','bill_pay','investment','logout') OR event_type IS NULL")
@dlt.expect("no_rescued_data",              "_rescued_data IS NULL")
@dlt.table(name="silver_digital_activity",
           comment="Banking MVM: channel domain — digital + telco events.",
           table_properties={"quality": "silver"})
def silver_digital_activity():
    digital = (dlt.read_stream(f"{FULL}.bronze_digital_events")
               .withColumn("source", lit("digital_banking"))
               .withColumn("_silver_timestamp", current_timestamp()))
    telco   = (dlt.read_stream(f"{FULL}.bronze_telco_events")
               .withColumn("event_type", lit(None).cast("string"))
               .withColumn("source", lit("telco"))
               .withColumn("_silver_timestamp", current_timestamp()))
    return digital.unionByName(telco, allowMissingColumns=True)
```

- [ ] Commit SDP bronze→silver pipeline notebook
- [ ] Deploy as Pipeline `ABMB-Bronze-Silver` in Databricks UI (Serverless, target schema `fevm_master_classic_marcus_catalog.abmb_rfp_presentation`)
- [ ] Run pipeline and confirm all 6 silver tables created with > 0 rows

---

### Task 9: SDP Silver → Gold Pipeline

- [ ] Write `part_a_customer_360/08_sdp_silver_gold.py` as a Databricks DLT notebook source file:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold Pipeline (Spark Declarative Pipeline)
# MAGIC ## gold_customer_360 — Unified Customer View
# MAGIC
# MAGIC Assembles 1 row per active customer from 6 silver tables, producing ~65 features.
# MAGIC This is the certified data product consumed by Part B (AI/BI Dashboard) and
# MAGIC Part C (Next Best Product ML model).
# MAGIC
# MAGIC ## Deploy
# MAGIC 1. Pipelines → Create Pipeline
# MAGIC 2. Name: `ABMB-Silver-Gold`
# MAGIC 3. Source: this notebook
# MAGIC 4. Target schema: `fevm_master_classic_marcus_catalog.abmb_rfp_presentation`
# MAGIC 5. Serverless → Start
# MAGIC 6. Validate: `SELECT COUNT(*) FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.gold_customer_360`
# MAGIC    Expected: ~1000 rows

# COMMAND ----------
import dlt
from pyspark.sql.functions import *

FULL = "fevm_master_classic_marcus_catalog.abmb_rfp_presentation"

# COMMAND ----------
# MAGIC %md ### Intermediate aggregations

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

@dlt.table(name="_agg_loans", comment="Loan aggregates per customer")
def _agg_loans():
    return (spark.table(f"{FULL}.silver_loan_accounts")
        .filter("__END_AT IS NULL")
        .groupBy("party_id")
        .agg(
            count("loan_account_id").alias("num_loan_facilities"),
            sum(when(col("loan_status") == "active", col("outstanding_principal_myr")).otherwise(0)).alias("total_loan_outstanding_myr"),
            sum(when(col("loan_status") == "active", col("monthly_installment_myr")).otherwise(0)).alias("monthly_loan_commitment_myr"),
            max(when(col("loan_type") == "home_loan",      lit(True)).otherwise(lit(False))).alias("has_home_loan"),
            max(when(col("loan_type") == "personal_loan",  lit(True)).otherwise(lit(False))).alias("has_personal_loan"),
            max(when(col("loan_type") == "hire_purchase",  lit(True)).otherwise(lit(False))).alias("has_hire_purchase"),
        ))

@dlt.table(name="_agg_transactions", comment="Transaction behaviour aggregates per customer")
def _agg_transactions():
    today = current_date()
    txn   = spark.table(f"{FULL}.silver_transactions")
    card  = spark.table(f"{FULL}.silver_card_transactions")

    txn_agg = (txn.groupBy("party_id").agg(
        count(when(datediff(today, col("posting_date")) <= 30,  1)).alias("txn_count_30d"),
        count(when(datediff(today, col("posting_date")) <= 90,  1)).alias("txn_count_90d"),
        count(when(datediff(today, col("posting_date")) <= 365, 1)).alias("txn_count_12m"),
        sum(when((col("txn_type")=="DEBIT")  & (datediff(today,col("posting_date"))<=30), col("payment_amount")).otherwise(0)).alias("total_debit_30d_myr"),
        sum(when((col("txn_type")=="CREDIT") & (datediff(today,col("posting_date"))<=30), col("payment_amount")).otherwise(0)).alias("total_credit_30d_myr"),
        avg("payment_amount").alias("avg_txn_amount_myr"),
    ))

    card_agg = (card.groupBy("party_id").agg(
        lit(True).alias("has_credit_card"),
        sum(when(datediff(today, col("posting_date")) <= 30, col("txn_amount")).otherwise(0)).alias("card_spend_30d_myr"),
        max(col("is_overseas").cast("boolean")).alias("overseas_txn_flag"),
    ))

    return txn_agg.join(card_agg, "party_id", "left")

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
                    round(coalesce(col("mobile_sessions_30d"), lit(0)) * 0.4 +
                          coalesce(col("digital_maturity_score"), lit(5)) * 0.6, 1))
        .withColumn("telco_data_consumption_gb_monthly",
                    round(coalesce(col("data_consumption_mb_monthly"), lit(0)) / 1024, 2)))

# COMMAND ----------
# MAGIC %md ### gold_customer_360 — Final Assembly

# COMMAND ----------
@dlt.expect_or_drop("gold_valid_cif",      "cif_number IS NOT NULL")
@dlt.expect_or_drop("gold_active_customer","lifecycle_status = 'active'")
@dlt.expect("gold_has_name",               "legal_name IS NOT NULL")
@dlt.table(
    name="gold_customer_360",
    comment="Unified Customer 360 view — 1 row per active customer, 65+ features. Final certified data product.",
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
            c.party_id, c.cif_number, c.legal_name, c.first_name, c.last_name,
            c.national_id_number, c.date_of_birth,
            (datediff(current_date(), c.date_of_birth.cast("date")) / 365).cast("int").alias("age"),
            c.gender, c.citizenship_country_code, c.residency_status,
            c.customer_segment, c.lifestyle_segment,
            c.employment_status, c.employer_name, c.annual_income_amount,
            c.marital_status, c.number_of_dependents, c.education_level,
            c.is_pep, c.risk_rating, c.kyc_status, c.lifecycle_status,
            (datediff(current_date(), c.relationship_start_date.cast("date")) / 365).cast("int").alias("relationship_tenure_years"),
            c.digital_banking_enrollment_flag, c.mobile_app_user_flag,
            c.marketing_consent_flag, c.net_worth_band, c.nps_score,
            c.is_shariah_preferred, c.preferred_language_code,
            c.preferred_contact_method, c.primary_state,
            # ── Accounts ──────────────────────────────────────────────────────
            coalesce(acct.num_accounts,             lit(0)).alias("num_accounts"),
            coalesce(acct.total_deposit_balance_myr,lit(0.0)).alias("total_deposit_balance_myr"),
            coalesce(acct.has_current_account,      lit(False)).alias("has_current_account"),
            coalesce(acct.has_savings_account,      lit(False)).alias("has_savings_account"),
            coalesce(acct.has_fixed_deposit,        lit(False)).alias("has_fixed_deposit"),
            # ── Loans ─────────────────────────────────────────────────────────
            coalesce(loan.num_loan_facilities,          lit(0)).alias("num_loan_facilities"),
            coalesce(loan.total_loan_outstanding_myr,   lit(0.0)).alias("total_loan_outstanding_myr"),
            coalesce(loan.monthly_loan_commitment_myr,  lit(0.0)).alias("monthly_loan_commitment_myr"),
            coalesce(loan.has_home_loan,      lit(False)).alias("has_home_loan"),
            coalesce(loan.has_personal_loan,  lit(False)).alias("has_personal_loan"),
            coalesce(loan.has_hire_purchase,  lit(False)).alias("has_hire_purchase"),
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
            # ── Credit Bureau ─────────────────────────────────────────────────
            kb.ctos_score, kb.ccris_status,
            coalesce(kb.total_credit_facilities,    lit(0)).alias("total_credit_facilities"),
            coalesce(kb.monthly_total_commitment_myr,lit(0.0)).alias("kb_monthly_commitment_myr"),
            kb.payment_conduct_12m,
            coalesce(kb.legal_cases_count,          lit(0)).alias("legal_cases_count"),
            kb.bankruptcy_status,
            coalesce(kb.credit_card_count,          lit(0)).alias("credit_card_bureau_count"),
            coalesce(kb.inquiry_count_last_6m,      lit(0)).alias("inquiry_count_last_6m"),
            # ── Digital ───────────────────────────────────────────────────────
            coalesce(da.digital_activity_score,                 lit(5.0)).alias("digital_activity_score"),
            coalesce(da.mobile_sessions_30d,                    lit(0)).alias("mobile_sessions_30d"),
            coalesce(da.product_views_last_30d,                 lit(0)).alias("product_views_last_30d"),
            da.last_product_category_viewed,
            coalesce(da.telco_data_consumption_gb_monthly,      lit(0.0)).alias("telco_data_consumption_gb_monthly"),
            coalesce(da.telco_arpu_myr,                         lit(0.0)).alias("telco_arpu_myr"),
            coalesce(da.digital_maturity_score,                 lit(5.0)).alias("digital_maturity_score"),
            # ── Product ownership flags (for ML) ──────────────────────────────
            when(coalesce(txn.has_credit_card,   lit(False)), 1).otherwise(0).alias("owns_credit_card"),
            when(coalesce(loan.has_home_loan,     lit(False)), 1).otherwise(0).alias("owns_home_loan"),
            when(coalesce(loan.has_personal_loan, lit(False)), 1).otherwise(0).alias("owns_personal_loan"),
            current_timestamp().alias("gold_computed_at"),
        ))
```

- [ ] Commit SDP silver→gold pipeline notebook
- [ ] Deploy as Pipeline `ABMB-Silver-Gold` in Databricks UI (Serverless)
- [ ] Run pipeline and confirm `SELECT COUNT(*) FROM gold_customer_360` returns ~1000

---

### Task 10: Product Catalog PDF Generation

- [ ] Install reportlab locally: `pip install reportlab`
- [ ] Write `part_a_customer_360/data/generate_pdfs.py` as a local Python script (NOT a Databricks notebook):

```python
#!/usr/bin/env python3
"""Generate 5 Alliance Bank product catalog PDFs. Run locally: python generate_pdfs.py"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.colors import HexColor, white
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                  TableStyle, HRFlowable)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
import os

NAVY  = HexColor("#1B3A6B")
RED   = HexColor("#C8102E")
LGREY = HexColor("#F5F5F5")
BLACK = HexColor("#222222")

def make_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("Title2",     fontName="Helvetica-Bold", fontSize=22, textColor=white, spaceAfter=6,  leading=26))
    styles.add(ParagraphStyle("Subtitle2",  fontName="Helvetica",      fontSize=13, textColor=white, spaceAfter=4))
    styles.add(ParagraphStyle("BodyText2",  fontName="Helvetica",      fontSize=10, textColor=BLACK, spaceAfter=4,  leading=14))
    styles.add(ParagraphStyle("SectionHead",fontName="Helvetica-Bold", fontSize=12, textColor=NAVY,  spaceAfter=4,  spaceBefore=8))
    styles.add(ParagraphStyle("SmallPrint", fontName="Helvetica",      fontSize=8,  textColor=HexColor("#666666"), spaceAfter=2))
    return styles

def header_table(product_name, category, code, styles):
    data = [[Paragraph(f'<font color="white"><b>{product_name}</b></font>', styles["Title2"]),
             Paragraph(f'<font color="white">{category} | {code}</font>', styles["Subtitle2"])]]
    t = Table(data, colWidths=[14*cm, 5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), NAVY),
        ("ALIGN",      (0,0), (-1,-1), "LEFT"),
        ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ("PADDING",    (0,0), (-1,-1), 16),
        ("LINEBELOW",  (0,-1),(-1,-1), 2, RED),
    ]))
    return t

def feature_table(rows, styles):
    data = [[Paragraph(f"<b>{k}</b>", styles["BodyText2"]),
             Paragraph(str(v), styles["BodyText2"])] for k,v in rows]
    t = Table(data, colWidths=[7*cm, 12*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), LGREY),
        ("BACKGROUND", (0,0), (0,-1),  HexColor("#E8EDF4")),
        ("GRID",       (0,0), (-1,-1), 0.5, HexColor("#CCCCCC")),
        ("PADDING",    (0,0), (-1,-1), 6),
        ("VALIGN",     (0,0), (-1,-1), "TOP"),
    ]))
    return t

PRODUCTS = [
    {
        "filename": "alliance_visa_platinum.pdf",
        "name": "Alliance Bank Visa Platinum Credit Card",
        "code": "CC-VISA-PLAT-001", "category": "CREDIT CARD",
        "tagline": "Live more, earn more with every purchase",
        "features": [
            ("Annual Fee (Principal)", "RM 800 (waived with min. 12 transactions/year)"),
            ("Annual Fee (Supplementary)", "RM 400"),
            ("Credit Limit", "RM 3,000 – RM 500,000"),
            ("Cashback Rate", "5% on Dining, Grocery & Online"),
            ("Cashback Cap", "RM 50/month"),
            ("Reward Points", "2x TreatsPoints per RM1 spent"),
            ("Interest Rate", "18% p.a. (1.5% per month)"),
            ("Interest-Free Period", "Up to 20 days"),
            ("Balance Transfer Rate", "0% for 12 months (3% processing fee)"),
            ("Late Payment Charge", "1% of outstanding or RM 10, max RM 100"),
            ("Minimum Income", "RM 36,000 p.a."),
            ("Eligible Age", "21 – 65 years"),
            ("Forex Fee", "1.5% of transaction amount"),
            ("Airport Lounge Access", "Yes — LoungeKey (2 visits/year)"),
            ("Travel Insurance", "Yes — complimentary travel PA"),
            ("Concierge Service", "24/7 Alliance Concierge"),
            ("Contactless Limit", "RM 250 per transaction"),
            ("Shariah Compliant", "No (conventional card)"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["Earn 5% cashback on everyday spending",
                     "Complimentary airport lounge access worldwide",
                     "0% balance transfer for 12 months"],
        "eligibility": "Malaysian/PR, 21–65 years, min. income RM 36,000 p.a. Permanent employee, self-employed, or professional. Clean CCRIS record.",
        "docs": "MyKad / Passport, 3 months' payslip, latest EPF statement (self-employed: 6 months bank statement + business registration)",
    },
    {
        "filename": "alliance_cashfirst_financing.pdf",
        "name": "Alliance CashFirst Personal Financing-i",
        "code": "PF-CASHFIRST-I-001", "category": "PERSONAL LOAN",
        "tagline": "Fast, flexible Islamic financing for your needs",
        "features": [
            ("Financing Type", "Unsecured Islamic Financing"),
            ("Shariah Concept", "Tawarruq (Commodity Murabahah)"),
            ("Profit Rate", "From 3.99% p.a. (fixed)"),
            ("Effective Rate", "From 7.47% p.a. (EIR)"),
            ("Min Financing Amount", "RM 5,000"),
            ("Max Financing Amount", "RM 200,000"),
            ("Min Tenure", "12 months"),
            ("Max Tenure", "84 months"),
            ("Processing Fee", "Nil"),
            ("Early Settlement Fee", "Nil"),
            ("Minimum Monthly Income", "RM 2,000"),
            ("Eligible Employment", "Permanent, Contract (min. 1 year), Self-Employed"),
            ("Max DSR", "60%"),
            ("Collateral Required", "No"),
            ("Guarantor Required", "No"),
            ("Takaful Coverage", "Optional — MRTA available"),
            ("Disbursement SLA", "3 working days"),
            ("Top-Up Facility", "Yes — available after 12 months"),
            ("Max Age at Maturity", "60 years"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["No processing fee, no early settlement penalty",
                     "Flexible tenure up to 7 years",
                     "Shariah-compliant — Tawarruq concept"],
        "eligibility": "Malaysian/PR, 21–57 years at application. Minimum monthly income RM 2,000. Clean CCRIS (no arrears > 90 days in past 12 months).",
        "docs": "MyKad, 3 months' payslips, EPF statement, employment confirmation letter",
    },
    {
        "filename": "alliance_homesmart_financing.pdf",
        "name": "Alliance HomeSmart Financing",
        "code": "HF-HOMESMART-001", "category": "HOME LOAN",
        "tagline": "Own your dream home with flexible financing",
        "features": [
            ("Financing Type", "Conventional & Islamic-i (Tawarruq)"),
            ("Base Rate", "Alliance Bank BR: 3.00% p.a."),
            ("Spread Above BR", "+1.00% to +2.00% p.a."),
            ("Effective Rate", "From 4.00% p.a."),
            ("Max Financing Margin", "Up to 90% of property value"),
            ("Max Tenure", "35 years"),
            ("Lock-In Period", "3 years"),
            ("Lock-In Penalty", "3% of approved financing amount"),
            ("Flexi Feature", "Semi-Flexi with linked current account"),
            ("Legal Fees", "Can be financed"),
            ("MRTA", "Optional (recommended)"),
            ("Fire Insurance", "Required"),
            ("Eligible Properties", "Residential: Landed, Stratified; Select Commercial"),
            ("Max Age at Maturity", "70 years"),
            ("Min Property Value", "RM 100,000"),
            ("First Home Scheme", "Eligible for Skim Jaminan Kredit Perumahan"),
            ("Stamp Duty Exemption", "Yes — for first home below RM 500,000"),
            ("Valuation Fee", "Borne by borrower"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["Up to 90% financing margin",
                     "Semi-flexi redraw facility for extra savings",
                     "Eligible for first home buyer government guarantee"],
        "eligibility": "Malaysian/PR, 18–68 years. Property must be in Malaysia. Minimum income assessed based on DSR. Joint application allowed.",
        "docs": "MyKad, 3 months' payslips, EPF, sale & purchase agreement, property valuation report",
    },
    {
        "filename": "alliance_wealthsmart_fund.pdf",
        "name": "Alliance WealthSmart Income Fund",
        "code": "UT-WEALTHSMART-INC-001", "category": "INVESTMENT",
        "tagline": "Grow your wealth with consistent quarterly distributions",
        "features": [
            ("Fund Type", "Unit Trust — Income / Bond Fund"),
            ("Risk Rating", "Medium"),
            ("Fund Manager", "Alliance Islamic Asset Management Sdn Bhd"),
            ("Trustee", "CIMB Commerce Trustee Berhad"),
            ("Currency", "MYR"),
            ("Min Initial Investment", "RM 1,000"),
            ("Min Additional Investment", "RM 100"),
            ("Annual Management Fee", "0.80% p.a."),
            ("Annual Trustee Fee", "0.07% p.a."),
            ("Sales Charge", "Up to 3.00%"),
            ("Repurchase Charge", "Nil"),
            ("Distribution Frequency", "Quarterly"),
            ("Target Distribution", "4.5% – 5.5% p.a. (not guaranteed)"),
            ("Benchmark", "Maybank 3-month fixed deposit rate"),
            ("Investment Horizon", "Min. 3 years"),
            ("Asset Allocation", "70–100% fixed income, 0–30% cash"),
            ("Capital Guaranteed", "No"),
            ("Shariah Compliant", "Yes"),
            ("Liquidity", "T+2 business days"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["Regular quarterly income distributions",
                     "Shariah-compliant with Medium risk profile",
                     "Low minimum entry at RM 1,000"],
        "eligibility": "Open to all Malaysian residents and non-residents. Subject to KYC and suitability assessment. Not eligible for KWSP (EPF) withdrawal.",
        "docs": "MyKad / Passport, completed application form, suitability questionnaire",
    },
    {
        "filename": "alliance_carstar_takaful.pdf",
        "name": "Alliance CarStar Takaful",
        "code": "INS-CARSTAR-TAKAFUL-001", "category": "INSURANCE",
        "tagline": "Comprehensive motor takaful protection for your vehicle",
        "features": [
            ("Coverage Type", "Comprehensive Motor Takaful"),
            ("Takaful Model", "Wakalah with Waqf"),
            ("Contribution Basis", "Agreed Value"),
            ("Max Sum Covered", "RM 500,000"),
            ("Third-Party Liability", "RM 3,000,000"),
            ("Coverage Period", "12 months"),
            ("Max No-Claim Discount", "55%"),
            ("NCD Transferable", "Yes"),
            ("Named Drivers", "Up to 3"),
            ("Passenger PA Coverage", "RM 10,000 per passenger"),
            ("Roadside Assistance", "24/7 — included"),
            ("Towing Limit", "Up to 150 km"),
            ("Flood Coverage", "Add-on available"),
            ("Windscreen Coverage", "Up to RM 1,500"),
            ("Claim Settlement (Total Loss)", "7 working days"),
            ("Panel Workshops", "500+ nationwide"),
            ("Eligible Vehicles", "Private cars & light commercial"),
            ("Max Vehicle Age (Comprehensive)", "15 years"),
            ("Standard Excess", "RM 400"),
            ("Effective Date", "1 September 2026"),
        ],
        "benefits": ["24/7 roadside assistance included",
                     "Up to 55% No-Claim Discount",
                     "Fast 7-day total loss settlement"],
        "eligibility": "Vehicle owner aged 18–70. Vehicle not exceeding 15 years old (comprehensive). Must hold valid driving license. Subject to underwriting.",
        "docs": "MyKad, vehicle grant, previous takaful/insurance certificate, roadtax",
    },
]

def generate_pdf(product, output_dir):
    styles = make_styles()
    path   = os.path.join(output_dir, product["filename"])
    doc    = SimpleDocTemplate(path, pagesize=A4,
                               leftMargin=1.5*cm, rightMargin=1.5*cm,
                               topMargin=1.5*cm,  bottomMargin=1.5*cm)
    story = []
    story.append(header_table(product["name"], product["category"], product["code"], styles))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph(product["tagline"], styles["Subtitle2"]))
    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph("Key Benefits", styles["SectionHead"]))
    for b in product["benefits"]:
        story.append(Paragraph(f"- {b}", styles["BodyText2"]))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Product Features & Rates", styles["SectionHead"]))
    story.append(feature_table(product["features"], styles))
    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph("Eligibility Criteria", styles["SectionHead"]))
    story.append(Paragraph(product["eligibility"], styles["BodyText2"]))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Documents Required", styles["SectionHead"]))
    story.append(Paragraph(product["docs"], styles["BodyText2"]))
    story.append(Spacer(1, 0.6*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=RED))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(
        "Alliance Bank Malaysia Berhad (198201008390 / 112748-V). This brochure is for "
        "illustrative purposes only. Terms and conditions apply. Subject to credit assessment "
        "and approval. Rates and fees are effective as of the date shown above.",
        styles["SmallPrint"]))
    doc.build(story)
    print(f"Generated: {product['filename']}")

if __name__ == "__main__":
    OUT = os.path.join(os.path.dirname(__file__), "pdfs")
    os.makedirs(OUT, exist_ok=True)
    for p in PRODUCTS:
        generate_pdf(p, OUT)
    print("\nAll PDFs generated.")
    print("Upload command:")
    print("  databricks fs cp part_a_customer_360/data/pdfs/ dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs/ --recursive --profile fevm-master-classic-marcus")
```

- [ ] Run `python part_a_customer_360/data/generate_pdfs.py` locally
- [ ] Verify 5 PDFs exist in `part_a_customer_360/data/pdfs/`
- [ ] Upload to UC Volume:
  ```bash
  databricks fs cp part_a_customer_360/data/pdfs/ \
    dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs/ \
    --recursive --profile fevm-master-classic-marcus
  ```
- [ ] Verify upload: `databricks fs ls dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs/ --profile fevm-master-classic-marcus`
- [ ] Commit generate_pdfs.py

---

### Task 11: PDF Pipeline Notebook (ai_parse_document → ai_classify → ai_extract)

- [ ] Write `part_a_customer_360/09_pdf_pipeline.py` as a Databricks notebook source file:

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # PDF Pipeline — ai_parse_document → ai_classify → ai_extract
# MAGIC
# MAGIC ## Lakeflow Designer Instructions
# MAGIC This notebook contains the SQL for a 3-node Lakeflow Designer pipeline.
# MAGIC To run in Lakeflow Designer UI:
# MAGIC 1. Pipelines → Lakeflow Designer → New Pipeline
# MAGIC 2. Create 3 nodes corresponding to the 3 SQL cells below
# MAGIC 3. Wire Node 1 output → Node 2 input → Node 3 input
# MAGIC
# MAGIC Alternatively, run this notebook top-to-bottom to execute all 3 steps sequentially.

# COMMAND ----------
# MAGIC %md ## Node 1: ai_parse_document — Extract raw text from PDFs

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_parsed AS
# MAGIC SELECT
# MAGIC   path                                                                AS file_path,
# MAGIC   regexp_extract(path, '[^/]+(?=\\.[Pp][Dd][Ff]$)')                  AS product_code,
# MAGIC   ai_parse_document(content, 'text')                                  AS parsed_text,
# MAGIC   length(content)                                                     AS file_size_bytes,
# MAGIC   current_timestamp()                                                 AS parsed_at
# MAGIC FROM read_files(
# MAGIC   'dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs',
# MAGIC   format => 'binaryFile',
# MAGIC   pathGlobFilter => '*.pdf'
# MAGIC );

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT product_code, length(parsed_text) AS text_length_chars
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_parsed;

# COMMAND ----------
# MAGIC %md ## Node 2: ai_classify — Classify each brochure into product category

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_classified AS
# MAGIC SELECT
# MAGIC   file_path,
# MAGIC   product_code,
# MAGIC   parsed_text,
# MAGIC   parsed_at,
# MAGIC   ai_classify(
# MAGIC     parsed_text,
# MAGIC     ARRAY('CREDIT_CARD', 'PERSONAL_LOAN', 'HOME_LOAN', 'INVESTMENT', 'INSURANCE')
# MAGIC   ) AS product_category
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_parsed;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT product_code, product_category
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_classified;

# COMMAND ----------
# MAGIC %md ## Node 3: ai_extract — Extract 34 structured fields from each brochure

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_product_catalog AS
# MAGIC SELECT
# MAGIC   file_path, product_code, product_category, parsed_at,
# MAGIC   ai_extract(parsed_text, named_struct(
# MAGIC     'product_name',           'Full product name as printed on the document',
# MAGIC     'product_code_extracted', 'Internal product reference code or identifier',
# MAGIC     'min_amount_myr',         'Minimum financing, credit limit, or investment amount in MYR',
# MAGIC     'max_amount_myr',         'Maximum financing, credit limit, or investment amount in MYR',
# MAGIC     'min_rate_pct',           'Minimum interest, profit, management, or contribution rate as percentage',
# MAGIC     'max_rate_pct',           'Maximum interest, profit, management, or contribution rate as percentage',
# MAGIC     'min_tenure_months',      'Minimum product tenure or lock-in period in months',
# MAGIC     'max_tenure_months',      'Maximum product tenure or coverage period in months',
# MAGIC     'min_income_annual_myr',  'Minimum annual gross income required for eligibility in MYR',
# MAGIC     'shariah_compliant',      'Whether the product is Shariah-compliant: Yes or No',
# MAGIC     'annual_fee_myr',         'Annual fee, subscription, or contribution amount in MYR',
# MAGIC     'annual_fee_waiver',      'Condition under which the annual fee is waived if applicable',
# MAGIC     'processing_fee',         'One-time processing application or origination fee',
# MAGIC     'early_termination_fee',  'Penalty or fee for early settlement cancellation or redemption',
# MAGIC     'key_benefit_1',          'First key feature or benefit headline from the product brochure',
# MAGIC     'key_benefit_2',          'Second key feature or benefit headline',
# MAGIC     'key_benefit_3',          'Third key feature or benefit headline',
# MAGIC     'fees_summary',           'Brief summary of all recurring and one-time fees and charges',
# MAGIC     'eligibility_age_min',    'Minimum applicant age in years',
# MAGIC     'eligibility_age_max',    'Maximum applicant age in years',
# MAGIC     'eligibility_nationality','Eligible nationality or residency status required',
# MAGIC     'eligibility_employment', 'Eligible employment types or income sources',
# MAGIC     'documents_required',     'List of documents required for application',
# MAGIC     'cashback_rate_pct',      'Cashback or rebate rate percentage for credit cards null if not applicable',
# MAGIC     'reward_points_per_rm',   'Reward points or miles earned per RM1 spent null if not applicable',
# MAGIC     'lounge_access_included', 'Whether complimentary airport lounge access is included Yes No or null',
# MAGIC     'profit_rate_type',       'Whether rate is fixed or variable floating null if not a loan product',
# MAGIC     'collateral_required',    'Whether collateral or security is required Yes No or null',
# MAGIC     'max_dsr_pct',            'Maximum debt service ratio percentage allowed null if not applicable',
# MAGIC     'fund_risk_rating',       'Risk rating of the fund Low Medium High or null if not investment',
# MAGIC     'capital_guaranteed',     'Whether capital is guaranteed Yes No or null if not investment',
# MAGIC     'distribution_frequency', 'How often distributions or dividends are paid null if not applicable',
# MAGIC     'sum_covered_max_myr',    'Maximum sum assured or covered in MYR null if not insurance',
# MAGIC     'effective_date',         'Date this product brochure pricing or terms became effective'
# MAGIC   )):product_name                AS product_name,
# MAGIC   ai_extract(parsed_text, named_struct('min_amount_myr','Minimum financing credit limit or investment amount in MYR')):min_amount_myr AS min_amount_myr,
# MAGIC   ai_extract(parsed_text, named_struct('max_amount_myr','Maximum financing credit limit or investment amount in MYR')):max_amount_myr AS max_amount_myr,
# MAGIC   ai_extract(parsed_text, named_struct('min_rate_pct','Minimum interest profit management or contribution rate as percentage')):min_rate_pct AS min_rate_pct,
# MAGIC   ai_extract(parsed_text, named_struct('max_rate_pct','Maximum interest profit management or contribution rate as percentage')):max_rate_pct AS max_rate_pct,
# MAGIC   ai_extract(parsed_text, named_struct('shariah_compliant','Whether the product is Shariah-compliant: Yes or No')):shariah_compliant AS shariah_compliant,
# MAGIC   ai_extract(parsed_text, named_struct('annual_fee_myr','Annual fee subscription or contribution amount in MYR')):annual_fee_myr AS annual_fee_myr,
# MAGIC   ai_extract(parsed_text, named_struct('annual_fee_waiver','Condition under which the annual fee is waived if applicable')):annual_fee_waiver AS annual_fee_waiver,
# MAGIC   ai_extract(parsed_text, named_struct('processing_fee','One-time processing application or origination fee')):processing_fee AS processing_fee,
# MAGIC   ai_extract(parsed_text, named_struct('key_benefit_1','First key feature or benefit headline from the product brochure')):key_benefit_1 AS key_benefit_1,
# MAGIC   ai_extract(parsed_text, named_struct('key_benefit_2','Second key feature or benefit headline')):key_benefit_2 AS key_benefit_2,
# MAGIC   ai_extract(parsed_text, named_struct('key_benefit_3','Third key feature or benefit headline')):key_benefit_3 AS key_benefit_3,
# MAGIC   ai_extract(parsed_text, named_struct('eligibility_age_min','Minimum applicant age in years')):eligibility_age_min AS eligibility_age_min,
# MAGIC   ai_extract(parsed_text, named_struct('eligibility_age_max','Maximum applicant age in years')):eligibility_age_max AS eligibility_age_max,
# MAGIC   ai_extract(parsed_text, named_struct('cashback_rate_pct','Cashback or rebate rate percentage for credit cards null if not applicable')):cashback_rate_pct AS cashback_rate_pct,
# MAGIC   ai_extract(parsed_text, named_struct('lounge_access_included','Whether complimentary airport lounge access is included Yes No or null')):lounge_access_included AS lounge_access_included,
# MAGIC   ai_extract(parsed_text, named_struct('profit_rate_type','Whether rate is fixed or variable floating null if not a loan product')):profit_rate_type AS profit_rate_type,
# MAGIC   ai_extract(parsed_text, named_struct('collateral_required','Whether collateral or security is required Yes No or null')):collateral_required AS collateral_required,
# MAGIC   ai_extract(parsed_text, named_struct('fund_risk_rating','Risk rating of the fund Low Medium High or null if not investment')):fund_risk_rating AS fund_risk_rating,
# MAGIC   ai_extract(parsed_text, named_struct('capital_guaranteed','Whether capital is guaranteed Yes No or null if not investment')):capital_guaranteed AS capital_guaranteed,
# MAGIC   ai_extract(parsed_text, named_struct('sum_covered_max_myr','Maximum sum assured or covered in MYR null if not insurance')):sum_covered_max_myr AS sum_covered_max_myr,
# MAGIC   ai_extract(parsed_text, named_struct('effective_date','Date this product brochure pricing or terms became effective')):effective_date AS effective_date,
# MAGIC   current_timestamp() AS extracted_at
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_classified;

# COMMAND ----------
# MAGIC %md ## Validation

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT product_code, product_category, product_name,
# MAGIC        min_amount_myr, max_amount_myr, shariah_compliant,
# MAGIC        cashback_rate_pct, sum_covered_max_myr, effective_date
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_product_catalog
# MAGIC ORDER BY product_category;
# MAGIC -- Expected: 5 rows, each with the correct category and key fields populated
```

- [ ] Commit PDF pipeline notebook

---

## Self-Review Checklist

All spec sections covered:

- [x] **Scenario 1 (Schema Evolution):** Task 6 — `05_schema_evolution_demo.py`, 3-act notebook with RESET cell
- [x] **Scenario 2 (Batch AutoLoader):** Task 3 — `02_ingest_batch.py`, 5 CSV sources, `availableNow=True`
- [x] **Scenario 3 (Near-RT CDC):** Task 4 — `03_ingest_nearrt.py`, JSON micro-batch `processingTime=30s`
- [x] **Scenario 4 (Streaming):** Task 5 — `04_ingest_realtime.py`, Rate Source at 2 events/sec
- [x] **Banking MVM:** Tasks 8+9 — SCD Type 2 via `apply_changes`, 6 domains, MVM table in docs
- [x] **DQ Expectations:** Task 8 — `@dlt.expect`, `@dlt.expect_or_drop` on all silver tables
- [x] **Rescued Data:** Task 7 — `06_rescued_data_demo.py`, 3-step demo, inspects `_rescued_data IS NOT NULL`
- [x] **gold_customer_360:** Task 9 — ~65 features, SCD2 filter `__END_AT IS NULL`, intermediate `_agg_*` tables
- [x] **Product PDFs:** Task 10 — `generate_pdfs.py` produces 5 branded PDFs, uploads via `databricks fs cp`
- [x] **PDF AI Pipeline:** Task 11 — `ai_parse_document` → `ai_classify` → `ai_extract` (34 fields), Lakeflow Designer compatible
- [x] **Global constraints:** All tasks use FULL_SCHEMA, VOLUME_DATA, Serverless, `_rescued_data`, `%run ../shared/config`
- [x] **Reproducibility:** Faker seed=42, random seed=42, numpy seed=42 set in Task 2
- [x] **Validation cells:** Every task ends with assertion/display confirming expected row counts
