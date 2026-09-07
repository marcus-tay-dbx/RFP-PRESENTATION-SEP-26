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

fake = Faker()  # en_US default — Malaysian patterns handled by custom logic below
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
        "is_shariah_preferred":           random.random() < 0.40,
        "source_system":                  "CBS_CORE",
    }
    customers_v1.append(base)

    # v2 adds occupation_code — showcases AutoLoader schema evolution (addNewColumns)
    v2_extra = {**base,
                "occupation_code":        random.choice(OCCUPATION_CODES),
                "record_updated_timestamp": (rec_updated + timedelta(days=30)).isoformat()}
    customers_v2.append(v2_extra)

df_v1 = pd.DataFrame(customers_v1)
df_v2 = pd.DataFrame(customers_v2)

# ── Write customer files ──────────────────────────────────────────────────────
os.makedirs(f"{VOLUME_DATA}/batch_v1/customer_master", exist_ok=True)
os.makedirs(f"{VOLUME_DATA}/batch_v2/customer_master", exist_ok=True)

df_v1.to_csv(f"{VOLUME_DATA}/batch_v1/customer_master/customers_v1.csv", index=False)
df_v2.to_csv(f"{VOLUME_DATA}/batch_v2/customer_master/customers_v2.csv", index=False)
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
os.makedirs(f"{VOLUME_DATA}/batch_v1/accounts", exist_ok=True)
df_acc.to_csv(f"{VOLUME_DATA}/batch_v1/accounts/accounts.csv", index=False)
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
os.makedirs(f"{VOLUME_DATA}/batch_v1/loans", exist_ok=True)
df_loans.to_csv(f"{VOLUME_DATA}/batch_v1/loans/loans.csv", index=False)
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
os.makedirs(f"{VOLUME_DATA}/batch_v1/transactions", exist_ok=True)
df_txn.to_csv(f"{VOLUME_DATA}/batch_v1/transactions/transactions.csv", index=False)
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
os.makedirs(f"{VOLUME_DATA}/batch_v1/cards", exist_ok=True)
df_cards.to_csv(f"{VOLUME_DATA}/batch_v1/cards/cards_txn.csv", index=False)
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

os.makedirs(f"{VOLUME_DATA}/nearrt/credit_bureau", exist_ok=True)
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
    with open(f"{VOLUME_DATA}/nearrt/credit_bureau/{cif}.json", "w") as f:
        json.dump(record, f)

print(f"✅ Credit bureau: 1000 JSON files written")

# COMMAND ----------
# MAGIC %md ### Generate Near-RT JSON Files — Telco Events (5,000 files)

# COMMAND ----------
TELCO_CARRIERS   = ["Maxis","Celcom","Digi","U Mobile","YES 4G"]
DEVICE_CLASSES   = ["flagship","mid_range","budget","feature_phone"]
DEVICE_WEIGHTS   = [0.25, 0.40, 0.28, 0.07]
DATA_PLANS       = ["UNLIMITED_50","UNLIMITED_30","15GB","8GB","3GB","BASIC"]

os.makedirs(f"{VOLUME_DATA}/nearrt/telco_events", exist_ok=True)
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
        fname = f"{VOLUME_DATA}/nearrt/telco_events/{cif}_{j:02d}.json"
        with open(fname, "w") as f:
            json.dump(record, f)

print(f"✅ Telco events: 5000 JSON files written")

# COMMAND ----------
# MAGIC %md ### Generate Batch CSV — Digital Banking Events (~15,000 rows)

# COMMAND ----------
EVENT_TYPES     = ["login","view_product","apply","fund_transfer","bill_pay","investment","logout"]
EVENT_WEIGHTS   = [0.25,   0.20,          0.05,   0.22,          0.12,      0.08,        0.08]
PRODUCT_CATS    = ["home_loan","personal_loan","credit_card","fixed_deposit","investment_fund","insurance","hire_purchase"]
CHANNELS        = ["mobile_app","mobile_app","mobile_app","web"]  # 75% mobile
DEVICE_TYPES    = ["iPhone","Android","iPad","Desktop"]

digital_events = []
for idx, cif in enumerate(customer_ids):
    party_id   = f"P{int(cif[3:]):08d}"
    n_events   = random.randint(8, 25)
    for j in range(n_events):
        evt_type  = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        evt_dt    = datetime(2026, 8, 1) + timedelta(
                        days=random.randint(0, 37),
                        hours=random.randint(6, 23),
                        minutes=random.randint(0, 59))
        digital_events.append({
            "event_id":                  f"DE{idx:06d}_{j:03d}",
            "party_id":                  party_id,
            "cif_number":                cif,
            "session_id":                f"SES{idx:06d}_{j//3:03d}",
            "event_timestamp":           evt_dt.isoformat(),
            "event_date":                evt_dt.date().isoformat(),
            "event_type":                evt_type,
            "product_category_viewed":   random.choice(PRODUCT_CATS) if evt_type == "view_product" else None,
            "channel":                   random.choice(CHANNELS),
            "device_type":               random.choice(DEVICE_TYPES),
            "page_url":                  f"/app/{evt_type.replace('_','-')}",
            "session_duration_sec":      random.randint(10, 900),
            "is_authenticated":          True,
            "source_system":             "DIGITAL_BANKING_APP",
        })

df_digital = pd.DataFrame(digital_events)
os.makedirs(f"{VOLUME_DATA}/batch_v1/digital_events", exist_ok=True)
df_digital.to_csv(f"{VOLUME_DATA}/batch_v1/digital_events/digital_events.csv", index=False)
print(f"✅ digital_events.csv: {len(df_digital)} rows")

# COMMAND ----------
# MAGIC %md ### Validate all output files

# COMMAND ----------
for path in [
    f"{VOLUME_DATA}/batch_v1/customer_master/customers_v1.csv",
    f"{VOLUME_DATA}/batch_v1/accounts/accounts.csv",
    f"{VOLUME_DATA}/batch_v1/loans/loans.csv",
    f"{VOLUME_DATA}/batch_v1/transactions/transactions.csv",
    f"{VOLUME_DATA}/batch_v1/cards/cards_txn.csv",
    f"{VOLUME_DATA}/batch_v1/digital_events/digital_events.csv",
]:
    rows = spark.read.csv(path, header=True).count()
    print(f"✅ {path.split('/')[-1]}: {rows} rows")

cb_count = len(dbutils.fs.ls(f"{VOLUME_DATA}/nearrt/credit_bureau/"))
te_count = len(dbutils.fs.ls(f"{VOLUME_DATA}/nearrt/telco_events/"))
print(f"✅ credit_bureau JSON files: {cb_count}")
print(f"✅ telco_events JSON files:  {te_count}")
