# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Event Generator
# MAGIC
# MAGIC **Run as:** Databricks Job (`DBX-Events-Generator`) — long-running continuous notebook.
# MAGIC
# MAGIC Simulates three live data feeds for the streaming demo:
# MAGIC
# MAGIC | Feed | Volume | Interval | Target directory |
# MAGIC |---|---|---|---|
# MAGIC | Digital events | 15–25 events/batch | Every **10 seconds** | `streaming/digital_events/` |
# MAGIC | Credit bureau | 3–8 customers/batch | Every **60 seconds** | `streaming/credit_bureau/` |
# MAGIC | Telco events | 5–12 customers/batch | Every **45 seconds** | `streaming/telco_events/` |
# MAGIC
# MAGIC Each batch is written as a single JSON-lines file.
# MAGIC Pipeline 1 (`DBX-RT-Bronze`) picks them up automatically via AutoLoader.

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import os, random, json, time, uuid
from datetime import datetime, date, timedelta

# ── Pulse configuration ────────────────────────────────────────────────────────
INTERVAL_DIGITAL = 10    # seconds between digital event batches
INTERVAL_CREDIT  = 60    # seconds between credit bureau micro-batches
INTERVAL_TELCO   = 45    # seconds between telco event micro-batches

BATCH_DIGITAL = 20       # events per digital batch
BATCH_CREDIT  = 5        # customer records per credit bureau batch
BATCH_TELCO   = 8        # customer records per telco batch

# ── Reference data (matches data generator) ────────────────────────────────────
EVENT_TYPES   = ["login","view_product","apply","fund_transfer","bill_pay","investment","logout"]
EVENT_WEIGHTS = [0.25, 0.20, 0.05, 0.22, 0.12, 0.08, 0.08]
PRODUCT_CATS  = ["home_loan","personal_loan","credit_card","fixed_deposit",
                 "investment_fund","insurance","hire_purchase"]
CHANNELS      = ["mobile_app","mobile_app","mobile_app","web_banking"]
DEVICE_TYPES  = ["iPhone","Android","iPad","Desktop"]

CCRIS_STATUSES   = ["clean","1_dpd","30_dpd","60_dpd","90_dpd"]
CCRIS_WEIGHTS    = [0.70, 0.12, 0.08, 0.06, 0.04]
PAYMENT_CONDUCTS = ["clean","1_missed","2_missed","3+_missed"]
PAYMENT_C_W      = [0.72, 0.14, 0.09, 0.05]
BANKRUPTCY_STAT  = ["none","voluntary","involuntary"]
BANKRUPTCY_W     = [0.97, 0.02, 0.01]

TELCO_CARRIERS = ["Maxis","Celcom","Digi","U Mobile","YES 4G"]
DEVICE_CLASSES = ["flagship","mid_range","budget","feature_phone"]
DEVICE_WEIGHTS_T = [0.25, 0.40, 0.28, 0.07]
DATA_PLANS     = ["UNLIMITED_50","UNLIMITED_30","15GB","8GB","3GB","BASIC"]

# COMMAND ----------
# MAGIC %md ## Load customer roster

# COMMAND ----------
# Load party_id → cif_number mapping from existing bronze_customer_master
try:
    rows = (spark.table(f"{FULL_SCHEMA}.bronze_customer_master")
            .select("party_id", "cif_number")
            .distinct()
            .limit(1000)
            .collect())
    CUSTOMERS = [(r.party_id, r.cif_number) for r in rows]
    print(f"✅ Loaded {len(CUSTOMERS)} customers from bronze_customer_master")
except Exception as e:
    # Fallback synthetic roster
    CUSTOMERS = [(f"P{i:08d}", f"CIF{i:06d}") for i in range(1, 1001)]
    print(f"⚠️  Using synthetic roster (bronze not available yet): {e}")

# COMMAND ----------
# MAGIC %md ## Ensure streaming directories exist

# COMMAND ----------
for subdir in ["streaming/digital_events", "streaming/credit_bureau", "streaming/telco_events"]:
    os.makedirs(f"{VOLUME_DATA}/{subdir}", exist_ok=True)
print("✅ Streaming directories ready")

# COMMAND ----------
# MAGIC %md ## Seed: write initial batch so Pipeline 1 has data on first start

# COMMAND ----------
def _write_json_lines(records: list, path: str):
    """Write a list of dicts as a JSON-lines file to a UC Volume path.
    Uses Python open() — UC Volumes are accessible as /Volumes/... on all compute types.
    """
    # Convert dbfs:/Volumes/... or /Volumes/... to local path
    local = path.replace("dbfs:", "")
    os.makedirs(os.path.dirname(local), exist_ok=True)
    with open(local, "w") as f:
        f.write("\n".join(json.dumps(r) for r in records))

def _ts():
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")

# COMMAND ----------
def make_digital_events(n=20):
    now = datetime.utcnow()
    events = []
    for _ in range(n):
        party_id, cif = random.choice(CUSTOMERS)
        evt_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        evt_dt   = now - timedelta(seconds=random.randint(0, INTERVAL_DIGITAL))
        events.append({
            "event_id":                 f"DE_{uuid.uuid4().hex[:12].upper()}",
            "party_id":                 party_id,
            "cif_number":               cif,
            "session_id":               f"SES_{uuid.uuid4().hex[:10].upper()}",
            "event_timestamp":          evt_dt.isoformat(),
            "event_date":               evt_dt.date().isoformat(),
            "event_type":               evt_type,
            "product_category_viewed":  random.choice(PRODUCT_CATS) if evt_type == "view_product" else None,
            "channel":                  random.choice(CHANNELS),
            "device_type":              random.choice(DEVICE_TYPES),
            "page_url":                 f"/app/{evt_type.replace('_','-')}",
            "session_duration_sec":     random.randint(10, 900),
            "is_authenticated":         True,
            "source_system":            "DIGITAL_BANKING_APP",
        })
    return events

def make_credit_bureau_updates(n=5):
    now = datetime.utcnow()
    records = []
    for _ in range(n):
        party_id, cif = random.choice(CUSTOMERS)
        ctos = random.randint(300, 850)
        records.append({
            "cif_number":                    cif,
            "party_id":                      party_id,
            "report_date":                   now.date().isoformat(),
            "ctos_score":                    ctos,
            "ccris_status":                  random.choices(CCRIS_STATUSES, weights=CCRIS_WEIGHTS)[0],
            "total_credit_facilities":       random.randint(0, 15),
            "total_outstanding_balance_myr": round(random.uniform(0, 2_000_000), 2),
            "monthly_total_commitment_myr":  round(random.uniform(0, 30_000), 2),
            "payment_conduct_12m":           random.choices(PAYMENT_CONDUCTS, weights=PAYMENT_C_W)[0],
            "legal_cases_count":             random.choices([0,1,2,3], weights=[0.90,0.06,0.03,0.01])[0],
            "bankruptcy_status":             random.choices(BANKRUPTCY_STAT, weights=BANKRUPTCY_W)[0],
            "credit_card_count":             random.randint(0, 8),
            "home_loan_count":               random.randint(0, 3),
            "personal_loan_count":           random.randint(0, 4),
            "hire_purchase_count":           random.randint(0, 3),
            "overdraft_count":               random.randint(0, 2),
            "inquiry_count_last_6m":         random.randint(0, 10),
            "inquiry_count_last_12m":        random.randint(0, 15),
            "oldest_facility_years":         random.randint(0, 20),
            "credit_utilisation_pct":        round(random.uniform(0, 100), 1),
            "debt_to_income_ratio":          round(random.uniform(0, 1.2), 3),
            "largest_single_facility_myr":   round(random.uniform(0, 1_000_000), 2),
            "secured_debt_pct":              round(random.uniform(0, 100), 1),
            "unsecured_debt_pct":            round(random.uniform(0, 100), 1),
            "bureau_pull_timestamp":         now.isoformat(),
            "bureau_provider":               random.choice(["CTOS","CCRIS","RAM Credit"]),
            "consent_flag":                  True,
            "consent_date":                  (now.date() - timedelta(days=7)).isoformat(),
            "aml_flag":                      random.random() < 0.01,
            "pep_flag":                      random.random() < 0.02,
            "sanction_flag":                 random.random() < 0.005,
            "data_source":                   "CTOS_API_v2",
        })
    return records

def make_telco_events(n=8):
    now = datetime.utcnow()
    records = []
    for _ in range(n):
        party_id, cif = random.choice(CUSTOMERS)
        records.append({
            "event_id":                      f"TELCO_{uuid.uuid4().hex[:10].upper()}",
            "cif_number":                    cif,
            "party_id":                      party_id,
            "event_date":                    now.date().isoformat(),
            "telco_carrier":                 random.choice(TELCO_CARRIERS),
            "data_plan":                     random.choice(DATA_PLANS),
            "data_consumption_mb_monthly":   round(random.uniform(100, 60_000), 1),
            "voice_minutes_monthly":         random.randint(0, 2_000),
            "sms_count_monthly":             random.randint(0, 500),
            "arpu_myr":                      round(random.uniform(25, 250), 2),
            "device_class":                  random.choices(DEVICE_CLASSES, weights=DEVICE_WEIGHTS_T)[0],
            "device_os":                     random.choices(["iOS","Android"], weights=[0.35,0.65])[0],
            "digital_maturity_score":        round(random.uniform(1, 10), 1),
            "roaming_flag":                  random.random() < 0.12,
            "postpaid_flag":                 random.random() < 0.65,
            "tenure_months_with_carrier":    random.randint(1, 120),
            "churn_risk_score":              round(random.uniform(0, 1), 3),
            "financial_inclusion_score":     round(random.uniform(1, 10), 1),
            "app_usage_banking_mins":        round(random.uniform(0, 120), 1),
            "location_cluster":              f"GEO_{random.randint(1,20):02d}",
            "consent_flag":                  True,
            "data_source":                   "TELCO_PARTNER_API",
        })
    return records

# COMMAND ----------
# MAGIC %md ## Seed initial files (50 digital, 10 credit, 15 telco)

# COMMAND ----------
_write_json_lines(make_digital_events(50),        f"{VOLUME_DATA}/streaming/digital_events/seed_{_ts()}.json")
_write_json_lines(make_credit_bureau_updates(10), f"{VOLUME_DATA}/streaming/credit_bureau/seed_{_ts()}.json")
_write_json_lines(make_telco_events(15),          f"{VOLUME_DATA}/streaming/telco_events/seed_{_ts()}.json")
print("✅ Seed data written — Pipeline 1 can now start")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚀 Main Loop — Continuous Event Stream
# MAGIC
# MAGIC | 🔴 Digital events | every ~10s | Real-time customer interactions |
# MAGIC | 🟡 Credit bureau  | every ~60s | Near-RT CTOS/CCRIS micro-batch  |
# MAGIC | 🟡 Telco events   | every ~45s | Near-RT operator signal micro-batch |

# COMMAND ----------
last_credit = time.time()
last_telco  = time.time()
iteration   = 0

print("🚀 Event generator running — Ctrl+C or cancel job to stop\n")

while True:
    now = time.time()
    ts  = _ts()
    iteration += 1

    # ── 🔴 Digital events — every iteration ───────────────────────────────────
    n_digital = random.randint(15, 25)
    _write_json_lines(
        make_digital_events(n_digital),
        f"{VOLUME_DATA}/streaming/digital_events/events_{ts}.json"
    )

    # ── 🟡 Credit bureau — every INTERVAL_CREDIT seconds ─────────────────────
    if now - last_credit >= INTERVAL_CREDIT:
        n_credit = random.randint(3, 8)
        _write_json_lines(
            make_credit_bureau_updates(n_credit),
            f"{VOLUME_DATA}/streaming/credit_bureau/cb_{ts}.json"
        )
        last_credit = now
        credit_msg = f"  💳 Credit bureau: {n_credit} records"
    else:
        credit_msg = f"  💳 Credit bureau: next in {int(INTERVAL_CREDIT - (now - last_credit))}s"

    # ── 🟡 Telco events — every INTERVAL_TELCO seconds ────────────────────────
    if now - last_telco >= INTERVAL_TELCO:
        n_telco = random.randint(5, 12)
        _write_json_lines(
            make_telco_events(n_telco),
            f"{VOLUME_DATA}/streaming/telco_events/telco_{ts}.json"
        )
        last_telco = now
        telco_msg = f"  📱 Telco events:   {n_telco} records"
    else:
        telco_msg = f"  📱 Telco events:   next in {int(INTERVAL_TELCO - (now - last_telco))}s"

    print(f"[{datetime.utcnow().strftime('%H:%M:%S')} | iter {iteration:04d}]"
          f"  🔴 Digital: {n_digital} events written")
    print(credit_msg)
    print(telco_msg)
    print()

    time.sleep(INTERVAL_DIGITAL)
