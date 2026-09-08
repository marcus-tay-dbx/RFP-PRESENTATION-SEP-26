# Databricks notebook source
# MAGIC %md
# MAGIC # Dirty Data Injector — Silver Transactions Quality Demo
# MAGIC
# MAGIC Writes a batch of intentionally malformed transactions to
# MAGIC `batch_v1/transactions/dirty_batch_001.csv`.
# MAGIC
# MAGIC AutoLoader in the Customer-360 pipeline will pick this up on the next run.
# MAGIC
# MAGIC ## What fails and why
# MAGIC | Expectation | Action | Dirty pattern |
# MAGIC |---|---|---|
# MAGIC | `valid_txn_id` | **DROP** | `payment_transaction_id` = NULL |
# MAGIC | `positive_amount` | **DROP** | `payment_amount` < 0 |
# MAGIC | `valid_currency` | ALLOW (flagged) | `currency_id` = 'XYZ' / 'BTC' |
# MAGIC | `valid_status` | ALLOW (flagged) | `instruction_status` = 'FROZEN' / 'FRAUD_HOLD' |
# MAGIC | `valid_rail` | ALLOW (flagged) | `payment_rail_type` = 'CRYPTO' / 'CBDC' |
# MAGIC | `posting_after_value` | ALLOW (flagged) | `posting_date` < `value_date` |

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import random, os, pandas as pd
from datetime import date, timedelta

random.seed(999)

# Load party_ids from existing bronze table
rows = (spark.table(f"{FULL_SCHEMA}.bronze_customer_master")
        .select("party_id", "cif_number")
        .limit(200).collect())
customers = [(r.party_id, r.cif_number) for r in rows]
print(f"Using {len(customers)} customers for dirty injection")

# COMMAND ----------
MCC_CODES = ["5411","5812","5999","4111","7011","5912","5200","5732","5945","4900"]

def make_dirty_txn(party_id, cif, dirty_type):
    txn_date  = date(2026, 9, 1) + timedelta(days=random.randint(0, 7))
    value_dt  = txn_date - timedelta(days=random.randint(0, 1))
    base = {
        "payment_transaction_id":     f"DIRTY{random.randint(100000000,999999999)}",
        "party_id":                   party_id,
        "cif_number":                 cif,
        "deposit_account_id":         f"{random.randint(1000000000,9999999999)}",
        "txn_type":                   "DEBIT",
        "payment_amount":             round(random.uniform(50, 5000), 2),
        "currency_id":                "MYR",
        "payment_rail_type":          "FPX",
        "instruction_status":         "settled",
        "posting_date":               txn_date.isoformat(),
        "value_date":                 value_dt.isoformat(),
        "description":                "Test dirty record",
        "counterparty_name":          "Test Corp",
        "counterparty_account":       f"{random.randint(1000000000,9999999999)}",
        "mcc_code":                   random.choice(MCC_CODES),
        "channel":                    "mobile_app",
        "sanctions_screening_status": "CLEARED",
        "fraud_flag":                 False,
        "running_balance_myr":        round(random.uniform(100, 50000), 2),
        "reference_number":           f"DREF{random.randint(100000000,999999999)}",
        "source_system":              "TXN_ENGINE",
    }
    if dirty_type == "null_id":
        base["payment_transaction_id"] = None          # DROP: valid_txn_id
    elif dirty_type == "negative_amount":
        base["payment_amount"] = -round(random.uniform(10, 2000), 2)   # DROP: positive_amount
    elif dirty_type == "invalid_currency":
        base["currency_id"] = random.choice(["XYZ","BTC","AED","JPY"])  # ALLOW+flag: valid_currency
    elif dirty_type == "invalid_status":
        base["instruction_status"] = random.choice(["FROZEN","FRAUD_HOLD","ON_HOLD","DISPUTED"])  # ALLOW+flag
    elif dirty_type == "invalid_rail":
        base["payment_rail_type"] = random.choice(["CRYPTO","BLIK","RTGS_EXT","CBDC"])  # ALLOW+flag
    elif dirty_type == "posting_before_value":
        vd = date.fromisoformat(base["value_date"])
        base["posting_date"] = (vd - timedelta(days=random.randint(1, 3))).isoformat()  # ALLOW+flag
    return base

# COMMAND ----------
# MAGIC %md ## Generate 2,400 dirty records — ~5% fail rate across 6 expectation types

# COMMAND ----------
dirty_types = ["null_id","negative_amount","invalid_currency",
               "invalid_status","invalid_rail","posting_before_value"]
records = []
for i in range(2400):
    pid, cif = random.choice(customers)
    dirty_type = dirty_types[i % 6]   # 400 of each type
    records.append(make_dirty_txn(pid, cif, dirty_type))

df = pd.DataFrame(records)
out_path = f"{VOLUME_DATA}/batch_v1/transactions/dirty_batch_001.csv"
os.makedirs(f"{VOLUME_DATA}/batch_v1/transactions", exist_ok=True)
df.to_csv(out_path, index=False)

# Summary
print(f"✅ Written {len(df)} dirty records → {out_path}")
print("\nBreakdown by dirty type:")
for dt in dirty_types:
    n = df.apply(lambda r: (
        (r['payment_transaction_id'] is None or pd.isna(r['payment_transaction_id'])) if dt=='null_id' else
        (r['payment_amount'] < 0) if dt=='negative_amount' else
        (r['currency_id'] not in ['MYR','USD','SGD','EUR','GBP']) if dt=='invalid_currency' else
        (r['instruction_status'] not in ['settled','pending','rejected','cancelled','reversed']) if dt=='invalid_status' else
        (r['payment_rail_type'] not in ['FPX','IBG','DuitNow','SWIFT','ATM','branch','card','internal']) if dt=='invalid_rail' else
        (r['posting_date'] < r['value_date'])
    ), axis=1).sum()
    pct = 100*n/len(df)
    action = "DROP" if dt in ("null_id","negative_amount") else "ALLOW+flag"
    print(f"  {dt:<25} → {n:>4} records ({pct:.1f}%)  [{action}]")

print(f"\n📊 ~{100*400/len(df):.1f}% DROP rate (null_id + negative_amount)")
print(f"📊 ~{100*1600/len(df):.1f}% ALLOW+flagged (currency/status/rail/posting)")
