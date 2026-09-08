# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Load Test — Alliance Bank Recommendation Endpoint
# MAGIC
# MAGIC Simulates real-world RM portal traffic and verifies endpoint behaviour under load.
# MAGIC
# MAGIC **Test plan:**
# MAGIC - Sample up to 200 customers from the feature table
# MAGIC - Send in batches of 10 over **5 rounds**
# MAGIC - Inject a drifted cohort (high-income INVESTMENT profiles) every round
# MAGIC - Log per-round latency and prediction distribution to MLflow
# MAGIC - Trigger Lakehouse Monitor refresh after all rounds
# MAGIC - Print a summary table

# COMMAND ----------
import requests
import json
import time
import datetime
import mlflow
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## 1. Load Customer Sample

# COMMAND ----------
SAMPLE_SIZE = 200
BATCH_SIZE  = 10
N_ROUNDS    = 5

feature_pdf = (
    spark.table(da.feature_table)
         .select(DA.FEATURES)
         .limit(SAMPLE_SIZE)
         .toPandas()
)

print(f"Loaded {len(feature_pdf):,} customers for load test")

# COMMAND ----------
# MAGIC %md ## 2. Resolve Endpoint URL

# COMMAND ----------
ctx     = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = ctx.apiUrl().get()
HOST    = api_url if api_url.startswith("https://") else f"https://{api_url}"
TOKEN   = ctx.apiToken().get()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

ENDPOINT_URL = f"{HOST}/serving-endpoints/{da.endpoint_name}/invocations"
print(f"Endpoint URL: {ENDPOINT_URL}")

# Quick connectivity check
probe = requests.post(
    ENDPOINT_URL,
    headers=HEADERS,
    json={"dataframe_records": [feature_pdf.iloc[0].to_dict()]},
    timeout=30
)
if probe.status_code == 200:
    print("Endpoint is reachable")
else:
    raise RuntimeError(
        f"Endpoint not reachable: {probe.status_code} — {probe.text[:300]}\n"
        f"Run 06-Real-Time-Inference first."
    )

# COMMAND ----------
# MAGIC %md ## 3. Build Drifted Cohort (INVESTMENT Profile)
# MAGIC
# MAGIC These rows simulate a sudden influx of high-net-worth customers
# MAGIC that the model may not have seen during training — triggering drift detection.

# COMMAND ----------
def make_drift_row() -> dict:
    """Generate a synthetic high-income INVESTMENT-profile customer."""
    row = {feat: 0.0 for feat in DA.FEATURES}
    row.update({
        "tenure_years":                       12.0,
        "total_deposit_balance_myr":          950000.0,
        "num_accounts":                       5,
        "annual_income_amount":               280000.0,
        "net_worth_band_encoded":             4,        # 1m_to_5m
        "ctos_score":                         810.0,
        "ccris_status_encoded":               0,        # clean
        "payment_conduct_score":              85.0,
        "age":                                48,
        "number_of_dependents":               2,
        "employment_status_encoded":          2,        # business_owner
        "monthly_loan_commitment_myr":        4000.0,
        "num_loan_facilities":                2,
        "txn_count_30d":                      45,
        "avg_txn_amount_myr":                 12000.0,
        "has_credit_card":                    1,
        "digital_maturity_score":             82.0,
        "telco_arpu_myr":                     180.0,
        "is_shariah_preferred_int":           0,
        "last_product_category_viewed_encoded": 4,     # investment
    })
    return row

drift_cohort = [make_drift_row() for _ in range(10)]
print(f"Drift cohort: {len(drift_cohort)} synthetic INVESTMENT-profile customers per round")

# COMMAND ----------
# MAGIC %md ## 4. Run Load Test — 5 Rounds

# COMMAND ----------
round_results = []

mlflow.set_experiment(da.experiment_name)

with mlflow.start_run(run_name=f"load_test_{datetime.date.today().isoformat()}") as lt_run:
    mlflow.log_param("sample_size",  SAMPLE_SIZE)
    mlflow.log_param("batch_size",   BATCH_SIZE)
    mlflow.log_param("n_rounds",     N_ROUNDS)
    mlflow.log_param("drift_cohort", len(drift_cohort))

    for rnd in range(1, N_ROUNDS + 1):
        print(f"\n--- Round {rnd}/{N_ROUNDS} ---")

        round_start       = time.time()
        round_latencies   = []
        round_predictions = []
        round_errors      = 0

        # Shuffle and batch the customer sample
        shuffled = feature_pdf.sample(frac=1, random_state=rnd).reset_index(drop=True)

        for i in range(0, len(shuffled), BATCH_SIZE):
            batch    = shuffled.iloc[i : i + BATCH_SIZE]
            records  = batch.to_dict(orient="records")

            t0   = time.time()
            resp = requests.post(
                ENDPOINT_URL,
                headers=HEADERS,
                json={"dataframe_records": records},
                timeout=30
            )
            lat_ms = (time.time() - t0) * 1000

            if resp.status_code == 200:
                result = resp.json()
                preds  = result.get("predictions", [])
                round_latencies.append(lat_ms)
                round_predictions.extend(preds if isinstance(preds, list) else [preds])
            else:
                round_errors += 1
                print(f"  Batch error {resp.status_code}: {resp.text[:100]}")

        # Inject drift cohort every round
        t0   = time.time()
        resp = requests.post(
            ENDPOINT_URL,
            headers=HEADERS,
            json={"dataframe_records": drift_cohort},
            timeout=30
        )
        drift_lat_ms = (time.time() - t0) * 1000
        if resp.status_code == 200:
            drift_preds = resp.json().get("predictions", [])
            round_predictions.extend(drift_preds if isinstance(drift_preds, list) else [drift_preds])
            print(f"  Drift cohort injected ({len(drift_cohort)} rows, {drift_lat_ms:.0f}ms)")
        else:
            print(f"  Drift cohort error: {resp.status_code}")

        # Per-round stats
        round_elapsed  = time.time() - round_start
        pred_dist      = pd.Series(round_predictions).value_counts().to_dict()
        avg_lat        = np.mean(round_latencies) if round_latencies else 0
        p95_lat        = np.percentile(round_latencies, 95) if round_latencies else 0

        print(f"  Requests:    {len(round_latencies)}")
        print(f"  Errors:      {round_errors}")
        print(f"  Avg latency: {avg_lat:.0f} ms")
        print(f"  P95 latency: {p95_lat:.0f} ms")
        print(f"  Predictions: {pred_dist}")

        # Log to MLflow
        mlflow.log_metric("avg_latency_ms", avg_lat,   step=rnd)
        mlflow.log_metric("p95_latency_ms", p95_lat,   step=rnd)
        mlflow.log_metric("errors",         round_errors, step=rnd)
        mlflow.log_metric("total_requests", len(round_latencies), step=rnd)
        for label, cnt in pred_dist.items():
            mlflow.log_metric(f"pred_{str(label).lower()}", cnt, step=rnd)

        round_results.append({
            "round":             rnd,
            "requests":          len(round_latencies),
            "errors":            round_errors,
            "avg_latency_ms":    round(avg_lat, 1),
            "p95_latency_ms":    round(p95_lat, 1),
            "elapsed_s":         round(round_elapsed, 1),
            **{f"pred_{k}": v for k, v in pred_dist.items()},
        })

    print(f"\nLoad test MLflow run: {lt_run.info.run_id}")

# COMMAND ----------
# MAGIC %md ## 5. Trigger Lakehouse Monitor Refresh

# COMMAND ----------
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

try:
    refresh = w.quality_monitors.run_refresh(table_name=da.inference_log_table)
    print(f"Monitor refresh triggered: {refresh.refresh_id}")
except Exception as e:
    print(f"Monitor refresh: {e}")

# COMMAND ----------
# MAGIC %md ## 6. Summary Table

# COMMAND ----------
summary_df = pd.DataFrame(round_results)
print("\nLoad Test Summary:")
print(summary_df.to_string(index=False))

# Totals
total_requests = summary_df["requests"].sum()
total_errors   = summary_df["errors"].sum()
overall_avg    = summary_df["avg_latency_ms"].mean()
overall_p95    = summary_df["p95_latency_ms"].max()
error_rate     = total_errors / total_requests * 100 if total_requests > 0 else 0

print(f"\nOverall Results:")
print(f"  Total requests:   {total_requests:,}")
print(f"  Total errors:     {total_errors}")
print(f"  Error rate:       {error_rate:.1f}%")
print(f"  Avg latency:      {overall_avg:.0f} ms")
print(f"  P95 latency:      {overall_p95:.0f} ms")
print(f"  Drift rows sent:  {N_ROUNDS * len(drift_cohort)}")
print(f"\nInference log:     {da.inference_log_table}")
print(f"Endpoint:          {da.endpoint_name}")
