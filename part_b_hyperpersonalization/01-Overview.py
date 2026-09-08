# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Part B — Hyperpersonalization: Next Best Product Recommendation
# MAGIC ## DBX Bank × Databricks — ML Lifecycle Demo
# MAGIC
# MAGIC DBX Bank serves 1,000+ customer profiles generated in Part A.
# MAGIC Part B trains a **multi-class classifier** to predict the next product each customer
# MAGIC should be offered — then deploys it as a **real-time serving endpoint** with full
# MAGIC **MLflow governance**, **feature store**, **drift monitoring**, and **CI/CD automation**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC  ┌──────────────────────────────────────────────────────────────────────────────┐
# MAGIC  │  PART A (Data Platform)              PART B (AI/ML Platform)                │
# MAGIC  │                                                                              │
# MAGIC  │  gold_customer_360 ──────────────► Feature Engineering ──► Feature Store    │
# MAGIC  │  silver_product_catalog             (03-Feature-Engineering)                │
# MAGIC  │                                              │                               │
# MAGIC  │                                              ▼                               │
# MAGIC  │                                     Model Training                          │
# MAGIC  │                                  RF vs XGBoost → Winner                    │
# MAGIC  │                                  (04-Model-Training)                        │
# MAGIC  │                                              │                               │
# MAGIC  │                                   ┌──────────┴──────────┐                  │
# MAGIC  │                                   ▼                      ▼                  │
# MAGIC  │                            Batch Inference        Real-Time Serving         │
# MAGIC  │                           (05-Batch-Inference)  (06-Real-Time-Inference)    │
# MAGIC  │                                   │                      │                  │
# MAGIC  │                                   └──────────┬──────────┘                  │
# MAGIC  │                                              ▼                               │
# MAGIC  │                                       Observability                         │
# MAGIC  │                                   SHAP + Drift Monitor                     │
# MAGIC  │                                    (07-Observability)                       │
# MAGIC  │                                              │                               │
# MAGIC  │                                              ▼                               │
# MAGIC  │                                      CI/CD Automation                      │
# MAGIC  │                                  Evaluate → Approve → Deploy               │
# MAGIC  │                                 (08-Continuous-Deployment)                  │
# MAGIC  │                                              │                               │
# MAGIC  │                                              ▼                               │
# MAGIC  │                                    Unity AI Gateway                         │
# MAGIC  │                                  GLM 5.2 Email Drafting                    │
# MAGIC  │                                  (09-Unity-AI-Gateway)                      │
# MAGIC  └──────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## Notebook Agenda

# COMMAND ----------
displayHTML("""
<style>
  body { font-family: 'DM Sans', sans-serif; }
  .agenda-table { border-collapse: collapse; width: 100%; margin: 20px 0; }
  .agenda-table th {
    background: #FF3621;
    color: white;
    padding: 12px 16px;
    text-align: left;
    font-size: 14px;
  }
  .agenda-table td {
    padding: 11px 16px;
    border-bottom: 1px solid #e8e8e8;
    vertical-align: top;
    font-size: 13px;
  }
  .agenda-table tr:nth-child(even) td { background: #f9f9f9; }
  .agenda-table tr:hover td { background: #fff3f0; }
  .badge {
    display: inline-block;
    border-radius: 4px;
    padding: 2px 8px;
    font-size: 11px;
    font-weight: 600;
    color: white;
  }
  .badge-ml     { background: #1B6FDB; }
  .badge-gov    { background: #28A745; }
  .badge-infra  { background: #6C757D; }
  .badge-cicd   { background: #FF3621; }
  .stage { font-weight: 700; color: #222; }
</style>

<h2 style="color:#FF3621; font-family:DM Sans,sans-serif;">
  Databricks ML Lifecycle — DBX Bank Hyperpersonalization
</h2>

<table class="agenda-table">
  <thead>
    <tr>
      <th>#</th>
      <th>Notebook</th>
      <th>Stage</th>
      <th>What it demonstrates</th>
      <th>Databricks product</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>02</td>
      <td><span class="stage">02-EDA</span></td>
      <td><span class="badge badge-ml">Data</span></td>
      <td>Segment distribution, income spread, CTOS by segment, product ownership heatmap</td>
      <td>Spark SQL, display(), Genie Code</td>
    </tr>
    <tr>
      <td>03</td>
      <td><span class="stage">03-Feature-Engineering</span></td>
      <td><span class="badge badge-ml">Feature Store</span></td>
      <td>Encode categoricals, generate next_best_product label with business rules, publish to Feature Store</td>
      <td>FeatureEngineeringClient, Unity Catalog, Delta</td>
    </tr>
    <tr>
      <td>04</td>
      <td><span class="stage">04-Model-Training</span></td>
      <td><span class="badge badge-ml">Training</span></td>
      <td>RF vs XGBoost competition, winner selected by macro F1, registered with feature lineage</td>
      <td>MLflow, AutoML-style competition, Feature Store</td>
    </tr>
    <tr>
      <td>05</td>
      <td><span class="stage">05-Batch-Inference</span></td>
      <td><span class="badge badge-ml">Inference</span></td>
      <td>Score all 1,000 customers, write inference log, create LHM monitor, inject drift rows</td>
      <td>fe.score_batch(), Lakehouse Monitoring, CDF</td>
    </tr>
    <tr>
      <td>06</td>
      <td><span class="stage">06-Real-Time-Inference</span></td>
      <td><span class="badge badge-ml">Serving</span></td>
      <td>Deploy champion to Model Serving, enable AI Gateway inference logging, query live endpoint</td>
      <td>Model Serving, AI Gateway, mlflow.deployments</td>
    </tr>
    <tr>
      <td>07</td>
      <td><span class="stage">07-Observability</span></td>
      <td><span class="badge badge-gov">Governance</span></td>
      <td>SHAP feature importance, prediction distribution, payload unpack → eval table, drift monitor</td>
      <td>SHAP, Lakehouse Monitoring, system tables</td>
    </tr>
    <tr>
      <td>07-DG</td>
      <td><span class="stage">07-Drift-Gate</span></td>
      <td><span class="badge badge-gov">Governance</span></td>
      <td>Standalone drift-check task: reads monitor metrics, emits retrain_needed task value</td>
      <td>Lakehouse Monitoring, Jobs task values</td>
    </tr>
    <tr>
      <td>08</td>
      <td><span class="stage">08-Continuous-Deployment</span></td>
      <td><span class="badge badge-cicd">CI/CD</span></td>
      <td>3-task job: Evaluate → Approve (F1 gate) → Deploy; UC model trigger wires new version → auto-run</td>
      <td>Databricks Jobs, UC model triggers</td>
    </tr>
    <tr>
      <td>09</td>
      <td><span class="stage">09-Unity-AI-Gateway</span></td>
      <td><span class="badge badge-gov">AI Governance</span></td>
      <td>GLM 5.2 email drafting service, rate limits, banking guardrails, traffic split, audit trail</td>
      <td>Unity AI Gateway, system.ai_gateway.usage</td>
    </tr>
    <tr>
      <td>99</td>
      <td><span class="stage">99-Load-Test-Endpoint</span></td>
      <td><span class="badge badge-infra">Operations</span></td>
      <td>200-customer load test, 5 rounds, drift injection, per-round MLflow metrics, monitor refresh</td>
      <td>Model Serving, MLflow, Lakehouse Monitoring</td>
    </tr>
  </tbody>
</table>

<p style="color:#666; font-size:12px; margin-top: 8px;">
  Run notebooks in order: 03 → 04 → 05 → 06 → 07 → 08 → 09 → 99<br/>
  <strong>Prerequisite:</strong> Part A gold_customer_360 must exist in
  <code>fevm_master_classic_marcus_catalog.rfp_presentation</code>
</p>
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quick Dependency Check

# COMMAND ----------
try:
    cnt = spark.table(f"{da.full_schema}.gold_customer_360").count()
    assert cnt >= 800, f"Expected >= 800 rows, got {cnt}"
    print(f"Part A dependency satisfied: gold_customer_360 has {cnt} rows")
except Exception as e:
    raise RuntimeError(
        f"Part A gold_customer_360 not found or insufficient data.\n"
        f"Run Part A notebooks first.\n{e}"
    )

print(f"Catalog:   {da.catalog}")
print(f"Schema:    {da.schema}")
print(f"Model:     {da.model_name}")
print(f"Endpoint:  {da.endpoint_name}")
