from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
import os, re
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="DBX Bank Customer 360 + Recommendation API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

w = WorkspaceClient()

CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "rfp_presentation"
FULL    = f"{CATALOG}.{SCHEMA}"


def _get_endpoint_name():
    ctx   = os.environ.get("DATABRICKS_USERNAME", "demo")
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ctx)[:20]
    return f"abmb-product-recommendation-{clean}"


def _get_glm_service():
    ctx   = os.environ.get("DATABRICKS_USERNAME", "demo")
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ctx)[:20]
    return f"abmb-glm-email-service-{clean}"


def _get_warehouse():
    warehouses = list(w.warehouses.list())
    running    = [wh for wh in warehouses if wh.state and "RUNNING" in str(wh.state)]
    if running:
        return running[0].id
    return warehouses[0].id if warehouses else None


def _exec(sql: str):
    return w.statement_execution.execute_statement(
        warehouse_id=_get_warehouse(), statement=sql, wait_timeout="30s"
    )


# ── Health ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "DBX Bank Customer 360 API"}


# ── Customer search ──────────────────────────────────────────────────────────
@app.get("/api/customers/search")
def search_customers(q: str = "", limit: int = 10):
    sql = f"""
        SELECT party_id, cif_number, legal_name, lifestyle_segment,
               primary_state, relationship_tenure_years, ctos_score
        FROM {FULL}.gold_customer_360
        WHERE lifecycle_status = 'active'
          AND (legal_name ILIKE '%{q}%' OR cif_number ILIKE '%{q}%')
        ORDER BY total_deposit_balance_myr DESC
        LIMIT {limit}
    """
    result = _exec(sql)
    cols   = ["party_id","cif_number","legal_name","lifestyle_segment",
              "primary_state","relationship_tenure_years","ctos_score"]
    return [dict(zip(cols, r)) for r in (result.result.data_array or [])]


# ── Customer 360 ──────────────────────────────────────────────────────────────
@app.get("/api/customer/{party_id}")
def get_customer_360(party_id: str):
    result = _exec(f"SELECT * FROM {FULL}.gold_customer_360 WHERE party_id = '{party_id}'")
    if not result.result.data_array:
        raise HTTPException(status_code=404, detail="Customer not found")
    cols = [c.name for c in result.manifest.schema.columns]
    return dict(zip(cols, result.result.data_array[0]))


# ── Product recommendations ──────────────────────────────────────────────────
FEATURE_COLS = [
    "tenure_years","total_deposit_balance_myr","num_accounts","annual_income_amount",
    "net_worth_band_encoded","ctos_score","ccris_status_encoded","payment_conduct_score",
    "age","number_of_dependents","employment_status_encoded","monthly_loan_commitment_myr",
    "num_loan_facilities","txn_count_30d","avg_txn_amount_myr","has_credit_card",
    "digital_maturity_score","telco_arpu_myr","is_shariah_preferred_int",
    "last_product_category_viewed_encoded"
]
CLASSES = ["CREDIT_CARD","HOME_LOAN","INSURANCE","INVESTMENT","NO_ACTION","PERSONAL_LOAN"]


@app.get("/api/recommend/{party_id}")
def get_recommendations(party_id: str, live: bool = False):
    if live:
        result = _exec(f"SELECT * FROM {FULL}.customer_features WHERE party_id = '{party_id}'")
        if not result.result.data_array:
            raise HTTPException(status_code=404, detail="Customer features not found")
        cols     = [c.name for c in result.manifest.schema.columns]
        features = dict(zip(cols, result.result.data_array[0]))
        payload  = {"dataframe_split": {
            "columns": FEATURE_COLS,
            "data":    [[float(features.get(c, 0) or 0) for c in FEATURE_COLS]]
        }}
        resp   = w.serving_endpoints.query(name=_get_endpoint_name(), request=payload)
        probs  = resp.predictions[0] if resp.predictions else []
        ranked = sorted(zip(CLASSES, probs), key=lambda x: x[1], reverse=True)
        return {"party_id": party_id, "live": True,
                "recommendation_1": ranked[0][0], "confidence_1": round(ranked[0][1]*100, 1),
                "recommendation_2": ranked[1][0], "confidence_2": round(ranked[1][1]*100, 1)}
    else:
        sql    = f"""SELECT recommendation_1, confidence_1, recommendation_2, confidence_2
                     FROM {FULL}.gold_product_recommendations WHERE party_id = '{party_id}'"""
        result = _exec(sql)
        if not result.result.data_array:
            raise HTTPException(status_code=404, detail="Recommendations not found")
        cols = ["recommendation_1","confidence_1","recommendation_2","confidence_2"]
        return dict(zip(cols, result.result.data_array[0]))


# ── Products catalog ──────────────────────────────────────────────────────────
@app.get("/api/products")
def get_products():
    sql    = f"""SELECT product_code, product_name, product_category,
                        key_benefit_1, key_benefit_2, key_benefit_3,
                        min_amount_myr, max_amount_myr, shariah_compliant
                 FROM {FULL}.silver_product_catalog"""
    result = _exec(sql)
    cols   = [c.name for c in result.manifest.schema.columns]
    return [dict(zip(cols, r)) for r in (result.result.data_array or [])]


# ── Email drafting (GLM 5.2 via Unity AI Gateway) ────────────────────────────
class EmailRequest(BaseModel):
    party_id:       str
    recommendation: str
    confidence:     float
    language:       Optional[str] = "en"


PRODUCT_MAP = {
    "CREDIT_CARD":   "Alliance Visa Platinum Credit Card",
    "PERSONAL_LOAN": "Alliance CashFirst Personal Financing-i",
    "HOME_LOAN":     "Alliance HomeSmart Financing",
    "INVESTMENT":    "Alliance WealthSmart Income Fund",
    "INSURANCE":     "Alliance CarStar Takaful",
}


@app.post("/api/draft-email")
def draft_email(req: EmailRequest):
    customer     = get_customer_360(req.party_id)
    product_name = PRODUCT_MAP.get(req.recommendation, req.recommendation)
    lang_instr   = "in Bahasa Malaysia" if req.language == "ms" else "in English"

    prompt = f"""You are a DBX Bank relationship manager writing to a valued customer.

Customer Profile:
- Name: {customer.get('legal_name', 'Valued Customer')}
- Segment: {customer.get('lifestyle_segment', 'retail').replace('_', ' ').title()}
- Tenure: {customer.get('relationship_tenure_years', 0)} years with DBX Bank
- Recommended product: {product_name} (model confidence: {req.confidence:.0f}%)

Write a short, warm, professional email {lang_instr} (maximum 150 words).
- Address the customer by name
- Reference their loyalty/tenure naturally
- Briefly introduce the recommended product and 1-2 key benefits
- End with a friendly call to action (schedule a meeting or call)
- DO NOT include specific interest rates, specific financial advice, account numbers, or IC numbers
- Sign off as: Warm regards, Your DBX Bank Relationship Team"""

    payload  = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 300, "temperature": 0.7}
    resp     = w.serving_endpoints.query(name=_get_glm_service(), request=payload)

    email_text = ""
    if hasattr(resp, "choices") and resp.choices:
        email_text = resp.choices[0].message.content
    elif isinstance(resp, dict):
        email_text = resp.get("choices", [{}])[0].get("message", {}).get("content", "")

    return {"party_id": req.party_id, "recommendation": req.recommendation,
            "product_name": product_name, "email_draft": email_text,
            "model": "system.ai.databricks-glm-5-2"}


# ── Serve React frontend ──────────────────────────────────────────────────────
if os.path.exists("frontend/dist"):
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="static")
