from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
import os, re
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Alliance Bank Customer Intelligence API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

w = WorkspaceClient()

CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "rfp_presentation"
FULL    = f"{CATALOG}.{SCHEMA}"

FEATURE_COLS = [
    "relationship_tenure_years", "total_deposit_balance_myr", "num_accounts",
    "annual_income_amount", "net_worth_band_encoded", "ctos_score",
    "ccris_status_encoded", "payment_conduct_score", "age",
    "number_of_dependents", "employment_status_encoded", "monthly_loan_commitment_myr",
    "num_loan_facilities", "txn_count_30d", "avg_txn_amount_myr", "has_credit_card",
    "digital_maturity_score", "telco_arpu_myr", "is_shariah_preferred_int",
    "last_product_category_viewed_encoded",
]
CLASSES = ["CREDIT_CARD", "HOME_LOAN", "INSURANCE", "INVESTMENT", "NO_ACTION", "PERSONAL_LOAN"]

PRODUCT_MAP = {
    "CREDIT_CARD":   "Alliance Visa Platinum Credit Card",
    "PERSONAL_LOAN": "Alliance CashFirst Personal Financing-i",
    "HOME_LOAN":     "Alliance HomeSmart Financing",
    "INVESTMENT":    "Alliance WealthSmart Income Fund",
    "INSURANCE":     "Alliance CarStar Takaful",
}

_warehouse_id: Optional[str] = None


def _username_slug() -> str:
    ctx = os.environ.get("DATABRICKS_USERNAME", "demo")
    return re.sub(r"[^a-zA-Z0-9_]", "_", ctx)[:20]


def _endpoint_name() -> str:
    return f"dbx-product-recommendation-{_username_slug()}"


def _glm_service() -> str:
    return f"dbx-glm-gateway-{_username_slug()}"


def _get_warehouse() -> str:
    global _warehouse_id
    if _warehouse_id:
        return _warehouse_id
    warehouses = list(w.warehouses.list())
    running = [wh for wh in warehouses if wh.state and "RUNNING" in str(wh.state)]
    wh = (running[0] if running else warehouses[0]) if warehouses else None
    _warehouse_id = wh.id if wh else None
    return _warehouse_id


def _rows(sql: str, cols: Optional[list] = None) -> list:
    """Execute SQL and return list of dicts. Returns [] on any error."""
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=_get_warehouse(), statement=sql, wait_timeout="30s"
        )
        if not result.result or not result.result.data_array:
            return []
        if cols is None:
            cols = [c.name for c in result.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in result.result.data_array]
    except Exception:
        return []


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "Alliance Bank Customer Intelligence API"}


# ── Customer list (top 20 active) ─────────────────────────────────────────────
@app.get("/api/customers")
def list_customers():
    sql = f"""
        SELECT party_id, cif_number, legal_name, lifestyle_segment,
               total_deposit_balance_myr, ctos_score, primary_state
        FROM {FULL}.gold_customer_360
        WHERE lifecycle_status = 'active'
        ORDER BY total_deposit_balance_myr DESC
        LIMIT 20
    """
    return _rows(sql)


# ── Customer search ───────────────────────────────────────────────────────────
@app.get("/api/customers/search")
def search_customers(q: str = "", limit: int = 10):
    if not q:
        return []
    safe_q = q.replace("'", "''")
    sql = f"""
        SELECT party_id, cif_number, legal_name, lifestyle_segment,
               total_deposit_balance_myr, ctos_score, primary_state
        FROM {FULL}.gold_customer_360
        WHERE lifecycle_status = 'active'
          AND (legal_name ILIKE '%{safe_q}%' OR cif_number ILIKE '%{safe_q}%')
        ORDER BY total_deposit_balance_myr DESC
        LIMIT {limit}
    """
    return _rows(sql)


# ── Customer 360 profile ──────────────────────────────────────────────────────
@app.get("/api/customer/{party_id}")
def get_customer_360(party_id: str):
    rows = _rows(f"SELECT * FROM {FULL}.gold_customer_360 WHERE party_id = '{party_id}'")
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found")
    return rows[0]


# ── Batch recommendations ─────────────────────────────────────────────────────
@app.get("/api/customer/{party_id}/recommendations")
def get_recommendations(party_id: str):
    sql = f"""
        SELECT recommendation_1, confidence_1, recommendation_2, confidence_2
        FROM {FULL}.gold_product_recommendations
        WHERE party_id = '{party_id}'
    """
    rows = _rows(sql)
    if not rows:
        raise HTTPException(status_code=404, detail="Recommendations not found")
    rec = rows[0]
    rec["confidence_1"] = round(float(rec.get("confidence_1") or 0), 1)
    rec["confidence_2"] = round(float(rec.get("confidence_2") or 0), 1)
    return rec


# ── Live scoring via model serving endpoint ───────────────────────────────────
@app.post("/api/customer/{party_id}/recommend-live")
def recommend_live(party_id: str):
    rows = _rows(f"SELECT * FROM {FULL}.gold_customer_360 WHERE party_id = '{party_id}'")
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found")
    features = rows[0]

    feature_vector = []
    for col in FEATURE_COLS:
        raw = features.get(col, 0)
        try:
            feature_vector.append(float(raw or 0))
        except (TypeError, ValueError):
            feature_vector.append(0.0)

    payload = {
        "dataframe_split": {
            "columns": FEATURE_COLS,
            "data":    [feature_vector],
        }
    }
    try:
        resp  = w.serving_endpoints.query(name=_endpoint_name(), request=payload)
        probs = resp.predictions[0] if resp.predictions else [1.0 / len(CLASSES)] * len(CLASSES)
        ranked = sorted(zip(CLASSES, probs), key=lambda x: x[1], reverse=True)
        return {
            "party_id":         party_id,
            "live":             True,
            "recommendation_1": ranked[0][0],
            "confidence_1":     round(float(ranked[0][1]) * 100, 1),
            "recommendation_2": ranked[1][0],
            "confidence_2":     round(float(ranked[1][1]) * 100, 1),
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Model endpoint error: {str(e)}")


# ── Products catalog ──────────────────────────────────────────────────────────
@app.get("/api/products")
def get_products():
    sql = f"""
        SELECT product_code, product_name, product_category,
               key_benefit_1, key_benefit_2, key_benefit_3,
               min_amount_myr, max_amount_myr, shariah_compliant
        FROM {FULL}.silver_product_catalog
    """
    return _rows(sql)


# ── Email drafting via GLM 5.2 / Unity AI Gateway ────────────────────────────
class EmailRequest(BaseModel):
    party_id:       str
    product:        Optional[str] = None
    confidence:     float = 0.0
    customer_name:  Optional[str] = None
    segment:        Optional[str] = None
    tenure:         Optional[float] = None
    language:       Optional[str] = "en"
    # Legacy alias from old frontend
    recommendation: Optional[str] = None


@app.post("/api/draft-email")
def draft_email(req: EmailRequest):
    product      = req.product or req.recommendation or "CREDIT_CARD"
    product_name = PRODUCT_MAP.get(product, product)

    customer_name = req.customer_name
    segment       = req.segment
    tenure        = req.tenure

    # Supplement from DB if fields not supplied
    if not customer_name or segment is None or tenure is None:
        try:
            c = get_customer_360(req.party_id)
            customer_name = customer_name or c.get("legal_name", "Valued Customer")
            segment       = segment or c.get("lifestyle_segment", "retail")
            tenure        = tenure if tenure is not None else c.get("relationship_tenure_years", 0)
        except HTTPException:
            customer_name = customer_name or "Valued Customer"
            segment       = segment or "retail"
            tenure        = tenure or 0

    seg_label  = str(segment).replace("_", " ").title()
    lang_instr = "in Bahasa Malaysia" if req.language == "ms" else "in English"

    prompt = f"""You are an Alliance Bank Malaysia relationship manager writing to a valued customer.

Customer Profile:
- Name: {customer_name}
- Segment: {seg_label}
- Tenure: {tenure} years with Alliance Bank
- Recommended product: {product_name} (model confidence: {req.confidence:.0f}%)

Write a short, warm, professional email {lang_instr} (maximum 150 words).
- Address the customer by name
- Reference their loyalty and tenure naturally
- Briefly introduce the recommended product and 1-2 key benefits
- End with a friendly call to action (schedule a meeting or call)
- DO NOT include specific interest rates, specific financial advice, account numbers, or IC numbers
- Sign off as: Warm regards, Your Alliance Bank Relationship Team"""

    payload = {
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  350,
        "temperature": 0.7,
    }

    try:
        resp = w.serving_endpoints.query(name=_glm_service(), request=payload)
        if hasattr(resp, "choices") and resp.choices:
            email_text = resp.choices[0].message.content
        elif isinstance(resp, dict):
            email_text = resp.get("choices", [{}])[0].get("message", {}).get("content", "")
        else:
            email_text = str(resp)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"GLM gateway error: {str(e)}")

    return {
        "party_id":     req.party_id,
        "product":      product,
        "product_name": product_name,
        "email_draft":  email_text,
        "model":        "system.ai.databricks-glm-5-2",
    }


# ── FMAPI: Explain why a product is recommended ───────────────────────────────
class ExplainRequest(BaseModel):
    party_id:      str
    product:       str
    confidence:    float = 0.0
    recommendation_rank: int = 1   # 1 = primary, 2 = secondary


@app.post("/api/explain-recommendation")
def explain_recommendation(req: ExplainRequest):
    """
    Uses GLM 5.2 (via FMAPI / Unity AI Gateway) to explain in plain English
    why this product was recommended for this customer, based on their 360 profile.
    """
    product_name = PRODUCT_MAP.get(req.product, req.product)

    # Fetch customer profile
    try:
        c = get_customer_360(req.party_id)
    except HTTPException:
        raise HTTPException(status_code=404, detail="Customer not found")

    name          = c.get("legal_name", "the customer")
    segment       = c.get("lifestyle_segment", "")
    income        = c.get("annual_income_amount", 0)
    ctos          = c.get("ctos_score", "N/A")
    tenure        = c.get("relationship_tenure_years", 0)
    has_card      = c.get("has_credit_card", False)
    has_home_loan = c.get("has_home_loan", False)
    has_personal  = c.get("has_personal_loan", False)
    commitment    = c.get("monthly_loan_commitment_myr", 0)
    net_worth     = c.get("net_worth_band", "")
    digital_score = c.get("digital_maturity_score", 0)
    product_views = c.get("last_product_category_viewed", "")

    prompt = f"""You are an Alliance Bank AI advisor. A machine learning model has recommended "{product_name}" for a customer with the following profile:

Customer Profile:
- Name: {name} | Segment: {segment} | Tenure: {tenure} years
- Annual Income: MYR {income:,.0f} | Net Worth Band: {net_worth}
- CTOS Credit Score: {ctos}/850
- Existing Products: Credit Card={has_card}, Home Loan={has_home_loan}, Personal Loan={has_personal}
- Monthly Loan Commitment: MYR {commitment:,.0f}
- Digital Maturity Score: {digital_score}/10
- Last Product Viewed: {product_views}

Model confidence: {req.confidence:.0f}%

In 2-3 concise sentences, explain to a bank relationship manager WHY "{product_name}" was recommended for this specific customer. Reference 2-3 specific profile attributes that made this recommendation. Be factual and data-driven. Do not use financial jargon."""

    payload = {
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  200,
        "temperature": 0.3,
    }

    try:
        # Try AI Gateway service first, fall back to FMAPI direct
        try:
            resp = w.serving_endpoints.query(name=_glm_service(), request=payload)
        except Exception:
            resp = w.serving_endpoints.query(name="databricks-glm-5-2", request=payload)

        if hasattr(resp, "choices") and resp.choices:
            explanation = resp.choices[0].message.content
        elif isinstance(resp, dict):
            explanation = resp.get("choices", [{}])[0].get("message", {}).get("content", "")
        else:
            explanation = str(resp)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"FMAPI error: {str(e)}")

    return {
        "party_id":     req.party_id,
        "product":      req.product,
        "product_name": product_name,
        "confidence":   req.confidence,
        "explanation":  explanation,
        "model":        "system.ai.databricks-glm-5-2",
    }


# ── Serve React frontend ──────────────────────────────────────────────────────
if os.path.exists("frontend/dist"):
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="frontend")
