from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
import os, re
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="DBX Bank Customer Intelligence API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

w = WorkspaceClient()

# ── Inference helper — uses SDK's api_client which handles OAuth token refresh ──
def _infer(endpoint_name: str, payload: dict) -> dict:
    """Call a Databricks serving endpoint via the SDK's API client.
    This correctly handles M2M OAuth used by Databricks Apps."""
    return w.api_client.do(
        "POST",
        f"/serving-endpoints/{endpoint_name}/invocations",
        body=payload,
    )


def _extract_glm_text(result: dict) -> str:
    """Robustly extract text from a GLM / chat-completion response.
    Handles: standard OpenAI format, GLM thinking models (reasoning_content),
    flash models, predictions format."""
    if not isinstance(result, dict):
        return str(result)
    choices = result.get("choices") or []
    if choices:
        choice = choices[0] if isinstance(choices, list) else choices
        if isinstance(choice, dict):
            msg = choice.get("message") or {}
            if isinstance(msg, dict):
                # Standard content field
                content = msg.get("content") or ""
                if content and str(content).strip():
                    return str(content).strip()
                # GLM thinking models: reasoning_content has the actual answer
                reasoning = msg.get("reasoning_content") or ""
                if reasoning and str(reasoning).strip():
                    # Extract the last substantive paragraph (after thinking steps)
                    lines = [l.strip() for l in str(reasoning).split('\n') if l.strip()]
                    # Look for email-like content (lines with Dear/Subject/Regards)
                    email_lines = []
                    in_email = False
                    for line in lines:
                        if any(kw in line for kw in ['Dear ', 'Subject:', 'Hi ', 'Greetings']):
                            in_email = True
                        if in_email:
                            email_lines.append(line)
                        if in_email and any(kw in line for kw in ['Regards', 'Sincerely', 'Warm regards', 'Best']):
                            break
                    if email_lines:
                        return '\n'.join(email_lines)
                    return '\n'.join(lines[-15:])  # last 15 lines
    # Predictions format
    preds = result.get("predictions") or []
    if preds:
        return str(preds[0])
    return ""

CATALOG = "fevm_master_classic_marcus_catalog"
SCHEMA  = "rfp_presentation"
FULL    = f"{CATALOG}.{SCHEMA}"

FEATURE_COLS = [
    "tenure_years", "total_deposit_balance_myr", "num_accounts",
    "annual_income_amount", "net_worth_band_encoded", "ctos_score",
    "ccris_status_encoded", "payment_conduct_score", "age",
    "number_of_dependents", "employment_status_encoded", "monthly_loan_commitment_myr",
    "num_loan_facilities", "txn_count_30d", "avg_txn_amount_myr", "has_credit_card",
    "digital_maturity_score", "telco_arpu_myr", "is_shariah_preferred_int",
    "last_product_category_viewed_encoded",
]
CLASSES = ["CREDIT_CARD", "HOME_LOAN", "INSURANCE", "INVESTMENT", "NO_ACTION", "PERSONAL_LOAN"]

PRODUCT_MAP = {
    "CREDIT_CARD":   "DBX Credit Card",
    "PERSONAL_LOAN": "DBX Personal Financing",
    "HOME_LOAN":     "DBX Home Financing",
    "INVESTMENT":    "DBX WealthSmart Fund",
    "INSURANCE":     "DBX Takaful Protection",
}

_warehouse_id: Optional[str] = None

# ── Endpoint names — override via env vars for portability ────────────────────
# RECOMMENDATION_ENDPOINT: the XGBoost model serving endpoint name
# GLM_ENDPOINT: use workspace FMAPI directly (databricks-glm-5-2 is always available)
RECOMMENDATION_ENDPOINT = os.environ.get("RECOMMENDATION_ENDPOINT", "dbx-product-rec-marcus-tay")
GLM_ENDPOINT            = os.environ.get("GLM_ENDPOINT",            "databricks-glm-5-3-flash")  # flash model: fast, no thinking overhead


def _endpoint_name() -> str:
    return RECOMMENDATION_ENDPOINT


def _glm_service() -> str:
    return GLM_ENDPOINT


_FALLBACK_WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "637b124ddfbe2377")

def _get_warehouse() -> str:
    global _warehouse_id
    if _warehouse_id:
        return _warehouse_id
    try:
        warehouses = list(w.warehouses.list())
        running = [wh for wh in warehouses if wh.state and "RUNNING" in str(wh.state)]
        wh = (running[0] if running else warehouses[0]) if warehouses else None
        _warehouse_id = wh.id if wh else _FALLBACK_WAREHOUSE
    except Exception:
        _warehouse_id = _FALLBACK_WAREHOUSE
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
    return {"status": "ok", "service": "DBX Bank Customer Intelligence API"}


# ── Debug (temporary) ──────────────────────────────────────────────────────────
@app.get("/api/debug")
def debug():
    """Debug endpoint to diagnose warehouse connectivity."""
    result = {"warehouse_id": None, "error": None, "test_query": None,
              "recommendation_endpoint": RECOMMENDATION_ENDPOINT,
              "glm_endpoint": GLM_ENDPOINT,
              "databricks_username": os.environ.get("DATABRICKS_USERNAME","not_set")}
    try:
        result["warehouse_id"] = _get_warehouse()
        test = w.statement_execution.execute_statement(
            warehouse_id=result["warehouse_id"],
            statement="SELECT 1 as test",
            wait_timeout="30s"
        )
        result["test_query"] = "OK" if test.result and test.result.data_array else "no_result"
    except Exception as e:
        result["error"] = str(e)[:500]
    # Test recommendation endpoint
    try:
        ep = w.serving_endpoints.get(name=RECOMMENDATION_ENDPOINT)
        result["rec_endpoint_state"] = str(ep.state.ready) if ep.state else "unknown"
    except Exception as e:
        result["rec_endpoint_state"] = f"ERROR: {str(e)[:200]}"
    return result


# ── Customer overview — with optional recommendations join ─────────────────────
_recs_table_exists: Optional[bool] = None

def _check_recs_table(force_recheck: bool = False) -> bool:
    """Check whether gold_product_recommendations exists. Rechecks if not found (table may appear after Part B)."""
    global _recs_table_exists
    if _recs_table_exists and not force_recheck:
        return True
    rows = _rows(f"SHOW TABLES IN {FULL} LIKE 'gold_product_recommendations'")
    _recs_table_exists = len(rows) > 0
    return _recs_table_exists


@app.get("/api/status")
def get_status():
    """Health + readiness check — shows which tables are available."""
    return {
        "status": "ok",
        "gold_customer_360": len(_rows(f"SELECT COUNT(*) FROM {FULL}.gold_customer_360")) > 0,
        "gold_product_recommendations": _check_recs_table(force_recheck=True),
    }


@app.get("/api/customers")
def list_customers(limit: int = 200):
    """Returns all active customers; joins recommendations if table exists."""
    has_recs = _check_recs_table()

    if has_recs:
        sql = f"""
            SELECT
                c.party_id, c.cif_number, c.legal_name, c.lifestyle_segment,
                c.customer_segment, c.primary_state, c.lifecycle_status,
                c.total_deposit_balance_myr, c.ctos_score,
                c.relationship_tenure_years, c.annual_income_amount,
                c.age, c.number_of_dependents, c.employment_status,
                c.mobile_app_user_flag, c.digital_maturity_score,
                c.is_shariah_preferred, c.nps_score,
                COALESCE(r.recommendation_1, 'PENDING') AS recommendation_1,
                COALESCE(CAST(r.confidence_1 AS STRING), '0') AS confidence_1
            FROM {FULL}.gold_customer_360 c
            LEFT JOIN {FULL}.gold_product_recommendations r ON c.party_id = r.party_id
            WHERE c.lifecycle_status = 'active'
            ORDER BY c.total_deposit_balance_myr DESC
            LIMIT {limit}
        """
    else:
        # Recommendations table not yet available (Part B pipeline still running)
        sql = f"""
            SELECT
                party_id, cif_number, legal_name, lifestyle_segment,
                customer_segment, primary_state, lifecycle_status,
                total_deposit_balance_myr, ctos_score,
                relationship_tenure_years, annual_income_amount,
                age, number_of_dependents, employment_status,
                mobile_app_user_flag, digital_maturity_score,
                is_shariah_preferred, nps_score,
                'PENDING' AS recommendation_1,
                '0' AS confidence_1
            FROM {FULL}.gold_customer_360
            WHERE lifecycle_status = 'active'
            ORDER BY total_deposit_balance_myr DESC
            LIMIT {limit}
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
    if not _check_recs_table():
        raise HTTPException(status_code=404, detail="Recommendations table not available — Part B pipeline still running")
    sql = f"""
        SELECT recommendation_1, confidence_1, recommendation_2, confidence_2
        FROM {FULL}.gold_product_recommendations
        WHERE party_id = '{party_id}'
    """
    rows = _rows(sql)
    if not rows:
        raise HTTPException(status_code=404, detail="No recommendations found for this customer")
    rec = rows[0]
    rec["confidence_1"] = round(float(rec.get("confidence_1") or 0), 1)
    rec["confidence_2"] = round(float(rec.get("confidence_2") or 0), 1)
    return rec


# ── Live scoring via model serving endpoint ───────────────────────────────────
@app.post("/api/customer/{party_id}/recommend-live")
def recommend_live(party_id: str):
    # Query customer_features — contains pre-encoded columns matching model training input
    rows = _rows(f"""
        SELECT {', '.join(FEATURE_COLS)}
        FROM {FULL}.customer_features
        WHERE party_id = '{party_id}'
    """)
    if not rows:
        # Fallback: try computing from gold_customer_360 with basic encoding
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
        "dataframe_records": [dict(zip(FEATURE_COLS, feature_vector))]
    }
    try:
        result = _infer(_endpoint_name(), payload)
        # Handle both dataframe_records and predictions response formats
        probs_raw = result.get("predictions") or result.get("outputs") or []
        if probs_raw and isinstance(probs_raw[0], list):
            probs = probs_raw[0]
        elif probs_raw and isinstance(probs_raw[0], dict):
            # predictions as list of dicts with class probabilities
            probs = [probs_raw[0].get(c, 0.0) for c in CLASSES]
        else:
            probs = [1.0 / len(CLASSES)] * len(CLASSES)
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

    prompt = f"""You are an DBX Bank Malaysia relationship manager writing to a valued customer.

Customer Profile:
- Name: {customer_name}
- Segment: {seg_label}
- Tenure: {tenure} years with DBX Bank
- Recommended product: {product_name} (model confidence: {req.confidence:.0f}%)

Write a short, warm, professional email {lang_instr} (maximum 150 words).
- Address the customer by name
- Reference their loyalty and tenure naturally
- Briefly introduce the recommended product and 1-2 key benefits
- End with a friendly call to action (schedule a meeting or call)
- DO NOT include specific interest rates, specific financial advice, account numbers, or IC numbers
- Sign off as: Warm regards, Your DBX Bank Relationship Team"""

    payload = {
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  350,
        "temperature": 0.7,
    }

    try:
        result_raw = _infer(_glm_service(), payload)
        email_text = _extract_glm_text(result_raw)
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

    def _f(v, default=0.0):
        try: return float(v or 0)
        except: return default

    name          = c.get("legal_name", "the customer") or "the customer"
    segment       = c.get("lifestyle_segment", "") or ""
    income        = _f(c.get("annual_income_amount"))
    ctos          = c.get("ctos_score") or "N/A"
    tenure        = _f(c.get("relationship_tenure_years"))
    has_card      = bool(c.get("has_credit_card", False))
    has_home_loan = bool(c.get("has_home_loan", False))
    has_personal  = bool(c.get("has_personal_loan", False))
    commitment    = _f(c.get("monthly_loan_commitment_myr"))
    net_worth     = c.get("net_worth_band", "") or ""
    digital_score = _f(c.get("digital_maturity_score"))
    product_views = c.get("last_product_category_viewed", "") or ""

    prompt = f"""You are an DBX Bank AI advisor. A machine learning model has recommended "{product_name}" for a customer with the following profile:

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
        # Try GLM service first, fall back to direct FMAPI endpoint
        try:
            result_raw = _infer(_glm_service(), payload)
        except Exception:
            result_raw = _infer("databricks-glm-5-2", payload)
        explanation = _extract_glm_text(result_raw)
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


# ── Streaming stats — row counts for bronze streaming tables ──────────────────
@app.get("/api/streaming-stats")
def streaming_stats():
    """Row counts for streaming bronze tables — used by live viz."""
    results = {}
    for table in ["bronze_digital_events", "bronze_credit_bureau", "bronze_telco_events"]:
        count_rows = _rows(f"SELECT COUNT(*) as n FROM {FULL}.{table}")
        results[table] = int(count_rows[0]["n"]) if count_rows else 0
    return results


# ── Table stats — row counts for all key lineage tables ───────────────────────
@app.get("/api/table-stats")
def table_stats():
    """Row counts for all key tables in the lineage."""
    tables = [
        "gold_customer_360", "gold_product_recommendations",
        "silver_customers", "silver_transactions", "silver_deposit_accounts",
        "customer_features",
        "bronze_digital_events", "bronze_credit_bureau", "bronze_telco_events",
        "bronze_customer_master", "bronze_core_banking_txn",
    ]
    results = {}
    for t in tables:
        rows = _rows(f"SELECT COUNT(*) as n FROM {FULL}.{t}")
        results[t] = int(rows[0]["n"]) if rows else -1
    return results


# ── Serve React frontend ──────────────────────────────────────────────────────
if os.path.exists("frontend/dist"):
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="frontend")
