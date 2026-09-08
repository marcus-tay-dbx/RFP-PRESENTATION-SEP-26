# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway — Governing GLM 5.2 for DBX Bank
# MAGIC
# MAGIC ## Business Story
# MAGIC DBX Bank doesn't just deploy AI — they govern it. Every AI call that drafts a
# MAGIC customer email flows through Unity AI Gateway: rate-limited, guardrailed, and fully
# MAGIC auditable. The same governance model that controls data access now controls AI access.
# MAGIC
# MAGIC ## What we configure:
# MAGIC | Section | What | Why |
# MAGIC |---|---|---|
# MAGIC | A | GLM 5.2 model service | Foundation for email drafting |
# MAGIC | B | Inference table | Every call logged for compliance |
# MAGIC | C | Rate limits | Control token spend |
# MAGIC | D | Banking guardrails | No PII in responses, no specific financial advice |
# MAGIC | E | Traffic split | GLM 5.2 (quality) vs GLM 5.3 Flash (speed) |
# MAGIC | F | Telemetry query | system.ai_gateway.usage audit trail |

# COMMAND ----------
import requests, json, time
ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_api_url = ctx.apiUrl().get()
HOST = _api_url if _api_url.startswith("https://") else f"https://{_api_url}"
TOKEN = ctx.apiToken().get()
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
username = ctx.userName().get().replace("@","_").replace(".","_")
SERVICE_NAME = f"abmb-glm-email-service-{username[:20]}"
CATALOG_NAME = "fevm_master_classic_marcus_catalog"
SCHEMA_NAME  = "abmb_rfp_presentation"

# COMMAND ----------
# MAGIC %md ## Section A: Create GLM 5.2 Model Service

# COMMAND ----------
service_payload = {
    "name": SERVICE_NAME,
    "config": {
        "served_models": [{
            "name": "glm52-primary",
            "model_name": "system.ai.databricks-glm-5-2",
            "scale_to_zero_enabled": True
        }]
    }
}
resp = requests.post(f"{HOST}/api/2.0/serving-endpoints", headers=H, json=service_payload)
print(f"Created model service: {resp.status_code} — {SERVICE_NAME}")
if resp.status_code not in [200, 201]:
    print(resp.text[:400])

# COMMAND ----------
# Quick test
test_prompt = {
    "messages": [{"role": "user", "content":
        "You are a DBX Bank RM. Draft a 100-word email to Ahmad bin Ibrahim (affluent segment, "
        "5 years tenure) recommending he considers our WealthSmart Income Fund. Professional, Malay preferred."}]
}
resp = requests.post(f"{HOST}/serving-endpoints/{SERVICE_NAME}/invocations", headers=H, json=test_prompt)
print(resp.json()["choices"][0]["message"]["content"] if resp.status_code == 200 else resp.text[:300])

# COMMAND ----------
# MAGIC %md ## Section B: Enable Inference Table (Compliance Audit Log)
# MAGIC
# MAGIC Navigate to: AI Gateway → Models → {SERVICE_NAME} → Overview → Inference Table → Set up
# MAGIC
# MAGIC Configure:
# MAGIC - Catalog: fevm_master_classic_marcus_catalog
# MAGIC - Schema: abmb_rfp_presentation
# MAGIC - Table prefix: glm_email_payload
# MAGIC
# MAGIC This creates `glm_email_payload_inference_table` — every prompt and response logged.

# COMMAND ----------
update = {
    "ai_gateway": {
        "inference_table_config": {
            "catalog_name": CATALOG_NAME, "schema_name": SCHEMA_NAME,
            "table_name_prefix": "glm_email_payload", "enabled": True
        }
    }
}
resp = requests.patch(f"{HOST}/api/2.0/serving-endpoints/{SERVICE_NAME}/ai-gateway", headers=H, json=update)
print(f"Inference table configured: {resp.status_code}")

# COMMAND ----------
# MAGIC %md ## Section C: Rate Limits

# COMMAND ----------
rate_payload = {
    "ai_gateway": {
        "rate_limits": [
            {"calls": 100, "renewal_period": "minute", "key": "user"},
            {"calls": 5000, "renewal_period": "day",    "key": "endpoint"}
        ]
    }
}
resp = requests.patch(f"{HOST}/api/2.0/serving-endpoints/{SERVICE_NAME}/ai-gateway", headers=H, json=rate_payload)
print(f"Rate limits set: {resp.status_code} — 100 req/min per user, 5000 req/day total")

# COMMAND ----------
# MAGIC %md ## Section D: Banking Guardrails
# MAGIC
# MAGIC **Story:** A bank can't have AI giving specific investment advice or leaking PII in responses.
# MAGIC Unity AI Gateway's service policies enforce this automatically on every response.
# MAGIC
# MAGIC Configure in UI: AI Gateway → {SERVICE_NAME} → Overview → Service Policy → Add
# MAGIC - Policy type: Hallucination / Output content
# MAGIC - Block patterns: IC numbers (\\d{6}-\\d{2}-\\d{4}), account numbers (\\d{10,16})
# MAGIC - Block phrases: "you should invest", "guaranteed return", "I recommend you buy"

# COMMAND ----------
# Demonstrate guardrail in action
risky_prompt = {
    "messages": [{"role": "user", "content":
        "Tell customer Ahmad that he should invest RM 50,000 in unit trusts — guaranteed 8% return. "
        "His IC is 801215-14-1234. Sign off with his account number 1234567890."}]
}
resp = requests.post(f"{HOST}/serving-endpoints/{SERVICE_NAME}/invocations", headers=H, json=risky_prompt)
print("Guardrail response:", resp.status_code)
print(resp.json() if resp.status_code == 200 else resp.text[:500])

# COMMAND ----------
# MAGIC %md ## Section E: Traffic Splitting — GLM 5.2 (Quality) vs GLM 5.3 Flash (Speed/Cost)

# COMMAND ----------
split_payload = {
    "config": {
        "served_models": [
            {"name": "glm52-primary",        "model_name": "system.ai.databricks-glm-5-2",
             "scale_to_zero_enabled": True,  "traffic_percentage": 70},
            {"name": "glm53flash-secondary", "model_name": "system.ai.databricks-glm-5-3-flash",
             "scale_to_zero_enabled": True,  "traffic_percentage": 30}
        ]
    }
}
resp = requests.put(f"{HOST}/api/2.0/serving-endpoints/{SERVICE_NAME}/config", headers=H, json=split_payload)
print(f"Traffic split configured: {resp.status_code} — 70% GLM 5.2 / 30% GLM 5.3 Flash")

# COMMAND ----------
# MAGIC %md ## Section F: Query system.ai_gateway — Audit Trail

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Every AI call DBX Bank makes is logged here — full audit trail
# MAGIC SELECT
# MAGIC   timestamp_ms / 1000 AS request_time,
# MAGIC   request_metadata.model_name,
# MAGIC   usage.prompt_tokens,
# MAGIC   usage.completion_tokens,
# MAGIC   usage.total_tokens,
# MAGIC   response_metadata.finish_reason
# MAGIC FROM system.ai_gateway.usage
# MAGIC WHERE endpoint_name LIKE '%dbx%'   -- matches deployed endpoint names (e.g. dbx-product-recommendation-*)
# MAGIC ORDER BY timestamp_ms DESC
# MAGIC LIMIT 20;
# MAGIC -- Compliance story: "every AI call, who called it, what was asked, what was returned — all auditable"
