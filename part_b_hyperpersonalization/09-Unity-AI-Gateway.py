# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Unity AI Gateway — Governing GLM 5.2 for DBX Bank
# MAGIC
# MAGIC ## Business Story
# MAGIC DBX Bank doesn't just deploy AI — they govern it. Every AI call that drafts
# MAGIC a customer email flows through Unity AI Gateway: rate-limited, guardrailed, and
# MAGIC fully auditable. The same governance model that controls data access now controls
# MAGIC AI access.
# MAGIC
# MAGIC ## What we configure:
# MAGIC | Section | What | Why |
# MAGIC |---|---|---|
# MAGIC | A | GLM 5.2 model service | Foundation for email drafting |
# MAGIC | B | Inference table (audit log) | Every call logged for compliance |
# MAGIC | C | Rate limits | Control token spend — 100 req/min per user |
# MAGIC | D | Banking guardrails | No PII in responses, no specific financial advice |
# MAGIC | E | Traffic split | GLM 5.2 (quality, 70%) vs GLM 5.3 Flash (speed, 30%) |
# MAGIC | F | Audit trail query | system.ai_gateway.usage |

# COMMAND ----------
import requests, json, time

ctx      = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url  = ctx.apiUrl().get()
HOST     = api_url if api_url.startswith("https://") else f"https://{api_url}"
TOKEN    = ctx.apiToken().get()
H        = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

print(f"Host:              {HOST}")
print(f"Gateway service:   {da.gateway_service}")
print(f"Catalog:           {da.catalog}")
print(f"Schema:            {da.schema}")

# COMMAND ----------
# MAGIC %md ## Section A: Create GLM 5.2 Model Service

# COMMAND ----------
service_payload = {
    "name": da.gateway_service,
    "config": {
        "served_models": [{
            "name": "glm52-primary",
            "model_name": "system.ai.databricks-glm-5-2",
            "scale_to_zero_enabled": True
        }]
    }
}

resp = requests.post(f"{HOST}/api/2.0/serving-endpoints", headers=H, json=service_payload)
if resp.status_code in [200, 201]:
    print(f"Model service created: {da.gateway_service}")
elif resp.status_code == 400 and "already exists" in resp.text.lower():
    print(f"Model service already exists: {da.gateway_service}")
else:
    print(f"Create service: {resp.status_code} — {resp.text[:400]}")

# COMMAND ----------
# MAGIC %md ### Quick Test — Email Draft for DBX Bank Customer

# COMMAND ----------
# Wait a moment for the service to initialise
time.sleep(5)

test_prompt = {
    "messages": [{
        "role": "user",
        "content": (
            "You are an DBX Bank Relationship Manager. "
            "Draft a 100-word email to Ahmad bin Ibrahim (Affluent segment, 7 years tenure) "
            "recommending he considers our WealthSmart Income Fund given his recent deposit activity. "
            "Professional tone, Bahasa Malaysia welcome."
        )
    }]
}

resp = requests.post(
    f"{HOST}/serving-endpoints/{da.gateway_service}/invocations",
    headers=H, json=test_prompt, timeout=30
)

if resp.status_code == 200:
    print("Email draft:")
    print(resp.json()["choices"][0]["message"]["content"])
else:
    print(f"Service not ready yet ({resp.status_code}) — continue to configure, test later")
    print(resp.text[:300])

# COMMAND ----------
# MAGIC %md ## Section B: Enable Inference Table (Compliance Audit Log)
# MAGIC
# MAGIC **Story:** Bank Negara Malaysia requires financial institutions to maintain
# MAGIC logs of AI-generated customer communications. This table captures every
# MAGIC prompt sent and response received — full audit trail, queryable via SQL.

# COMMAND ----------
inference_update = {
    "ai_gateway": {
        "inference_table_config": {
            "catalog_name":      da.catalog,
            "schema_name":       da.schema,
            "table_name_prefix": "glm_email_payload",
            "enabled":           True
        }
    }
}

resp = requests.patch(
    f"{HOST}/api/2.0/serving-endpoints/{da.gateway_service}/ai-gateway",
    headers=H, json=inference_update
)
print(f"Inference table configured: {resp.status_code}")
if resp.status_code == 200:
    print(f"  Audit log: {da.catalog}.{da.schema}.glm_email_payload_inference_table")
else:
    print(f"  {resp.text[:200]}")

# COMMAND ----------
# MAGIC %md ## Section C: Rate Limits
# MAGIC
# MAGIC DBX Bank sets two tiers:
# MAGIC - **Per-user**: 100 requests/minute — prevents runaway RM email generation
# MAGIC - **Per-endpoint (daily)**: 5,000 requests — keeps monthly AI spend predictable

# COMMAND ----------
rate_payload = {
    "ai_gateway": {
        "rate_limits": [
            {"calls": 100,  "renewal_period": "minute", "key": "user"},
            {"calls": 5000, "renewal_period": "day",    "key": "endpoint"}
        ]
    }
}

resp = requests.patch(
    f"{HOST}/api/2.0/serving-endpoints/{da.gateway_service}/ai-gateway",
    headers=H, json=rate_payload
)
print(f"Rate limits configured: {resp.status_code}")
print(f"  100 req/min per user | 5,000 req/day per endpoint")

# COMMAND ----------
# MAGIC %md ## Section D: Banking Guardrails
# MAGIC
# MAGIC **Story:** Malaysian financial regulations (SC Act 2007, PDPA 2010) prohibit
# MAGIC AI systems from providing specific investment advice or leaking customer PII.
# MAGIC Unity AI Gateway enforces these policies on every response automatically.
# MAGIC
# MAGIC Guardrails configured:
# MAGIC - Block IC number patterns: `\d{6}-\d{2}-\d{4}`
# MAGIC - Block account numbers: `\d{10,16}`
# MAGIC - Block phrases: "you should invest", "guaranteed return", "I recommend you buy"

# COMMAND ----------
# Demonstrate guardrail intent (service policy set via UI or Terraform in production)
guardrail_demo = {
    "messages": [{
        "role": "user",
        "content": (
            "Tell customer Ahmad that he should invest RM 50,000 in unit trusts — "
            "guaranteed 8% return. His IC is 801215-14-1234 and account 1234567890123."
        )
    }]
}

resp = requests.post(
    f"{HOST}/serving-endpoints/{da.gateway_service}/invocations",
    headers=H, json=guardrail_demo, timeout=30
)

print(f"Guardrail demo response: {resp.status_code}")
if resp.status_code == 200:
    response_text = resp.json()["choices"][0]["message"]["content"]
    print(response_text)
    # In production, the guardrail policy would block/redact PII and specific advice
    print("\nNOTE: In production, Service Policy blocks PII and guaranteed-return phrases.")
    print("Configure via: AI Gateway UI → {da.gateway_service} → Service Policy → Add policy")
else:
    print(resp.text[:300])

# COMMAND ----------
# MAGIC %md ## Section E: Traffic Splitting — GLM 5.2 (Quality) vs GLM 5.3 Flash (Speed)
# MAGIC
# MAGIC **Story:** DBX Bank wants to A/B test model quality vs cost:
# MAGIC - **70% → GLM 5.2**: full-quality email drafts for Affluent & Priority customers
# MAGIC - **30% → GLM 5.3 Flash**: faster, cheaper model for Mass Market notifications

# COMMAND ----------
split_payload = {
    "config": {
        "served_models": [
            {
                "name":                    "glm52-primary",
                "model_name":              "system.ai.databricks-glm-5-2",
                "scale_to_zero_enabled":   True,
                "traffic_percentage":      70
            },
            {
                "name":                    "glm53flash-secondary",
                "model_name":              "system.ai.databricks-glm-5-3-flash",
                "scale_to_zero_enabled":   True,
                "traffic_percentage":      30
            }
        ]
    }
}

resp = requests.put(
    f"{HOST}/api/2.0/serving-endpoints/{da.gateway_service}/config",
    headers=H, json=split_payload
)
print(f"Traffic split configured: {resp.status_code}")
print(f"  70% → GLM 5.2  (quality, Affluent/Priority)")
print(f"  30% → GLM 5.3 Flash  (speed, Mass Market)")

# COMMAND ----------
# MAGIC %md ## Section F: Query system.ai_gateway — Full Audit Trail
# MAGIC
# MAGIC **Story:** Compliance team can query every AI call made by DBX Bank RMs —
# MAGIC who called it, when, what was asked, what model responded, how many tokens.
# MAGIC Zero friction: same SQL interface as all other Databricks data.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Every AI call DBX Bank makes is logged here — full audit trail
# MAGIC SELECT
# MAGIC   timestamp_ms / 1000                     AS request_time_epoch,
# MAGIC   from_unixtime(timestamp_ms / 1000)      AS request_time,
# MAGIC   request_metadata.model_name,
# MAGIC   usage.prompt_tokens,
# MAGIC   usage.completion_tokens,
# MAGIC   usage.total_tokens,
# MAGIC   response_metadata.finish_reason,
# MAGIC   request_metadata.user
# MAGIC FROM system.ai_gateway.usage
# MAGIC WHERE endpoint_name LIKE 'dbx-glm-gateway%'
# MAGIC ORDER BY timestamp_ms DESC
# MAGIC LIMIT 20;
# MAGIC -- Compliance story: "every AI call, who called it, what model, how many tokens — auditable in SQL"

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------
print("Unity AI Gateway configuration complete.")
print(f"  Service:        {da.gateway_service}")
print(f"  Audit log:      {da.catalog}.{da.schema}.glm_email_payload_inference_table")
print(f"  Rate limit:     100 req/min per user | 5,000 req/day")
print(f"  Traffic split:  70% GLM 5.2 / 30% GLM 5.3 Flash")
print(f"  Guardrails:     PII blocking, no financial advice (configure via UI)")
print(f"  Audit query:    system.ai_gateway.usage")
print(f"  Next step:      99-Load-Test-Endpoint.py")
