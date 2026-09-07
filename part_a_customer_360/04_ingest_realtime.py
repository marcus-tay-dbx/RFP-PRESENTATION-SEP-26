# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # Real-Time Event Streaming — Bronze Layer
# MAGIC
# MAGIC **Real-Time Event Streaming:** Uses Spark's built-in Rate Source — no external Kafka needed
# MAGIC for the demo. A UDF generates realistic digital banking events (mobile app, web, ATM) and
# MAGIC writes them continuously to `bronze_digital_events` as a Delta streaming table.
# MAGIC
# MAGIC This simulates the customer digital channel feed that would come from an event broker
# MAGIC (Kafka / Confluent / Event Hub) in production.
# MAGIC
# MAGIC Key design choices:
# MAGIC - `format("rate")` — emits monotonically increasing row IDs at a fixed throughput; zero infra deps
# MAGIC - UDF enriches each row with 13 realistic fields derived from broadcast party_ids
# MAGIC - `from_json` parses the UDF output into typed struct columns before writing to Delta
# MAGIC - Continuous write (no `availableNow`) — stream stays alive for live demo effect

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, col, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField, LongType, BooleanType
import uuid
import random

# COMMAND ----------
# MAGIC %md ### Load party IDs and broadcast

# COMMAND ----------
party_ids = [
    row.cif_number
    for row in spark.table(f"{FULL_SCHEMA}.bronze_customer_master")
        .select("cif_number")
        .limit(1000)
        .collect()
]
party_ids_bc = spark.sparkContext.broadcast(party_ids)
print(f"✅ Broadcast {len(party_ids)} party IDs for event generation")

# COMMAND ----------
# MAGIC %md ### Define event generator UDF

# COMMAND ----------
EVENT_TYPES = [
    "page_view", "product_view", "apply_start", "apply_submit",
    "login", "logout", "transfer_init", "transfer_confirm",
    "balance_check", "statement_download", "support_chat_open",
]
PRODUCT_CATEGORIES = [
    "savings_account", "current_account", "fixed_deposit",
    "personal_loan", "home_loan", "auto_loan",
    "credit_card", "debit_card", "unit_trust", "insurance",
]
PAGE_NAMES = [
    "home", "dashboard", "accounts", "loans", "cards",
    "investments", "promotions", "calculator", "apply_now", "profile",
]
DEVICE_TYPES = ["mobile_ios", "mobile_android", "web_desktop", "web_mobile", "tablet"]
CHANNELS    = ["mobile_app", "internet_banking", "atm", "branch_kiosk"]
IP_STATES   = [
    "Selangor", "Kuala Lumpur", "Johor", "Penang", "Perak",
    "Sabah", "Sarawak", "Kedah", "Pahang", "Negeri Sembilan",
]

@udf(returnType=StringType())
def random_event_json(row_id):
    import json, uuid, random
    ids = party_ids_bc.value
    party_id = ids[int(row_id) % len(ids)] if ids else f"CIF{int(row_id):07d}"
    return json.dumps({
        "event_id":                str(uuid.uuid4()),
        "party_id":                party_id,
        "session_id":              str(uuid.uuid4())[:8],
        "event_type":              random.choice(EVENT_TYPES),
        "product_category_viewed": random.choice(PRODUCT_CATEGORIES),
        "page_name":               random.choice(PAGE_NAMES),
        "duration_seconds":        random.randint(2, 420),
        "device_type":             random.choice(DEVICE_TYPES),
        "channel":                 random.choice(CHANNELS),
        "session_count_today":     random.randint(1, 12),
        "ip_state":                random.choice(IP_STATES),
        "is_authenticated":        random.choice([True, False]),
        "row_seq":                 int(row_id),
    })

# COMMAND ----------
# MAGIC %md ### Define event schema for from_json parsing

# COMMAND ----------
event_schema = StructType([
    StructField("event_id",                StringType(),  True),
    StructField("party_id",                StringType(),  True),
    StructField("session_id",              StringType(),  True),
    StructField("event_type",              StringType(),  True),
    StructField("product_category_viewed", StringType(),  True),
    StructField("page_name",               StringType(),  True),
    StructField("duration_seconds",        LongType(),    True),
    StructField("device_type",             StringType(),  True),
    StructField("channel",                 StringType(),  True),
    StructField("session_count_today",     LongType(),    True),
    StructField("ip_state",                StringType(),  True),
    StructField("is_authenticated",        BooleanType(), True),
    StructField("row_seq",                 LongType(),    True),
])

# COMMAND ----------
# MAGIC %md ### Start real-time stream

# COMMAND ----------
raw_stream = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 2)
        .load()
)

parsed_stream = (
    raw_stream
        .withColumn("_json", random_event_json(col("value")))
        .withColumn("event", from_json(col("_json"), event_schema))
        .select("event.*")
        .withColumn("event_timestamp", current_timestamp())
)

stream_query = (
    parsed_stream.writeStream
        .format("delta")
        .option("checkpointLocation", f"{VOLUME_DATA}/_schemas/digital_events/checkpoint")
        .option("mergeSchema", "true")
        .toTable(f"{FULL_SCHEMA}.bronze_digital_events")
)

print("✅ bronze_digital_events stream started — emitting 2 events/second")

# COMMAND ----------
# MAGIC %md ### Live row count (re-run to refresh)

# COMMAND ----------
display(spark.sql(f"SELECT COUNT(*) as total_events FROM {FULL_SCHEMA}.bronze_digital_events"))

# COMMAND ----------
# MAGIC %md ### Stop stream (run when demo is complete)

# COMMAND ----------
stream_query.stop()
print("✅ Real-time stream stopped")
