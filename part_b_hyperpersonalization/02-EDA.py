# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./00-Setup

# COMMAND ----------
# MAGIC %md
# MAGIC # Exploratory Data Analysis — DBX Bank Customer 360
# MAGIC Understanding the 1,000 customer profiles from Part A before building the recommendation model.

# COMMAND ----------
# MAGIC %md ## Load gold_customer_360

# COMMAND ----------
df = spark.table(f"{da.full_schema}.gold_customer_360")
print(f"Rows: {df.count():,} | Columns: {len(df.columns)}")
df.printSchema()

# COMMAND ----------
# MAGIC %md ## Customer Segment Distribution
# MAGIC *DBX Bank targets: Mass Market, Emerging Affluent, Affluent, Priority, Private Banking*

# COMMAND ----------
display(
    df.groupBy("customer_segment")
      .count()
      .orderBy("count", ascending=False)
)

# COMMAND ----------
# MAGIC %md ## Lifestyle Segment Distribution

# COMMAND ----------
display(
    df.groupBy("lifestyle_segment")
      .count()
      .orderBy("count", ascending=False)
)

# COMMAND ----------
# MAGIC %md ## Annual Income Distribution by Segment

# COMMAND ----------
display(
    df.select("customer_segment", "annual_income_amount")
      .filter("annual_income_amount IS NOT NULL AND annual_income_amount > 0")
)

# COMMAND ----------
# MAGIC %md ## CTOS Score Distribution by Customer Segment
# MAGIC *CTOS > 700 = credit-eligible. Basis for CREDIT_CARD recommendation rule.*

# COMMAND ----------
display(
    df.select("customer_segment", "ctos_score")
      .filter("ctos_score IS NOT NULL AND ctos_score > 0")
)

# COMMAND ----------
# MAGIC %md ## Product Ownership Matrix
# MAGIC *Which products do customers already have? Drives cross-sell gap analysis.*

# COMMAND ----------
from pyspark.sql import functions as F

product_summary = df.agg(
    F.round(F.avg("has_credit_card") * 100, 1).alias("pct_has_credit_card"),
    F.round(F.avg("has_home_loan")   * 100, 1).alias("pct_has_home_loan"),
    F.round(F.avg("has_personal_loan") * 100, 1).alias("pct_has_personal_loan"),
    F.round(F.sum("has_credit_card").cast("long"), 0).alias("n_credit_card"),
    F.round(F.sum("has_home_loan").cast("long"), 0).alias("n_home_loan"),
    F.round(F.sum("has_personal_loan").cast("long"), 0).alias("n_personal_loan"),
    F.count("*").alias("total_customers")
)
display(product_summary)

# COMMAND ----------
# MAGIC %md ## Digital Maturity by Segment
# MAGIC *High digital maturity = preferred channel for personalized offers*

# COMMAND ----------
display(
    df.groupBy("customer_segment")
      .agg(
          F.round(F.avg("digital_maturity_score"), 2).alias("avg_digital_maturity"),
          F.round(F.avg("nps_score"), 2).alias("avg_nps"),
          F.count("*").alias("n")
      )
      .orderBy("avg_digital_maturity", ascending=False)
)

# COMMAND ----------
# MAGIC %md ## Shariah-Preferred Customers
# MAGIC *DBX Bank offers Islamic banking products — key segmentation dimension*

# COMMAND ----------
display(
    df.groupBy("is_shariah_preferred")
      .count()
      .withColumnRenamed("is_shariah_preferred", "shariah_preferred")
)

# COMMAND ----------
# MAGIC %md ## Geographic Distribution (Primary State)

# COMMAND ----------
display(
    df.groupBy("primary_state")
      .count()
      .orderBy("count", ascending=False)
)

# COMMAND ----------
# MAGIC %md ## Net Worth Band Distribution

# COMMAND ----------
display(
    df.groupBy("net_worth_band")
      .count()
      .orderBy("count", ascending=False)
)

# COMMAND ----------
# MAGIC %md ## Key Statistics

# COMMAND ----------
stats = df.select(
    "annual_income_amount", "total_deposit_balance_myr", "ctos_score",
    "num_accounts", "monthly_loan_commitment_myr", "age",
    "txn_count_30d", "avg_txn_amount_myr", "digital_maturity_score"
).describe()
display(stats)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Genie Code Prompt Suggestions
# MAGIC
# MAGIC Use these prompts in Genie Code (AI Assistant → Genie) to deepen the exploration:
# MAGIC
# MAGIC ```
# MAGIC 1. "Show me the correlation matrix between annual_income_amount, ctos_score,
# MAGIC     total_deposit_balance_myr, and monthly_loan_commitment_myr"
# MAGIC
# MAGIC 2. "For customers without a credit card (has_credit_card = 0), what percentage
# MAGIC     have a ctos_score above 700 and income above 60,000?"
# MAGIC
# MAGIC 3. "Which customer_segment has the highest proportion of shariah-preferred customers?"
# MAGIC
# MAGIC 4. "Plot the age distribution for customers who have a home loan vs those who don't"
# MAGIC
# MAGIC 5. "What is the average telco ARPU by lifestyle segment?"
# MAGIC ```

# COMMAND ----------
print("EDA complete.")
print(f"  Total customers analysed: {df.count():,}")
print(f"  Feature columns available: {len(df.columns)}")
print(f"  Next step: 03-Feature-Engineering.py")
