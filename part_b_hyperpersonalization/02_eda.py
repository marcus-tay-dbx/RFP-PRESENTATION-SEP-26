# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install seaborn matplotlib pandas

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import *

df = spark.table(f"{FULL_SCHEMA}.gold_customer_360").toPandas()

# COMMAND ----------
# MAGIC %md ## Feature Distributions

# COMMAND ----------
fig, axes = plt.subplots(2, 3, figsize=(15, 8))
df['ctos_score'].hist(bins=30, ax=axes[0,0], color='#1B3A6B', edgecolor='white')
axes[0,0].set_title('CTOS Score Distribution')
df['annual_income_amount'].apply(lambda x: x/1000).hist(bins=30, ax=axes[0,1], color='#C8102E', edgecolor='white')
axes[0,1].set_title('Annual Income (RM thousands)')
df['age'].hist(bins=20, ax=axes[0,2], color='#1B3A6B', edgecolor='white')
axes[0,2].set_title('Age Distribution')
df['total_deposit_balance_myr'].apply(lambda x: x/1000).hist(bins=30, ax=axes[1,0], color='#C8102E', edgecolor='white')
axes[1,0].set_title('Total Balance (RM thousands)')
df['digital_activity_score'].hist(bins=10, ax=axes[1,1], color='#1B3A6B', edgecolor='white')
axes[1,1].set_title('Digital Activity Score')
df['txn_count_30d'].hist(bins=20, ax=axes[1,2], color='#C8102E', edgecolor='white')
axes[1,2].set_title('Transactions Last 30 Days')
plt.suptitle('DBX Bank Customer Feature Distributions', fontsize=14, fontweight='bold')
plt.tight_layout()
display(fig)

# COMMAND ----------
# MAGIC %md ## Correlation Matrix (features for recommendation model)

# COMMAND ----------
feature_cols = ['age','annual_income_amount','total_deposit_balance_myr','ctos_score',
                'txn_count_30d','digital_activity_score','telco_arpu_myr',
                'num_accounts','num_loan_facilities','monthly_loan_commitment_myr']
corr_df = df[feature_cols].corr()
fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr_df, annot=True, fmt='.2f', cmap='Blues', ax=ax,
            linewidths=0.5, square=True)
ax.set_title('Feature Correlation Matrix')
display(fig)

# COMMAND ----------
# MAGIC %md ## Label Distribution (next_best_product — rule-based proxy labels)

# COMMAND ----------
# Quick label preview using same rules as feature engineering
from pyspark.sql.functions import when, col

gold = spark.table(f"{FULL_SCHEMA}.gold_customer_360")
label_preview = (gold
    .withColumn("label",
        when((col("owns_credit_card") == 0) & (col("ctos_score") > 700) & (col("annual_income_amount") > 60000), "CREDIT_CARD")
        .when((col("owns_home_loan") == 0) & (col("age").between(28, 45)) & (col("marital_status") == "married") & (col("annual_income_amount") > 48000), "HOME_LOAN")
        .when((col("owns_personal_loan") == 0) & ((col("monthly_loan_commitment_myr") * 12 / col("annual_income_amount")) > 0.2), "PERSONAL_LOAN")
        .when(col("net_worth_band").isin("500k_to_1m","1m_to_5m","5m_to_10m","over_10m") & (col("has_fixed_deposit") == False), "INVESTMENT")
        .when((col("has_hire_purchase") == True) & (col("owns_credit_card") == 0), "INSURANCE")
        .otherwise("NO_ACTION"))
    .groupBy("label").count().orderBy("count", ascending=False))
display(label_preview)
