# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../shared/config

# COMMAND ----------
# MAGIC %md
# MAGIC # PDF Pipeline — ai_parse_document → ai_classify → ai_extract
# MAGIC
# MAGIC ## Lakeflow Designer Instructions
# MAGIC This notebook contains the SQL for a 3-node Lakeflow Designer pipeline.
# MAGIC To run in Lakeflow Designer UI:
# MAGIC 1. Pipelines → Lakeflow Designer → New Pipeline
# MAGIC 2. Create 3 nodes corresponding to the 3 SQL cells below
# MAGIC 3. Wire Node 1 output → Node 2 input → Node 3 input
# MAGIC
# MAGIC Alternatively, run this notebook top-to-bottom to execute all 3 steps sequentially.

# COMMAND ----------
# MAGIC %md ## Node 1: ai_parse_document — Extract raw text from PDFs

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_parsed AS
# MAGIC SELECT
# MAGIC   path                                                               AS file_path,
# MAGIC   regexp_extract(path, '[^/]+(?=\\.[Pp][Dd][Ff]$)')                AS product_code,
# MAGIC   ai_parse_document(content, 'text')                                 AS parsed_text,
# MAGIC   length(content)                                                    AS file_size_bytes,
# MAGIC   current_timestamp()                                                AS parsed_at
# MAGIC FROM read_files(
# MAGIC   'dbfs:/Volumes/fevm_master_classic_marcus_catalog/abmb_rfp_presentation/product_pdfs',
# MAGIC   format => 'binaryFile',
# MAGIC   pathGlobFilter => '*.pdf'
# MAGIC );

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT product_code, length(parsed_text) AS text_length_chars
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_parsed;

# COMMAND ----------
# MAGIC %md ## Node 2: ai_classify — Classify each brochure into product category

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_classified AS
# MAGIC SELECT
# MAGIC   file_path,
# MAGIC   product_code,
# MAGIC   parsed_text,
# MAGIC   parsed_at,
# MAGIC   ai_classify(
# MAGIC     parsed_text,
# MAGIC     ARRAY('CREDIT_CARD', 'PERSONAL_LOAN', 'HOME_LOAN', 'INVESTMENT', 'INSURANCE')
# MAGIC   ) AS product_category
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_parsed;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT product_code, product_category
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_classified;

# COMMAND ----------
# MAGIC %md ## Node 3: ai_extract — Extract 34 structured fields from each brochure

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_product_catalog AS
# MAGIC SELECT
# MAGIC   file_path, product_code, product_category, parsed_at,
# MAGIC   ai_extract(parsed_text, named_struct(
# MAGIC     'product_name',           'Full product name as printed on the document',
# MAGIC     'min_amount_myr',         'Minimum financing credit limit or investment amount in MYR',
# MAGIC     'max_amount_myr',         'Maximum financing credit limit or investment amount in MYR',
# MAGIC     'min_rate_pct',           'Minimum interest profit management or contribution rate as percentage',
# MAGIC     'max_rate_pct',           'Maximum interest profit management or contribution rate as percentage',
# MAGIC     'min_tenure_months',      'Minimum product tenure or lock-in period in months',
# MAGIC     'max_tenure_months',      'Maximum product tenure or coverage period in months',
# MAGIC     'min_income_annual_myr',  'Minimum annual gross income required for eligibility in MYR',
# MAGIC     'shariah_compliant',      'Whether the product is Shariah-compliant Yes or No',
# MAGIC     'annual_fee_myr',         'Annual fee subscription or contribution amount in MYR',
# MAGIC     'annual_fee_waiver',      'Condition under which the annual fee is waived if applicable',
# MAGIC     'processing_fee',         'One-time processing application or origination fee',
# MAGIC     'early_termination_fee',  'Penalty or fee for early settlement cancellation or redemption',
# MAGIC     'key_benefit_1',          'First key feature or benefit headline from the product brochure',
# MAGIC     'key_benefit_2',          'Second key feature or benefit headline',
# MAGIC     'key_benefit_3',          'Third key feature or benefit headline',
# MAGIC     'fees_summary',           'Brief summary of all recurring and one-time fees and charges',
# MAGIC     'eligibility_age_min',    'Minimum applicant age in years',
# MAGIC     'eligibility_age_max',    'Maximum applicant age in years',
# MAGIC     'eligibility_nationality','Eligible nationality or residency status required',
# MAGIC     'eligibility_employment', 'Eligible employment types or income sources',
# MAGIC     'documents_required',     'List of documents required for application',
# MAGIC     'cashback_rate_pct',      'Cashback or rebate rate percentage for credit cards null if not applicable',
# MAGIC     'reward_points_per_rm',   'Reward points or miles earned per RM1 spent null if not applicable',
# MAGIC     'lounge_access_included', 'Whether complimentary airport lounge access is included Yes No or null',
# MAGIC     'profit_rate_type',       'Whether rate is fixed or variable floating null if not a loan product',
# MAGIC     'collateral_required',    'Whether collateral or security is required Yes No or null',
# MAGIC     'max_dsr_pct',            'Maximum debt service ratio percentage allowed null if not applicable',
# MAGIC     'fund_risk_rating',       'Risk rating of the fund Low Medium High or null if not investment',
# MAGIC     'capital_guaranteed',     'Whether capital is guaranteed Yes No or null if not investment',
# MAGIC     'distribution_frequency', 'How often distributions or dividends are paid null if not applicable',
# MAGIC     'sum_covered_max_myr',    'Maximum sum assured or covered in MYR null if not insurance',
# MAGIC     'effective_date',         'Date this product brochure pricing or terms became effective',
# MAGIC     'product_code_extracted', 'Internal product reference code or identifier'
# MAGIC   )) AS extracted_fields,
# MAGIC   current_timestamp() AS extracted_at
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_pdf_classified;

# COMMAND ----------
# MAGIC %md ## Validation

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT product_code, product_category,
# MAGIC   extracted_fields.product_name AS product_name,
# MAGIC   extracted_fields.min_amount_myr AS min_amount_myr,
# MAGIC   extracted_fields.shariah_compliant AS shariah_compliant,
# MAGIC   extracted_fields.effective_date AS effective_date
# MAGIC FROM fevm_master_classic_marcus_catalog.abmb_rfp_presentation.silver_product_catalog
# MAGIC ORDER BY product_category;
