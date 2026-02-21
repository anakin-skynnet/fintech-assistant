# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Closure Analytics Dashboard
# MAGIC Analytics by **business unit** and **global financial closure** status.

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.dropdown("closure_period", "", ["", "2025-02", "2025-01", "2024-12"], "Closure period (optional)")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
period_filter = dbutils.widgets.get("closure_period")
full_schema = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Closure by business unit

# COMMAND ----------

period_clause = f"AND closure_period = '{period_filter}'" if period_filter else ""
df_bu = spark.sql(f"""
SELECT
  business_unit,
  closure_period,
  COUNT(*) AS row_count,
  SUM(COALESCE(amount, 0)) AS total_amount,
  COUNT(DISTINCT source_file_name) AS file_count
FROM {full_schema}.closure_data
WHERE 1=1 {period_clause}
GROUP BY business_unit, closure_period
ORDER BY closure_period DESC, total_amount DESC
""")
display(df_bu)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. File validation status by business unit (audit)

# COMMAND ----------

audit_period_clause = f"AND date_format(processed_at, 'yyyy-MM') = '{period_filter}'" if period_filter else ""
df_audit = spark.sql(f"""
SELECT
  business_unit,
  validation_status,
  COUNT(*) AS file_count,
  MAX(processed_at) AS last_processed
FROM {full_schema}.closure_file_audit
WHERE 1=1 {audit_period_clause}
GROUP BY business_unit, validation_status
ORDER BY business_unit, validation_status
""")
display(df_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Global financial closure — sent periods

# COMMAND ----------

df_global = spark.sql(f"""
SELECT closure_period, sent_at, recipient_email, job_run_id
FROM {full_schema}.global_closure_sent
ORDER BY sent_at DESC
LIMIT 24
""")
display(df_global)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Amount by business unit (chart)

# COMMAND ----------

# Summary for chart: total amount by BU for latest period
df_chart = spark.sql(f"""
SELECT
  COALESCE(business_unit, 'Unknown') AS business_unit,
  SUM(COALESCE(amount, 0)) AS total_amount
FROM {full_schema}.closure_data
WHERE 1=1 {period_clause}
GROUP BY business_unit
ORDER BY total_amount DESC
""")
display(df_chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rejected files (need review)

# COMMAND ----------

df_rejected = spark.sql(f"""
SELECT file_name, business_unit, rejection_reason, processed_at, moved_to_review_at
FROM {full_schema}.closure_file_audit
WHERE validation_status = 'rejected'
ORDER BY processed_at DESC
LIMIT 50
""")
display(df_rejected)
