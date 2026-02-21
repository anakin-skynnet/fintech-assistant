# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog setup for Getnet Financial Closure
# MAGIC Run once per environment to create schema, volume, audit table, closure table, and audit_errors view.
# MAGIC Requires: **catalog must already exist** (create it in Data Explorer if needed); current user needs USE CATALOG, CREATE SCHEMA, CREATE VOLUME, CREATE TABLE.

# COMMAND ----------

# Widgets for catalog/schema/volume (defaults match bundle variables)
dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("volume_raw", "raw_closure_files", "Raw files volume")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
volume_raw = dbutils.widgets.get("volume_raw")
full_schema = f"{catalog}.{schema_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema and volume

# COMMAND ----------

# Use catalog first, then create schema by name only (avoids "database name is not valid" in some UC runtimes)
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}` COMMENT 'Getnet financial closure - audit, closure data, and volumes'").collect()
spark.sql(f"USE SCHEMA `{schema_name}`")

# COMMAND ----------

# Create managed volume for raw Excel files from SharePoint (Databricks Runtime 13.3+)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_schema}.{volume_raw} COMMENT 'Raw closure files ingested from SharePoint'").collect()
# Volume for global closure output files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_schema}.global_closure_output COMMENT 'Generated global closure files'").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit table (one row per file)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_file_audit (
  file_name STRING NOT NULL,
  file_path_in_volume STRING NOT NULL,
  business_unit STRING,
  validation_status STRING NOT NULL,
  rejection_reason STRING,
  validation_errors_summary STRING,
  rejection_explanation STRING,
  processed_at TIMESTAMP,
  processed_by_job_run_id STRING,
  moved_to_review_at TIMESTAMP,
  attachment_paths STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'One row per ingested file; validation_status valid|rejected; validation_errors_summary is JSON array of {row, field, value, invalid_cause}'
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closure data table (validated rows from Excel)

# COMMAND ----------

# Placeholder schema - align columns with config/closure_schema.yaml and Santander doc
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_data (
  source_file_name STRING NOT NULL,
  closure_period STRING,
  business_unit STRING,
  row_index INT,
  amount DOUBLE,
  currency STRING,
  account_code STRING,
  description STRING,
  value_date DATE,
  ingested_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Validated closure rows from Excel; extend columns per closure_schema.yaml'
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: audit errors flattened for Genie (wrong-field summary)
# MAGIC Parses validation_errors_summary JSON into rows so BUs can query row, field, value, invalid_cause.

# COMMAND ----------

# LATERAL VIEW OUTER INLINE explodes array of structs; empty/null summary yields no rows for that file
spark.sql(f"""
CREATE OR REPLACE VIEW {full_schema}.closure_audit_errors AS
SELECT
  a.file_name,
  a.file_path_in_volume,
  a.business_unit,
  a.validation_status,
  a.processed_at,
  e.row AS error_row,
  e.field AS error_field,
  e.value AS error_value,
  e.invalid_cause AS invalid_cause
FROM {full_schema}.closure_file_audit a
LATERAL VIEW OUTER INLINE(
  CASE
    WHEN a.validation_errors_summary IS NOT NULL AND TRIM(a.validation_errors_summary) != '' AND TRIM(a.validation_errors_summary) != '[]'
    THEN FROM_JSON(a.validation_errors_summary, 'ARRAY<STRUCT<row: INT, field: STRING, value: STRING, invalid_cause: STRING>>')
    ELSE ARRAY()
  END
) AS e
WHERE a.validation_status = 'rejected'
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global closure sent (audit for Job 4)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.global_closure_sent (
  closure_period STRING NOT NULL,
  sent_at TIMESTAMP NOT NULL,
  job_run_id STRING,
  recipient_email STRING,
  file_path STRING
)
USING DELTA
COMMENT 'Log of global closure file sent to Financial Lead per period'
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run execution counter (prefix for job/pipeline runs)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_run_counter (
  job_key STRING NOT NULL,
  run_number BIGINT NOT NULL,
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Execution count per job/pipeline; used as numeric prefix in output file names'
""").collect()
# Seed initial counter for global_closure_send (and optionally closure_pipeline)
for key in ["global_closure_send", "closure_pipeline"]:
    if spark.sql(f"SELECT 1 FROM {full_schema}.closure_run_counter WHERE job_key = '{key}'").count() == 0:
        spark.sql(f"""
          INSERT INTO {full_schema}.closure_run_counter (job_key, run_number, updated_at)
          VALUES ('{key}', 0, current_timestamp())
        """).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closure anomalies (for anomaly detection job)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_anomalies (
  period STRING NOT NULL,
  business_unit STRING,
  metric STRING,
  expected_value DOUBLE,
  actual_value DOUBLE,
  severity STRING,
  detected_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Anomalies detected vs prior period or thresholds'
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SLA and quality metrics (for closure health)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_sla_metrics (
  period STRING NOT NULL,
  business_unit STRING,
  first_file_at TIMESTAMP,
  first_valid_at TIMESTAMP,
  hours_to_valid DOUBLE,
  files_rejected INT,
  files_valid INT
)
USING DELTA
COMMENT 'SLA metrics per period and BU'
""").collect()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_quality_summary (
  period STRING NOT NULL,
  total_files INT,
  pct_valid DOUBLE,
  pct_rejected DOUBLE,
  most_common_error_types STRING,
  updated_at TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Quality summary per period; most_common_error_types is JSON or comma-separated'
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC Schema, volumes, audit, closure, audit_errors view, global_closure_sent, closure_run_counter, closure_anomalies, closure_sla_metrics, closure_quality_summary are ready.
