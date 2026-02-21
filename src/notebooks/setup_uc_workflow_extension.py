# Databricks notebook source
# MAGIC %md
# MAGIC # Workflow extension: reviewer approval and global team
# MAGIC Run **once** after setup_uc to add columns and table for the reviewer/orchestrator/global-team workflow.
# MAGIC See docs/WORKFLOW_ENRICHMENT_AND_AUTOMATION.md.

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
full_schema = f"{catalog}.{schema}"


def add_col_safe(table: str, col: str, dtype: str) -> None:
    try:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
        print(f"Added {col}")
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"{col} already exists, skip.")
        else:
            raise

# COMMAND ----------

# closure_file_audit: reviewer and approval state
add_col_safe(f"{full_schema}.closure_file_audit", "reviewed_at", "TIMESTAMP")
add_col_safe(f"{full_schema}.closure_file_audit", "reviewed_by", "STRING")
add_col_safe(f"{full_schema}.closure_file_audit", "approval_status", "STRING")
add_col_safe(f"{full_schema}.closure_file_audit", "approved_at", "TIMESTAMP")
add_col_safe(f"{full_schema}.closure_file_audit", "approved_by", "STRING")

# COMMAND ----------

# global_closure_sent: orchestrator and global team notification
add_col_safe(f"{full_schema}.global_closure_sent", "created_by", "STRING")
add_col_safe(f"{full_schema}.global_closure_sent", "approved_by", "STRING")
add_col_safe(f"{full_schema}.global_closure_sent", "global_team_notified_at", "TIMESTAMP")

# COMMAND ----------

# New table: one row per recipient per global send (financial_lead + global_team)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.global_closure_recipients (
  closure_period STRING NOT NULL,
  sent_at TIMESTAMP NOT NULL,
  recipient_email STRING NOT NULL,
  recipient_role STRING,
  job_run_id STRING
)
USING DELTA
COMMENT 'Log of who received the global closure (Financial Lead + global team)'
""").collect()
print("global_closure_recipients table ready.")

# COMMAND ----------

# Run execution counter (prefix for job/pipeline output file names)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full_schema}.closure_run_counter (
  job_key STRING NOT NULL,
  run_number BIGINT NOT NULL,
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Execution count per job/pipeline; prefix in output file names'
""").collect()
for key in ["global_closure_send", "closure_pipeline"]:
    if spark.sql(f"SELECT 1 FROM {full_schema}.closure_run_counter WHERE job_key = '{key}'").count() == 0:
        spark.sql(f"""
          INSERT INTO {full_schema}.closure_run_counter (job_key, run_number, updated_at)
          VALUES ('{key}', 0, current_timestamp())
        """).collect()
print("closure_run_counter ready.")
