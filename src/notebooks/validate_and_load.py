# Databricks notebook source
# MAGIC %md
# MAGIC # Job 2: Validate and Load
# MAGIC **File-level audit**: Each Excel file is treated as a single unit. If any row or value is missing or wrong, the **whole file** is flagged invalid, the audit row stores date and reason (errors detected), and the file is sent back to BUs (via Job 3). Only **perfect** files are marked valid and loaded into closure_data.

# COMMAND ----------

import os
import sys
import json
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("volume_raw", "raw_closure_files", "Volume (raw)")
dbutils.widgets.text("config_path", "", "Optional: path to closure_schema.yaml (leave empty to use embedded default)")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_raw = dbutils.widgets.get("volume_raw")
config_path = dbutils.widgets.get("config_path")

volume_base = f"/Volumes/{catalog}/{schema}/{volume_raw}"
full_schema = f"{catalog}.{schema}"
audit_table = f"{full_schema}.closure_file_audit"
closure_table = f"{full_schema}.closure_data"
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
except Exception:
    run_id = f"run-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

# COMMAND ----------

# Load schema config: from path or embedded default
def load_closure_schema():
    if config_path and config_path.strip():
        with open(config_path.replace("/dbfs", "") if config_path.startswith("/dbfs") else config_path, "r") as f:
            import yaml
            return yaml.safe_load(f)
    # Embedded minimal default (align with config/closure_schema.yaml)
    return {
        "columns": [
            {"name": "amount", "type": "double", "validations": ["not_null", "greater_than_zero"]},
            {"name": "currency", "type": "string", "validations": ["not_null"]},
            {"name": "account_code", "type": "string", "validations": ["not_null"]},
            {"name": "description", "type": "string", "validations": []},
            {"name": "value_date", "type": "date", "validations": ["not_null", "date_format"], "date_format": "yyyy-MM-dd"},
            {"name": "business_unit", "type": "string", "validations": ["not_null"]},
        ],
        "sheet_name": None,
        "header_row": 1,
    }

schema_config = load_closure_schema()
columns_config = schema_config.get("columns", [])
max_errors_per_file = 100

# COMMAND ----------

# List files already in audit (processed) and which are rejected (eligible for re-ingestion)
audit_df = spark.table(audit_table)
processed_paths = {row.file_path_in_volume for row in audit_df.select("file_path_in_volume").collect()}
rejected_paths = {row.file_path_in_volume for row in audit_df.filter("validation_status = 'rejected'").select("file_path_in_volume").collect()}

# COMMAND ----------

# List all Excel files in volume (recursive by date folders)
from pyspark.sql import Row
def list_volume_files(base):
    out = []
    try:
        for f in dbutils.fs.ls(base):
            if f.isDir():
                out.extend(list_volume_files(f.path))
            elif f.name.lower().endswith((".xlsx", ".xls")):
                out.append(f.path)
    except Exception:
        pass
    return out

all_excel = list_volume_files(volume_base)
# Process: (1) new files not in audit, (2) rejected files (re-ingestion: re-validate and update audit / load if valid)
to_process = [p for p in all_excel if p not in processed_paths or p in rejected_paths]
reprocess_rejected = [p for p in to_process if p in rejected_paths]
new_files = [p for p in to_process if p not in processed_paths]

# COMMAND ----------

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
import pandas as pd
from validator import validate_dataframe

# COMMAND ----------

now = datetime.utcnow()
audit_rows = []
closure_dfs = []

import tempfile
import shutil
for file_path in to_process:
    file_name = file_path.split("/")[-1]
    try:
        # Copy to local temp file for pandas (Volume may not be directly readable by pandas)
        with tempfile.NamedTemporaryFile(suffix=os.path.splitext(file_name)[1], delete=False) as tmp:
            tmp_path = tmp.name
        dbutils.fs.cp(file_path, f"file:{tmp_path}")
        pdf = pd.read_excel(tmp_path, sheet_name=0, header=0)
        os.unlink(tmp_path)
    except Exception as e:
        audit_rows.append({
            "file_name": file_name,
            "file_path_in_volume": file_path,
            "business_unit": None,
            "validation_status": "rejected",
            "rejection_reason": str(e),
            "validation_errors_summary": json.dumps([{"row": 0, "field": "_", "value": "", "invalid_cause": "read_error"}]),
            "rejection_explanation": None,
            "processed_at": now,
            "processed_by_job_run_id": run_id,
            "moved_to_review_at": None,
            "attachment_paths": None,
            "created_at": now,
            "updated_at": now,
        })
        continue
    errors = validate_dataframe(pdf, columns_config, max_errors_per_file)
    if errors:
        # Human-readable summary for BUs (first 5 errors + total)
        err_lines = []
        for e in errors[:5]:
            err_lines.append(f"Row {e.get('row', '?')}: {e.get('field', '?')} — {e.get('invalid_cause', 'invalid')} (value: {str(e.get('value', ''))[:50]})")
        rejection_explanation = ". ".join(err_lines) + (f" ({len(errors)} error(s) total)." if len(errors) > 5 else ".")
        audit_rows.append({
            "file_name": file_name,
            "file_path_in_volume": file_path,
            "business_unit": pdf["business_unit"].iloc[0] if "business_unit" in pdf.columns and len(pdf) else None,
            "validation_status": "rejected",
            "rejection_reason": f"{len(errors)} validation error(s) — whole file invalid",
            "validation_errors_summary": json.dumps(errors),
            "rejection_explanation": rejection_explanation,
            "processed_at": now,
            "processed_by_job_run_id": run_id,
            "moved_to_review_at": None,
            "attachment_paths": None,
            "created_at": now,
            "updated_at": now,
        })
    else:
        audit_rows.append({
            "file_name": file_name,
            "file_path_in_volume": file_path,
            "business_unit": pdf["business_unit"].iloc[0] if "business_unit" in pdf.columns and len(pdf) else None,
            "validation_status": "valid",
            "rejection_reason": None,
            "validation_errors_summary": None,
            "rejection_explanation": None,
            "processed_at": now,
            "processed_by_job_run_id": run_id,
            "moved_to_review_at": None,
            "attachment_paths": None,
            "created_at": now,
            "updated_at": now,
        })
        pdf["source_file_name"] = file_name
        pdf["ingested_at"] = now
        if "value_date" in pdf.columns:
            pdf["value_date"] = pd.to_datetime(pdf["value_date"], errors="coerce")
        closure_dfs.append(pdf)

# COMMAND ----------

# Write audit: append new rows; merge (update) re-ingested rejected rows
if audit_rows:
    new_audit_rows = [r for r in audit_rows if r["file_path_in_volume"] not in rejected_paths]
    updated_audit_rows = [r for r in audit_rows if r["file_path_in_volume"] in rejected_paths]
    if new_audit_rows:
        audit_spark = spark.createDataFrame(new_audit_rows)
        audit_spark.write.format("delta").mode("append").saveAsTable(audit_table)
    if updated_audit_rows:
        from pyspark.sql import Row as SparkRow
        updates_df = spark.createDataFrame(updated_audit_rows)
        updates_df.createOrReplaceTempView("audit_updates")
        spark.sql(f"""
            MERGE INTO {audit_table} AS t
            USING audit_updates AS u ON t.file_path_in_volume = u.file_path_in_volume
            WHEN MATCHED THEN UPDATE SET
              t.validation_status = u.validation_status,
              t.rejection_reason = u.rejection_reason,
              t.validation_errors_summary = u.validation_errors_summary,
              t.rejection_explanation = u.rejection_explanation,
              t.processed_at = u.processed_at,
              t.processed_by_job_run_id = u.processed_by_job_run_id,
              t.updated_at = u.updated_at
        """)

# COMMAND ----------

# Write valid data to closure_data (includes newly valid re-ingested files)
for pdf in closure_dfs:
    # Align columns with closure_data table
    cols = ["source_file_name", "closure_period", "business_unit", "row_index", "amount", "currency", "account_code", "description", "value_date", "ingested_at"]
    for c in cols:
        if c not in pdf.columns:
            pdf[c] = None
    pdf["row_index"] = range(1, len(pdf) + 1)
    pdf["closure_period"] = now.strftime("%Y-%m")
    subset = [c for c in cols if c in pdf.columns]
    spark.createDataFrame(pdf[subset]).write.format("delta").mode("append").saveAsTable(closure_table)

# COMMAND ----------

print(f"Processed {len(to_process)} file(s). Audit rows: {len(audit_rows)}. Valid files loaded: {len(closure_dfs)}")
