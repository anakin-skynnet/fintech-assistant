# Databricks notebook source
# MAGIC %md
# MAGIC # Optional: Agent — Rejection explanation
# MAGIC Reads validation_errors_summary for rejected files, calls LLM to generate plain-language explanation.
# MAGIC Run as a task after Job 2 (validate_and_load); updates audit.rejection_explanation.

# COMMAND ----------

import json
import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")

# COMMAND ----------

import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
from notebook_utils import safe_catalog, safe_schema, log

catalog = safe_catalog(dbutils.widgets.get("catalog"))
schema = safe_schema(dbutils.widgets.get("schema"))
full_schema = f"{catalog}.{schema}"
audit_table = f"{full_schema}.closure_file_audit"

# COMMAND ----------

# Load prompts (optional: from workspace file or use defaults)
system_msg = "You are a helpful assistant that explains data validation errors in plain language for business users."
user_tpl = """The following validation errors were found in a financial closure file.
For each error, the row number, field name, invalid value, and rule are given.
Produce a short, bullet-point summary that a business user can use to fix the file.
Do not include technical jargon.

Errors (JSON):
{validation_errors_summary}"""

# COMMAND ----------

# Fetch rejected rows where rejection_explanation is null
rejected = spark.sql(f"""
  SELECT file_path_in_volume, file_name, validation_errors_summary
  FROM {audit_table}
  WHERE validation_status = 'rejected' AND (rejection_explanation IS NULL OR TRIM(rejection_explanation) = '')
""").collect()

if not rejected:
    log("AGENT", "No rejected files needing explanation.")
    dbutils.notebook.exit("0")

# COMMAND ----------

def generate_explanation(errors_json: str) -> str:
    """Call LLM or fallback to a simple bullet summary from JSON."""
    try:
        from databricks.ai import ai
        reply = ai.llm.chat(
            system=system_msg,
            messages=[{"role": "user", "content": user_tpl.format(validation_errors_summary=errors_json)}],
        )
        return (reply.choices[0].message.content or "").strip() if reply.choices else ""
    except Exception as e:
        log("AGENT", "LLM call failed, using fallback:", e)
    # Fallback: parse JSON and format bullets
    try:
        errs = json.loads(errors_json) if errors_json else []
        lines = []
        for e in errs[:20]:
            row = e.get("row", "")
            field = e.get("field", "")
            value = e.get("value", "")
            cause = e.get("invalid_cause", "")
            lines.append(f"• Row {row}, field '{field}': value '{value}' — {cause}")
        return "\n".join(lines) if lines else "Validation errors occurred; check the error list."
    except Exception:
        return "Validation errors occurred; see validation_errors_summary for details."

# COMMAND ----------

updates = []
for row in rejected:
    path = row.file_path_in_volume
    summary = row.validation_errors_summary or "[]"
    explanation = generate_explanation(summary)
    updates.append({
        "file_path_in_volume": path,
        "rejection_explanation": explanation,
        "updated_at": datetime.utcnow(),
    })

if not updates:
    dbutils.notebook.exit("0")

# COMMAND ----------

# Update audit table with rejection_explanation (merge by file_path_in_volume)
updates_df = spark.createDataFrame(updates)
updates_df.createOrReplaceTempView("explanation_updates")
spark.sql(f"""
  MERGE INTO {audit_table} AS t
  USING (
    SELECT file_path_in_volume, rejection_explanation, updated_at
    FROM explanation_updates
  ) AS u ON t.file_path_in_volume = u.file_path_in_volume
  WHEN MATCHED THEN UPDATE SET
    t.rejection_explanation = u.rejection_explanation,
    t.updated_at = u.updated_at
""")

# COMMAND ----------

log("AGENT", f"Updated rejection_explanation for {len(updates)} file(s).")
