# Databricks notebook source
# MAGIC %md
# MAGIC # Optional: Agent — Global closure executive summary
# MAGIC Builds metrics from closure_data, calls LLM for 3–5 bullet executive summary.
# MAGIC Run from Job 4 (global_closure_send) before sending email; result can be passed to email body.

# COMMAND ----------

import json
import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("closure_period", "", "Closure period (e.g. 2025-02); empty = current month")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
period = dbutils.widgets.get("closure_period").strip() or datetime.utcnow().strftime("%Y-%m")
full_schema = f"{catalog}.{schema}"
closure_table = f"{full_schema}.closure_data"

# COMMAND ----------

# Build metrics summary from closure_data
df = spark.sql(f"""
  SELECT business_unit, COUNT(*) AS row_count, SUM(COALESCE(amount, 0)) AS total_amount
  FROM {closure_table}
  WHERE closure_period = '{period}'
  GROUP BY business_unit
  ORDER BY business_unit
""")
rows = df.collect()
total_rows = sum(r.row_count for r in rows)
total_amount = sum(r.total_amount for r in rows)
lines = [f"Period: {period}. Total rows: {total_rows}. Total amount: {total_amount:.2f}."]
for r in rows:
    lines.append(f"  {r.business_unit}: {r.row_count} rows, amount {r.total_amount:.2f}.")
metrics_summary = "\n".join(lines)

# COMMAND ----------

system_msg = "You are a financial analyst assistant. Summarize closure metrics in 3-5 bullet points for the Financial Lead."
user_msg = f"""Closure period: {period}
Aggregated metrics (e.g. totals by BU, row counts):
{metrics_summary}

Produce a short executive summary to include in the email body or as an attachment."""

# COMMAND ----------

def generate_summary() -> str:
    try:
        from databricks.ai import ai
        reply = ai.llm.chat(
            system=system_msg,
            messages=[{"role": "user", "content": user_msg}],
        )
        if reply.choices and reply.choices[0].message.content:
            return reply.choices[0].message.content.strip()
    except Exception as e:
        print(f"LLM call failed ({e}), using fallback.")
    return f"Global closure for {period}: {total_rows} rows, total amount {total_amount:.2f}. By BU: " + "; ".join(f"{r.business_unit} {r.total_amount:.2f}" for r in rows)

# COMMAND ----------

summary = generate_summary()
print(summary)
# Store for caller (e.g. run_notebook and read from display)
dbutils.notebook.exit(json.dumps({"summary": summary, "period": period}))
