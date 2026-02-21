# Databricks notebook source
# MAGIC %md
# MAGIC # Closure SLA and quality metrics
# MAGIC Computes first_file_at, first_valid_at, hours_to_valid, files valid/rejected per period and BU; and quality summary (pct valid, most common errors).

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("period", "", "Period (yyyy-MM); empty = current month")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
period = dbutils.widgets.get("period").strip() or datetime.utcnow().strftime("%Y-%m")
full_schema = f"{catalog}.{schema}"
audit_table = f"{full_schema}.closure_file_audit"
errors_view = f"{full_schema}.closure_audit_errors"
sla_table = f"{full_schema}.closure_sla_metrics"
quality_table = f"{full_schema}.closure_quality_summary"

# COMMAND ----------

# SLA: per BU and period, first file time, first valid time, hours to valid, counts
sla_df = spark.sql(f"""
  SELECT
    date_format(processed_at, 'yyyy-MM') AS period,
    business_unit,
    MIN(processed_at) AS first_file_at,
    MIN(CASE WHEN validation_status = 'valid' THEN processed_at END) AS first_valid_at,
    COUNT(CASE WHEN validation_status = 'rejected' THEN 1 END) AS files_rejected,
    COUNT(CASE WHEN validation_status = 'valid' THEN 1 END) AS files_valid
  FROM {audit_table}
  WHERE date_format(processed_at, 'yyyy-MM') = '{period}'
  GROUP BY date_format(processed_at, 'yyyy-MM'), business_unit
""")
# Compute hours_to_valid
from pyspark.sql import functions as F
sla_df = sla_df.withColumn(
  "hours_to_valid",
  F.when(
    F.col("first_valid_at").isNotNull() & F.col("first_file_at").isNotNull(),
    (F.unix_timestamp("first_valid_at") - F.unix_timestamp("first_file_at")) / 3600.0
  ).otherwise(None)
)
# Merge: replace only this period's rows
sla_df.createOrReplaceTempView("sla_updates")
spark.sql(f"""
  MERGE INTO {sla_table} AS t
  USING (SELECT * FROM sla_updates) AS u ON t.period = u.period AND t.business_unit <=> u.business_unit
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
print(f"SLA metrics written for {period}.")

# COMMAND ----------

# Quality summary: total files, pct valid, pct rejected, most common error types
audit_summary = spark.sql(f"""
  SELECT
    COUNT(*) AS total_files,
    SUM(CASE WHEN validation_status = 'valid' THEN 1 ELSE 0 END) AS valid_count,
    SUM(CASE WHEN validation_status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count
  FROM {audit_table}
  WHERE date_format(processed_at, 'yyyy-MM') = '{period}'
""").collect()[0]
total = audit_summary.total_files or 0
valid_count = audit_summary.valid_count or 0
rejected_count = audit_summary.rejected_count or 0
pct_valid = (valid_count / total * 100) if total else 0
pct_rejected = (rejected_count / total * 100) if total else 0

# Most common error causes from closure_audit_errors
try:
    err_counts = spark.sql(f"""
      SELECT invalid_cause, COUNT(*) AS cnt
      FROM {errors_view}
      WHERE date_format(processed_at, 'yyyy-MM') = '{period}'
      GROUP BY invalid_cause
      ORDER BY cnt DESC
      LIMIT 5
    """).collect()
    most_common = ", ".join(f"{r.invalid_cause}({r.cnt})" for r in err_counts)
except Exception:
    most_common = ""

now = datetime.utcnow()
quality_row = [{
  "period": period,
  "total_files": total,
  "pct_valid": round(pct_valid, 2),
  "pct_rejected": round(pct_rejected, 2),
  "most_common_error_types": most_common,
  "updated_at": now,
}]
# Merge by period
new_row_df = spark.createDataFrame(quality_row)
new_row_df.createOrReplaceTempView("quality_updates")
spark.sql(f"""
  MERGE INTO {quality_table} AS t
  USING (SELECT * FROM quality_updates) AS u ON t.period = u.period
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
print(f"Quality summary: {period} — valid {pct_valid:.1f}%, rejected {pct_rejected:.1f}%.")
