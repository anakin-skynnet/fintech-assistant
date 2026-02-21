# Databricks notebook source
# MAGIC %md
# MAGIC # Closure anomaly detection
# MAGIC Compares current period vs prior period (totals by BU); writes rows to closure_anomalies when variance exceeds threshold.

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("variance_pct_threshold", "15", "Variance % to flag (e.g. 15 = 15%)")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
threshold_pct = float(dbutils.widgets.get("variance_pct_threshold"))
full_schema = f"{catalog}.{schema}"
closure_table = f"{full_schema}.closure_data"
anomalies_table = f"{full_schema}.closure_anomalies"

# COMMAND ----------

# Current and prior period
now = datetime.utcnow()
current_period = now.strftime("%Y-%m")
# Prior month
year, month = map(int, current_period.split("-"))
month -= 1
if month < 1:
    month, year = 12, year - 1
prior_period = f"{year}-{month:02d}"

# COMMAND ----------

# Totals by BU this period and prior period
current_df = spark.sql(f"""
  SELECT business_unit, SUM(COALESCE(amount, 0)) AS total_amount, COUNT(*) AS row_count
  FROM {closure_table}
  WHERE closure_period = '{current_period}'
  GROUP BY business_unit
""")
prior_df = spark.sql(f"""
  SELECT business_unit, SUM(COALESCE(amount, 0)) AS total_amount, COUNT(*) AS row_count
  FROM {closure_table}
  WHERE closure_period = '{prior_period}'
  GROUP BY business_unit
""")

current_by_bu = {row.business_unit: (row.total_amount, row.row_count) for row in current_df.collect()}
prior_by_bu = {row.business_unit: (row.total_amount, row.row_count) for row in prior_df.collect()}

# COMMAND ----------

anomaly_rows = []
for bu, (curr_amt, curr_cnt) in current_by_bu.items():
    prev_amt, prev_cnt = prior_by_bu.get(bu) or (0.0, 0)
    if prev_amt and prev_amt > 0:
        pct_change = ((curr_amt - prev_amt) / prev_amt) * 100
        if abs(pct_change) >= threshold_pct:
            severity = "high" if abs(pct_change) >= 2 * threshold_pct else "medium"
            anomaly_rows.append({
                "period": current_period,
                "business_unit": bu,
                "metric": "total_amount",
                "expected_value": prev_amt,
                "actual_value": curr_amt,
                "severity": severity,
                "detected_at": now,
            })
    if prev_cnt and prev_cnt > 0 and curr_cnt != prev_cnt:
        pct_change = ((curr_cnt - prev_cnt) / prev_cnt) * 100
        if abs(pct_change) >= threshold_pct:
            anomaly_rows.append({
                "period": current_period,
                "business_unit": bu,
                "metric": "row_count",
                "expected_value": float(prev_cnt),
                "actual_value": float(curr_cnt),
                "severity": "medium",
                "detected_at": now,
            })

# COMMAND ----------

if anomaly_rows:
    spark.createDataFrame(anomaly_rows).write.format("delta").mode("append").saveAsTable(anomalies_table)
    print(f"Wrote {len(anomaly_rows)} anomaly row(s) for {current_period}.")
else:
    print(f"No anomalies for {current_period} (threshold {threshold_pct}%).")
