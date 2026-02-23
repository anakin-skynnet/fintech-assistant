# Databricks notebook source
# MAGIC %md
# MAGIC # Demo bootstrap — seed volume with sample closure Excel files
# MAGIC Creates valid sample files (BU_A, BU_B, BU_C) in the raw volume under a date folder so the pipeline can run without SharePoint or manual upload. Run once after setup_uc.

# COMMAND ----------

import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("volume_raw", "raw_closure_files", "Volume (raw)")
dbutils.widgets.text("bus", "BU_A,BU_B,BU_C", "Comma-separated business units (one file per BU)")
dbutils.widgets.text("rows_per_file", "5", "Rows per Excel file")
dbutils.widgets.text("value_date", "", "Value date yyyy-MM-dd (empty = today)")

# COMMAND ----------

import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
from notebook_utils import safe_catalog, safe_schema, log

catalog = safe_catalog(dbutils.widgets.get("catalog"))
schema = safe_schema(dbutils.widgets.get("schema"))
volume_raw = (dbutils.widgets.get("volume_raw") or "raw_closure_files").strip()
bus_str = dbutils.widgets.get("bus")
rows_per_file = max(1, min(1000, int(dbutils.widgets.get("rows_per_file") or "5")))
value_date_str = dbutils.widgets.get("value_date").strip()

bus_list = [b.strip() for b in bus_str.split(",") if b.strip()]
if not bus_list:
    bus_list = ["BU_A", "BU_B", "BU_C"]

value_date = value_date_str or datetime.utcnow().strftime("%Y-%m-%d")
period_suffix = value_date[:7].replace("-", "")  # 202502

volume_base = f"/Volumes/{catalog}/{schema}/{volume_raw}"
date_folder = datetime.utcnow().strftime("%Y-%m-%d")
volume_folder = f"{volume_base}/{date_folder}"

# COMMAND ----------

import pandas as pd

def make_sample_rows(business_unit: str, num_rows: int, base_date: str) -> list:
    rows = []
    for i in range(1, num_rows + 1):
        rows.append({
            "amount": 10000.0 * i + 500,
            "currency": "BRL",
            "account_code": f"ACC-{business_unit}-{i:04d}",
            "description": f"Demo closure line {i} for {business_unit}",
            "value_date": base_date,
            "business_unit": business_unit,
        })
    return rows

columns = ["amount", "currency", "account_code", "description", "value_date", "business_unit"]

# COMMAND ----------

dbutils.fs.mkdirs(volume_folder)
created = []

for bu in bus_list:
    rows = make_sample_rows(bu, rows_per_file, value_date)
    df = pd.DataFrame(rows, columns=columns)
    file_name = f"closure_{bu.lower().replace('_', '_')}_{period_suffix}.xlsx"
    local_path = f"/tmp/{file_name}"
    df.to_excel(local_path, index=False, sheet_name="Closure", engine="openpyxl")
    volume_path = f"{volume_folder}/{file_name}"
    dbutils.fs.cp(f"file:{local_path}", volume_path)
    try:
        os.remove(local_path)
    except Exception:
        pass
    created.append(volume_path)
    log("BOOTSTRAP", f"Created {volume_path}")

# COMMAND ----------

log("BOOTSTRAP", f"Done: {len(created)} file(s) in {volume_folder}. Run Validate and load or the full pipeline next.")
