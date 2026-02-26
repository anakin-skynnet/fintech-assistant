# Databricks notebook source
# MAGIC %md
# MAGIC # Job 3: Move Rejected Files to SharePoint Review Folder
# MAGIC Find audit rows with validation_status=rejected and moved_to_review_at IS NULL; upload to SharePoint review folder; set moved_to_review_at. Idempotent: only moves not-yet-moved.

# COMMAND ----------

import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("volume_raw", "raw_closure_files", "Volume (raw)")
dbutils.widgets.text("secret_scope", "getnet-sharepoint", "Secret scope")

# COMMAND ----------

import sys

def _notebook_dir():
    """Resolve the notebook's parent directory in the Databricks workspace filesystem."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_path = ctx.notebookPath().get()
        return "/Workspace" + os.path.dirname(nb_path)
    except Exception:
        return "/Workspace"

sys.path.insert(0, os.path.join(_notebook_dir(), "..", "python"))
from notebook_utils import safe_catalog, safe_schema, log

catalog = safe_catalog(dbutils.widgets.get("catalog"))
schema = safe_schema(dbutils.widgets.get("schema"))
volume_raw = (dbutils.widgets.get("volume_raw") or "raw_closure_files").strip()
secret_scope = (dbutils.widgets.get("secret_scope") or "getnet-sharepoint").strip()
full_schema = f"{catalog}.{schema}"
audit_table = f"{full_schema}.closure_file_audit"

# COMMAND ----------

def get_secret(key):
    return dbutils.secrets.get(scope=secret_scope, key=key)

try:
    tenant_id = get_secret("tenant_id")
    client_id = get_secret("client_id")
    client_secret = get_secret("client_secret")
    site_id = get_secret("sharepoint_site_id")
    drive_id = get_secret("sharepoint_drive_id")
    review_folder_path = get_secret("sharepoint_review_folder_path")
except Exception as e:
    log("REJECT", "Fatal: missing or invalid secrets in scope", secret_scope, str(e))
    dbutils.notebook.exit('{"success": false, "reason": "secrets_failed"}')

# COMMAND ----------

from sharepoint_client import get_graph_token, upload_file

# COMMAND ----------

access_token = get_graph_token(tenant_id, client_id, client_secret)

# COMMAND ----------

# Rejected files not yet moved
df = spark.sql(f"""
SELECT file_name, file_path_in_volume, attachment_paths
FROM {audit_table}
WHERE validation_status = 'rejected' AND moved_to_review_at IS NULL
""")
rows = df.collect()

# COMMAND ----------

now = datetime.utcnow()
updated_ids = []

for row in rows:
    file_path = row.file_path_in_volume
    file_name = row.file_name
    try:
        with open(file_path, "rb") as f:
            content = f.read()
        upload_file(access_token, site_id, drive_id, review_folder_path, file_name, content)
        updated_ids.append((file_path, now))
    except Exception as e:
        log("REJECT", f"Failed to upload {file_name}:", e)

# COMMAND ----------

# Update audit: set moved_to_review_at for successfully uploaded files
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

dt = DeltaTable.forName(spark, audit_table)
for file_path, ts in updated_ids:
    dt.update(col("file_path_in_volume") == file_path, {"moved_to_review_at": lit(ts), "updated_at": lit(ts)})

# COMMAND ----------

log("REJECT", f"Moved {len(updated_ids)} rejected file(s) to SharePoint review folder.")
