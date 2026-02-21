# Databricks notebook source
# MAGIC %md
# MAGIC # Job 1: Ingest from SharePoint to UC Volume
# MAGIC Lists files in the SharePoint BU closure folder, downloads Excel and attachments, writes to raw_closure_files volume.

# COMMAND ----------

import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("volume_raw", "raw_closure_files", "Volume (raw files)")
dbutils.widgets.text("secret_scope", "getnet-sharepoint", "Secret scope for SharePoint/Graph")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_raw = dbutils.widgets.get("volume_raw")
secret_scope = dbutils.widgets.get("secret_scope")

volume_base = f"/Volumes/{catalog}/{schema}/{volume_raw}"
# Subfolder by date for organization
date_str = datetime.utcnow().strftime("%Y-%m-%d")
volume_path = f"{volume_base}/{date_str}"

# COMMAND ----------

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope=secret_scope, key=key)

tenant_id = get_secret("tenant_id")
client_id = get_secret("client_id")
client_secret = get_secret("client_secret")
site_id = get_secret("sharepoint_site_id")
drive_id = get_secret("sharepoint_drive_id")
folder_path = get_secret("sharepoint_folder_path")

# COMMAND ----------

import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
from sharepoint_client import get_graph_token, list_folder, download_file, DriveItem

# COMMAND ----------

access_token = get_graph_token(tenant_id, client_id, client_secret)
items = list_folder(access_token, site_id, drive_id, folder_path)

# Filter: Excel and common attachment extensions
EXCEL_EXT = (".xlsx", ".xls")
ATTACHMENT_EXT = (".txt", ".pdf", ".doc", ".docx")
def keep(item: DriveItem) -> bool:
    if not item.is_file:
        return False
    low = item.name.lower()
    return low.endswith(EXCEL_EXT) or low.endswith(ATTACHMENT_EXT)

files_to_download = [i for i in items if keep(i)]

# COMMAND ----------

# Ensure volume path exists
dbutils.fs.mkdirs(volume_path)

# COMMAND ----------

downloaded = 0
skipped = 0
for item in files_to_download:
    dest = f"{volume_path}/{item.name}"
    try:
        dbutils.fs.ls(dest)
        skipped += 1
        continue
    except Exception:
        pass
    try:
        content = download_file(access_token, item.download_url)
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(item.name)[1]) as tmp:
            tmp.write(content)
            tmp.flush()
            dbutils.fs.cp(f"file:{tmp.name}", dest)
        os.unlink(tmp.name)
        downloaded += 1
    except Exception as e:
        print(f"Failed to download {item.name}: {e}")

# COMMAND ----------

print(f"Downloaded: {downloaded}, Skipped (exists): {skipped}, Total listed: {len(files_to_download)}")
