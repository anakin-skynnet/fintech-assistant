# Databricks notebook source
# MAGIC %md
# MAGIC # Notify BU on rejection
# MAGIC For each rejected file (moved to review), send the BU contact an email with rejection explanation and link.
# MAGIC Uses config/bu_contacts.yaml (or secret) to resolve business_unit → email. Optional: post to Teams.
# MAGIC Run after reject_to_sharepoint in the pipeline so review folder link is available.

# COMMAND ----------

import os
import sys
import yaml
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("secret_scope", "getnet-outlook", "Secret scope for Outlook/Graph")
dbutils.widgets.text("bu_contacts_path", "", "Path to bu_contacts.yaml (empty = use default workspace path)")
dbutils.widgets.text("app_link", "", "Optional link to app or Genie (included in email)")
dbutils.widgets.text("teams_webhook_url", "", "Optional: Teams incoming webhook URL (empty = no Teams)")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
full_schema = f"{catalog}.{schema}"
audit_table = f"{full_schema}.closure_file_audit"
secret_scope = dbutils.widgets.get("secret_scope")
bu_contacts_path = dbutils.widgets.get("bu_contacts_path").strip()
app_link = dbutils.widgets.get("app_link").strip()
teams_webhook_url = dbutils.widgets.get("teams_webhook_url").strip()

# COMMAND ----------

# Load BU contacts: bu_contacts_path or default config
def load_bu_contacts():
    if bu_contacts_path:
        with open(bu_contacts_path, "r") as f:
            data = yaml.safe_load(f)
        return {c["business_unit"]: c for c in (data.get("bu_contacts") or [])}
    try:
        # Repo root: config/bu_contacts.yaml (when run from bundle, path is relative to repo)
        repo_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
        path = os.path.join(repo_root, "config", "bu_contacts.yaml")
        if os.path.exists(path):
            with open(path, "r") as f:
                data = yaml.safe_load(f)
            return {c["business_unit"]: c for c in (data.get("bu_contacts") or [])}
    except Exception as e:
        print(f"Could not load bu_contacts: {e}")
    return {}

bu_map = load_bu_contacts()
if not bu_map:
    print("No bu_contacts loaded. Set bu_contacts_path or place config/bu_contacts.yaml in workspace.")

# COMMAND ----------

# Rejected files already moved to review (so we have a place to point them to)
# Optional: only those not yet notified if you add notified_bu_at to closure_file_audit
audit_cols = [c.name for c in spark.table(audit_table).schema]
has_notified = "notified_bu_at" in audit_cols

if has_notified:
    rejected_df = spark.sql(f"""
    SELECT file_name, file_path_in_volume, business_unit, rejection_explanation, rejection_reason, processed_at
    FROM {audit_table}
    WHERE validation_status = 'rejected' AND moved_to_review_at IS NOT NULL AND notified_bu_at IS NULL
    """)
else:
    rejected_df = spark.sql(f"""
    SELECT file_name, file_path_in_volume, business_unit, rejection_explanation, rejection_reason, processed_at
    FROM {audit_table}
    WHERE validation_status = 'rejected' AND moved_to_review_at IS NOT NULL
    """)

rejected = rejected_df.collect()
if not rejected:
    print("No rejected files to notify (or already notified).")
    dbutils.notebook.exit("0")

# COMMAND ----------

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
tenant_id = dbutils.secrets.get(scope=secret_scope, key="tenant_id")
client_id = dbutils.secrets.get(scope=secret_scope, key="client_id")
client_secret = dbutils.secrets.get(scope=secret_scope, key="client_secret")
from outlook_send import get_graph_token, send_mail_simple

token = get_graph_token(tenant_id, client_id, client_secret)

# COMMAND ----------

def send_teams_if_configured(message: str):
    if not teams_webhook_url:
        return
    try:
        import requests
        body = {"text": message}
        r = requests.post(teams_webhook_url, json=body, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"Teams post failed: {e}")

# COMMAND ----------

period = datetime.utcnow().strftime("%Y-%m")
notified_paths = []

for row in rejected:
    bu = row.business_unit or "Unknown"
    email_info = bu_map.get(bu, {})
    to_email = (email_info.get("email") or "").strip()
    if not to_email:
        print(f"No contact for BU {bu}; skip file {row.file_name}")
        continue

    explanation = (row.rejection_explanation or row.rejection_reason or "See validation errors in the app.").strip()
    body = f"""Your financial closure file was rejected and moved to the review folder.

File: {row.file_name}
Business unit: {bu}
Processed at: {row.processed_at}

Rejection details:
{explanation}
"""
    if app_link:
        body += f"\nLink to app / dashboard: {app_link}"

    subject = f"Getnet Closure — File rejected: {row.file_name} (action needed)"
    try:
        send_mail_simple(token, to_email=to_email, subject=subject, body_text=body)
        notified_paths.append(row.file_path_in_volume)
        send_teams_if_configured(f"Getnet Closure: BU **{bu}** — file **{row.file_name}** rejected. Check email for details.")
    except Exception as e:
        print(f"Failed to send to {to_email} for {row.file_name}: {e}")

# COMMAND ----------

# Mark as notified if column exists (avoids duplicate emails on next run)
if has_notified and notified_paths:
    from delta.tables import DeltaTable
    from pyspark.sql.functions import col, lit
    now = datetime.utcnow()
    dt = DeltaTable.forName(spark, audit_table)
    for path in notified_paths:
        dt.update(col("file_path_in_volume") == path, {"notified_bu_at": lit(now), "updated_at": lit(now)})

# COMMAND ----------

print(f"Notified BU for {len(notified_paths)} rejected file(s).")
