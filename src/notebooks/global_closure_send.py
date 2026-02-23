# Databricks notebook source
# MAGIC %md
# MAGIC # Job 4: Global Closure and Send
# MAGIC When all expected BUs have valid (and optionally reviewer-approved) files for the period: aggregate closure_data, write global file, send to Financial Lead and global team via Outlook, log in global_closure_sent and global_closure_recipients.

# COMMAND ----------

import os
import sys
import json
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog", "getnet_closure_dev", "Catalog")
dbutils.widgets.text("schema", "financial_closure", "Schema")
dbutils.widgets.text("closure_period_type", "monthly", "Closure period type")
dbutils.widgets.text("secret_scope_sharepoint", "getnet-sharepoint", "Secret scope (SharePoint/Graph)")
dbutils.widgets.text("secret_scope_outlook", "getnet-outlook", "Secret scope (Outlook - optional, can same as SharePoint)")
dbutils.widgets.text("run_prefix_job_key", "global_closure_send", "Job key for run counter prefix (e.g. global_closure_send or closure_pipeline)")
dbutils.widgets.dropdown("require_reviewer_approval", "false", ["true", "false"], "Require reviewer approval per BU (need approval_status column)")
dbutils.widgets.text("closure_roles_path", "", "Optional: path to closure_roles.yaml for global_team (or use secret global_team_emails)")
dbutils.widgets.text("global_team_emails", "", "Override: comma-separated global_team emails (overrides closure_roles_path/secret)")
dbutils.widgets.text("teams_webhook_url", "", "Optional: Teams incoming webhook URL to post when global closure is sent")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
full_schema = f"{catalog}.{schema}"
audit_table = f"{full_schema}.closure_file_audit"
closure_table = f"{full_schema}.closure_data"
global_sent_table = f"{full_schema}.global_closure_sent"
recipients_table = f"{full_schema}.global_closure_recipients"
scope_sp = dbutils.widgets.get("secret_scope_sharepoint")
scope_outlook = dbutils.widgets.get("secret_scope_outlook")
run_prefix_job_key = dbutils.widgets.get("run_prefix_job_key").strip() or "global_closure_send"
require_reviewer_approval = dbutils.widgets.get("require_reviewer_approval") == "true"

# Current closure period (e.g. 2025-02)
period = datetime.utcnow().strftime("%Y-%m")

# COMMAND ----------

# Expected BUs (from config or default)
expected_bus = ["BU_A", "BU_B", "BU_C"]  # Override by loading config/business_units.yaml if available

# COMMAND ----------

# Check: all expected BUs have at least one valid file in this period
# If require_reviewer_approval: also require at least one file per BU with approval_status = 'approved'
audit_columns = [c.name for c in spark.table(audit_table).schema]
has_approval = "approval_status" in audit_columns

if require_reviewer_approval and not has_approval:
    print("require_reviewer_approval is true but closure_file_audit has no approval_status column. Run setup_uc_workflow_extension. Using valid-only gate.")
    require_reviewer_approval = False

if require_reviewer_approval:
    valid_per_bu = spark.sql(f"""
    SELECT business_unit, count(*) as cnt
    FROM {audit_table}
    WHERE validation_status = 'valid'
      AND date_format(processed_at, 'yyyy-MM') = '{period}'
      AND approval_status = 'approved'
    GROUP BY business_unit
    """)
else:
    valid_per_bu = spark.sql(f"""
    SELECT business_unit, count(*) as cnt
    FROM {audit_table}
    WHERE validation_status = 'valid'
      AND date_format(processed_at, 'yyyy-MM') = '{period}'
    GROUP BY business_unit
    """)
valid_bus = {row.business_unit for row in valid_per_bu.collect() if row.business_unit}
missing = set(expected_bus) - valid_bus
if missing:
    print(f"Not all BUs ready. Missing: {missing}. Skip send.")
    dbutils.notebook.exit(json.dumps({"sent": False, "reason": f"missing_bus_{list(missing)}"}))

# COMMAND ----------

# Already sent for this period?
sent = spark.sql(f"SELECT 1 FROM {global_sent_table} WHERE closure_period = '{period}' LIMIT 1").count()
if sent > 0:
    print(f"Already sent for period {period}. Skip.")
    dbutils.notebook.exit(json.dumps({"sent": False, "reason": "already_sent"}))

# COMMAND ----------

# Aggregate closure data for period
df = spark.sql(f"""
SELECT * FROM {closure_table}
WHERE closure_period = '{period}'
ORDER BY business_unit, row_index
""")
pdf = df.toPandas()

# COMMAND ----------

# Get next execution number (prefix) from closure_run_counter
counter_table = f"{full_schema}.closure_run_counter"
run_number = 1
try:
    spark.sql(f"""
      UPDATE {counter_table}
      SET run_number = run_number + 1, updated_at = current_timestamp()
      WHERE job_key = '{run_prefix_job_key}'
    """)
    row = spark.sql(f"SELECT run_number FROM {counter_table} WHERE job_key = '{run_prefix_job_key}'").first()
    if row is not None:
        run_number = int(row.run_number)
except Exception as e:
    print(f"Run counter read failed, using run_number=1: {e}")

# COMMAND ----------

# Write global file (Excel or CSV) to global_closure_output volume; prefix = execution number
out_volume = "global_closure_output"
out_dir = f"/Volumes/{catalog}/{schema}/{out_volume}"
out_name = f"{run_number:03d}_global_closure_{period}.csv"
out_path = f"{out_dir}/{out_name}"
dbutils.fs.mkdirs(out_dir)
print(f"Execution #{run_number} — writing {out_name}")

# Write to local temp then copy to volume
import tempfile
with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as tmp:
    pdf.to_csv(tmp.name, index=False)
    with open(tmp.name, "rb") as f:
        content = f.read()
    os.unlink(tmp.name)
# Write to volume via dbfs
with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
    tmp.write(content)
    tmp.flush()
    dbutils.fs.cp(f"file:{tmp.name}", out_path)
    os.unlink(tmp.name)

# COMMAND ----------

# Optional: generate executive summary via agent (if enabled)
body_text = f"Please find attached the global closure file for period {period}."
try:
    metrics_lines = [f"Period: {period}. Total rows: {len(pdf)}. Total amount: {pdf['amount'].sum():.2f}."]
    if "business_unit" in pdf.columns:
        for bu, grp in pdf.groupby("business_unit"):
            metrics_lines.append(f"  {bu}: {len(grp)} rows, amount {grp['amount'].sum():.2f}.")
    metrics_summary = "\n".join(metrics_lines)
    system_msg = "You are a financial analyst assistant. Summarize closure metrics in 3-5 bullet points for the Financial Lead."
    user_msg = f"Closure period: {period}\nAggregated metrics:\n{metrics_summary}\n\nProduce a short executive summary for the email body."
    from databricks.ai import ai
    reply = ai.llm.chat(system=system_msg, messages=[{"role": "user", "content": user_msg}])
    if reply.choices and reply.choices[0].message.content:
        body_text = reply.choices[0].message.content.strip() + "\n\n---\nAttached: global closure file for " + period + "."
except Exception as e:
    print(f"Agent summary skipped (using default body): {e}")

# COMMAND ----------

# Load global_team list: widget override > secret global_team_emails > closure_roles.yaml
def load_global_team_emails():
    w_emails = dbutils.widgets.get("global_team_emails").strip()
    if w_emails:
        return [e.strip() for e in w_emails.split(",") if e.strip()]
    try:
        raw = dbutils.secrets.get(scope=scope_outlook, key="global_team_emails")
        return [e.strip() for e in raw.split(",") if e.strip()]
    except Exception:
        pass
    roles_path = dbutils.widgets.get("closure_roles_path").strip()
    if roles_path:
        try:
            path = roles_path.replace("/dbfs", "") if roles_path.startswith("/dbfs") else roles_path
            with open(path, "r") as f:
                import yaml
                roles = yaml.safe_load(f)
                gt = roles.get("global_team") or []
                return [e.strip() for e in (gt if isinstance(gt, list) else [gt]) if e and str(e).strip()]
        except Exception as e:
            print(f"Could not load closure_roles from {roles_path}: {e}")
    return []

global_team_emails = load_global_team_emails()

# COMMAND ----------

# Send via Outlook
financial_lead_email = dbutils.secrets.get(scope=scope_outlook, key="financial_lead_email")
tenant_id = dbutils.secrets.get(scope=scope_sp, key="tenant_id")
client_id = dbutils.secrets.get(scope=scope_sp, key="client_id")
client_secret = dbutils.secrets.get(scope=scope_sp, key="client_secret")

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
from outlook_send import get_graph_token, send_mail_with_attachment

token = get_graph_token(tenant_id, client_id, client_secret)
send_mail_with_attachment(
    token,
    to_email=financial_lead_email,
    subject=f"Getnet Global Financial Closure {period}",
    body_text=body_text,
    attachment_name=out_name,
    attachment_content=content,
)

# COMMAND ----------

try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
except Exception:
    run_id = f"run-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
now = datetime.utcnow()

# Log recipient (financial_lead)
recipient_rows = [{"closure_period": period, "sent_at": now, "recipient_email": financial_lead_email, "recipient_role": "financial_lead", "job_run_id": run_id}]

# Send to global team and log each
for to_email in global_team_emails:
    if not to_email or to_email == financial_lead_email:
        continue
    try:
        send_mail_with_attachment(
            token,
            to_email=to_email,
            subject=f"Getnet Global Financial Closure {period}",
            body_text=body_text,
            attachment_name=out_name,
            attachment_content=content,
        )
        recipient_rows.append({"closure_period": period, "sent_at": now, "recipient_email": to_email, "recipient_role": "global_team", "job_run_id": run_id})
    except Exception as e:
        print(f"Failed to send to global_team {to_email}: {e}")

# COMMAND ----------

# Persist recipients log (if table exists)
try:
    spark.createDataFrame(recipient_rows).write.format("delta").mode("append").saveAsTable(recipients_table)
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" not in str(e).upper():
        print(f"Could not write global_closure_recipients: {e}")

# COMMAND ----------

# Log send in global_closure_sent (with global_team_notified_at when we notified global team)
row = {
    "closure_period": period,
    "sent_at": now,
    "job_run_id": run_id,
    "recipient_email": financial_lead_email,
    "file_path": out_path,
}
sent_cols = [c.name for c in spark.table(global_sent_table).schema]
if global_team_emails and "global_team_notified_at" in sent_cols:
    row["global_team_notified_at"] = now
spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(global_sent_table)

# COMMAND ----------

print(f"Global closure for {period} sent to {financial_lead_email}" + (f" and {len(global_team_emails)} global_team recipient(s)." if global_team_emails else "."))

# Optional: post to Teams channel
teams_url = dbutils.widgets.get("teams_webhook_url").strip()
if teams_url:
    try:
        sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
        from teams_send import send_teams_webhook, format_global_sent_message
        send_teams_webhook(teams_url, format_global_sent_message(period, out_name))
        print("Teams notification sent.")
    except Exception as e:
        print(f"Teams webhook failed (non-fatal): {e}")
