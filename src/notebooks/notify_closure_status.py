# Databricks notebook source
# MAGIC %md
# MAGIC # Notify closure status (rejected / global sent / blocked)
# MAGIC Sends email via Graph API. Call from jobs or alerts. Requires getnet-outlook (or getnet-sharepoint) scope with financial_lead_email and Mail.Send.

# COMMAND ----------

import json
import os

# COMMAND ----------

dbutils.widgets.text("notification_type", "rejected", "Type: rejected | global_sent | blocked")
dbutils.widgets.text("message", "", "Short message body")
dbutils.widgets.text("link", "", "Optional link to app or dashboard")
dbutils.widgets.text("secret_scope", "getnet-outlook", "Secret scope for Outlook/Graph")
dbutils.widgets.text("to_email", "", "Override recipient (empty = use financial_lead_email)")

# COMMAND ----------

notification_type = dbutils.widgets.get("notification_type")
message = dbutils.widgets.get("message")
link = dbutils.widgets.get("link")
secret_scope = dbutils.widgets.get("secret_scope")
to_email = dbutils.widgets.get("to_email").strip()

if not to_email:
    to_email = dbutils.secrets.get(scope=secret_scope, key="financial_lead_email")

# COMMAND ----------

subject_map = {
    "rejected": "Getnet Closure — File(s) rejected (action needed)",
    "global_sent": "Getnet Closure — Global closure sent",
    "blocked": "Getnet Closure — Global closure blocked (missing BUs)",
}
subject = subject_map.get(notification_type, "Getnet Closure — Notification")
body = message + ("\n\nLink: " + link if link else "")

# COMMAND ----------

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
tenant_id = dbutils.secrets.get(scope=secret_scope, key="tenant_id")
client_id = dbutils.secrets.get(scope=secret_scope, key="client_id")
client_secret = dbutils.secrets.get(scope=secret_scope, key="client_secret")
from outlook_send import get_graph_token, send_mail_simple
token = get_graph_token(tenant_id, client_id, client_secret)
send_mail_simple(token, to_email=to_email, subject=subject, body_text=body)
print(f"Sent '{notification_type}' notification to {to_email}.")
