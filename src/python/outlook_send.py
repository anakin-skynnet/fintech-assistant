"""
Send email with attachment via Microsoft Graph API (Outlook).
Requires Mail.Send (application) or delegated send. Store credentials in Databricks secrets.
"""
import base64
import requests
from typing import Optional

try:
    import msal
except ImportError:
    msal = None


def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """App-only token for Graph."""
    if msal is None:
        raise ImportError("msal required: pip install msal")
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id, authority=authority, client_credential=client_secret
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(result.get("error_description", str(result)))
    return result["access_token"]


def send_mail_simple(
    access_token: str,
    *,
    to_email: str,
    subject: str,
    body_text: str,
    from_user_id: Optional[str] = None,
) -> None:
    """Send email via Graph API (no attachment)."""
    user_id = from_user_id or "me"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body_text},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        }
    }
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


def send_mail_with_attachment(
    access_token: str,
    *,
    to_email: str,
    subject: str,
    body_text: str,
    attachment_name: str,
    attachment_content: bytes,
    from_user_id: Optional[str] = None,
) -> None:
    """
    Send email via Graph API with one file attachment.
    from_user_id: optional user id (or "me") that sends the mail; requires Mail.Send for that user.
    """
    content_b64 = base64.b64encode(attachment_content).decode("ascii")
    user_id = from_user_id or "me"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body_text},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": attachment_name,
                    "contentBytes": content_b64,
                }
            ],
        }
    }
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
