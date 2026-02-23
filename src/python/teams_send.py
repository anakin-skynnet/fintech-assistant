"""
Send a simple message to a Microsoft Teams channel via an incoming webhook.
Use for closure notifications (e.g. "Global closure for 2025-02 sent") without Graph API.
Configure the webhook URL in job widgets or secret (e.g. teams_webhook_url).
"""

import requests
from typing import Optional


def send_teams_webhook(webhook_url: str, text: str, *, timeout: int = 10) -> None:
    """
    POST a plain text message to a Teams incoming webhook URL.
    Raises on HTTP error.
    """
    if not webhook_url or not webhook_url.strip():
        return
    body = {"text": text}
    r = requests.post(webhook_url.strip(), json=body, timeout=timeout)
    r.raise_for_status()


def format_global_sent_message(period: str, attachment_name: str) -> str:
    """Short message for Teams when global closure is sent."""
    return (
        f"**Getnet Global Financial Closure** — Period **{period}** has been sent. "
        f"Attachment: {attachment_name}. Check your email for the file."
    )


def format_rejected_message(business_unit: str, file_name: str) -> str:
    """Short message for Teams when a BU file is rejected."""
    return (
        f"Getnet Closure: BU **{business_unit}** — file **{file_name}** was rejected. "
        "Check email for details and fix then re-upload."
    )
