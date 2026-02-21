"""
Microsoft Graph client for SharePoint: list folder, download/upload files.
Uses app-only (client credentials) auth. Store tenant_id, client_id, client_secret in Databricks secrets.
"""
import os
import requests
from typing import List, Optional, Tuple
from dataclasses import dataclass

try:
    import msal
except ImportError:
    msal = None


@dataclass
class DriveItem:
    id: str
    name: str
    size: int
    is_file: bool
    download_url: Optional[str] = None


def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Obtain access token for Microsoft Graph using client credentials."""
    if msal is None:
        raise ImportError("msal is required. Install with: pip install msal")
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Failed to acquire token: {result.get('error_description', result)}")
    return result["access_token"]


def list_folder(
    access_token: str,
    site_id: str,
    drive_id: str,
    folder_path: str = "",
) -> List[DriveItem]:
    """
    List items in a SharePoint drive folder.
    folder_path: path relative to drive root, e.g. "BU Closure" or "BU Closure/2025"
    """
    base = "https://graph.microsoft.com/v1.0"
    if folder_path:
        path = f"/sites/{site_id}/drives/{drive_id}/root:/{folder_path}:/children"
    else:
        path = f"/sites/{site_id}/drives/{drive_id}/root/children"
    url = f"{base}{path}"
    headers = {"Authorization": f"Bearer {access_token}"}
    items = []
    while url:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        for item in data.get("value", []):
            is_file = "file" in item
            download_url = None
            if is_file and "content" in item:
                download_url = item["@microsoft.graph.downloadUrl"] or item.get("content", {}).get("url")
            if not download_url and is_file:
                download_url = f"{base}/sites/{site_id}/drives/{drive_id}/items/{item['id']}/content"
            items.append(
                DriveItem(
                    id=item["id"],
                    name=item["name"],
                    size=item.get("size", 0),
                    is_file=is_file,
                    download_url=download_url,
                )
            )
        url = data.get("@odata.nextLink")
    return items


def download_file(access_token: str, download_url: str) -> bytes:
    """Download file content. Use @microsoft.graph.downloadUrl (no auth) or /content endpoint (auth required)."""
    headers = {"Authorization": f"Bearer {access_token}"} if "graph.microsoft.com" in download_url and "/content" in download_url else {}
    r = requests.get(download_url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content


def upload_file(
    access_token: str,
    site_id: str,
    drive_id: str,
    folder_path: str,
    file_name: str,
    content: bytes,
) -> str:
    """Upload file to SharePoint folder. Returns item id."""
    base = "https://graph.microsoft.com/v1.0"
    path = f"/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"
    url = f"{base}{path}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.put(url, headers=headers, data=content, timeout=60)
    r.raise_for_status()
    return r.json().get("id", "")
