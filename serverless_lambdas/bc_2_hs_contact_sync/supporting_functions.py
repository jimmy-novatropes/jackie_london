import requests
import json
from typing import Dict, Any, List, Optional, Tuple
BC_SCOPE = "https://api.businesscentral.dynamics.com/.default"


# ---------------- BC paging ----------------
def get_page(
    url: str, headers: Dict[str, str], top: int
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    # if "$top=" not in url:
    #     sep = "&" if "?" in url else "?"
    #     url = f"{url}{sep}$top={top}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value", []), data.get("@odata.nextLink")


# ---------------- Auth ----------------
def get_bc_token(tenant_id, client_id, client_secret) -> str:
    token_url = (
        "https://login.microsoftonline.com/"
        f"{tenant_id}/oauth2/v2.0/token"
    )
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": BC_SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def load_json_file(path: str) -> dict:
    """Safely load JSON from file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load JSON {path}: {e}")
        return {}