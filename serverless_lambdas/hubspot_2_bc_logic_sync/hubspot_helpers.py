import json
from typing import Dict, Any
from credentials_load import load_secrets, load_secrets_locally, is_running_in_lambda
import requests


def hubspot_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def _load_creds() -> Dict[str, Any]:
    loader = load_secrets if is_running_in_lambda() else load_secrets_locally
    return loader("JACKIE_LONDON_KEYS")

def invert_dict(d):
    return {v: k for k, v in d.items()}


def load_json_file(file_path):
    """
    Load a JSON file and return its contents as a Python dictionary.

    Args:
    file_path (str): The path to the JSON file.

    Returns:
    dict: The contents of the JSON file as a Python dictionary.
    """
    try:
        with open(file_path, "r") as json_file:
            data = json.load(json_file)
        return data
    except FileNotFoundError:
        # print(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {str(e)}")
        return None


def _get_bc_token(TENANT_ID, CREDS) -> str:
    """Obtain a Business Central OAuth2 access token."""
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CREDS["client_id"],
            "client_secret": CREDS["client_secret"],
            "scope": "https://api.businesscentral.dynamics.com/.default",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_company(company_id, token, properties=None, associations=None):
    params = {}
    if properties:
        params["properties"] = ",".join(properties.keys())
    if associations:
        params["associations"] = ",".join(associations)

    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
    return requests.get(url, headers=hubspot_headers(token), params=params).json()

def get_contact(contact_id, token, properties=None, associations=None):
    params = {}
    if properties:
        params["properties"] = ",".join(properties.keys())
    if associations:
        params["associations"] = ",".join(associations)

    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    return requests.get(url, headers=hubspot_headers(token), params=params).json()
