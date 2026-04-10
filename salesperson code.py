import requests
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
from typing import Dict, Any

def load_jackielondon_creds() -> Dict[str, Any]:
    if is_running_in_lambda():
        return load_secrets("JACKIE_LONDON_KEYS")
    return load_secrets_locally("JACKIE_LONDON_KEYS")


CREDS = load_jackielondon_creds()
HS_TOKEN: str = CREDS.get("HUBSPOT_TOKEN", "")



HEADERS = {
    "Authorization": f"Bearer {HS_TOKEN}",
    "Content-Type": "application/json"
}

SALESPERSON_PROP = "salesperson_code"


def get_all_companies():
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    params = {
        "limit": 100,
        "properties": SALESPERSON_PROP,
        "associations": "contacts"
    }

    companies = []
    after = None

    while True:
        if after:
            params["after"] = after

        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()

        companies.extend(data.get("results", []))

        paging = data.get("paging", {})
        next_link = paging.get("next", {})
        after = next_link.get("after")

        if not after:
            break

    return companies


def update_contacts(contact_ids, salesperson):
    if not contact_ids or not salesperson:
        return

    unique_ids = list(set(contact_ids))

    payload = {
        "inputs": [
            {
                "id": cid,
                "properties": {
                    SALESPERSON_PROP: salesperson
                }
            }
            for cid in unique_ids
        ]
    }

    resp = requests.post(
        "https://api.hubapi.com/crm/v3/objects/contacts/batch/update",
        headers=HEADERS,
        json=payload
    )
    resp.raise_for_status()


def sync_all_companies():
    companies = get_all_companies()

    for company in companies:
        props = company.get("properties", {})
        salesperson = props.get(SALESPERSON_PROP)

        if not salesperson:
            continue

        contacts = company.get("associations", {}).get("contacts", {}).get("results", [])
        contact_ids = [c["id"] for c in contacts]

        update_contacts(contact_ids, salesperson)


# run
sync_all_companies()