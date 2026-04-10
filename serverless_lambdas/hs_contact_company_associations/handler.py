"""
HubSpot Deals → Companies Association (NetSuite)
------------------------------------------------
Links unassociated deals to companies using NetSuite Customer ID.

Handles pagination up to ~10K deals efficiently.
"""

import requests
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
# from supporting_functions import load_secrets, load_netsuite_credentials
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
import json
def load_jackielondon_creds() -> Dict[str, Any]:
    if is_running_in_lambda():
        return load_secrets("JACKIE_LONDON_KEYS")
    return load_secrets_locally("JACKIE_LONDON_KEYS")


CREDS = load_jackielondon_creds()
HUBSPOT_TOKEN: str = CREDS.get("HUBSPOT_TOKEN", "")

HS_BASE = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

PAGE_SIZE = 100
MAX_RECORDS = 5000

NETSUITE_CONTACT_PROP = "company"
NETSUITE_COMPANY_PROP = "name"



# === FETCH CONTACTS ======================================================

def get_contacts(weeks_back=24) -> List[Dict]:
    url = f"{HS_BASE}/crm/v3/objects/contacts/search"
    cutoff_dt = datetime.now(timezone.utc) - timedelta(weeks=weeks_back)

    results = []
    after = None

    while True:

        params = {
            "limit": PAGE_SIZE,
            # "properties": ["dealname", NETSUITE_DEAL_PROP, "lastmodifieddate", "num_associated_contacts"],
            "properties": ["email","firstname","lastname",NETSUITE_CONTACT_PROP,"lastmodifieddate", "name"],
            "associations": ["companies"],
            "archived": "false",
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "number_of_associated_companies",
                            "operator": "EQ",
                            "value": "0"
                        },
                        {
                            "propertyName": "bc_unique_id",
                            "operator": "HAS_PROPERTY",
                        },
                        {
                            "propertyName": "company",
                            "operator": "HAS_PROPERTY",
                        }
                    ]
                }
            ],
            "sorts": [
                {
                    # "propertyName": "number_of_associated_stores",
                    # "propertyName": "createdate",
                    "propertyName": "lastmodifieddate",
                    "direction": "DESCENDING"
                }
            ]
        }
        if after:
            params["after"] = after

        r = requests.post(url, headers=HEADERS, json=params)
        r.raise_for_status()
        data = r.json()

        for c in data.get("results", []):
            lastmod = c["properties"].get("lastmodifieddate")
            if not lastmod:
                continue

            lastmod_dt = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
            if lastmod_dt < cutoff_dt:
                return results

            results.append(c)

            if len(results) >= MAX_RECORDS:
                return results

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return results


# === FILTER UNASSOCIATED CONTACTS ========================================

def filter_unassociated(contacts: List[Dict]) -> List[Dict]:
    return [
        c for c in contacts
        if not c.get("associations", {}).get("companies", {}).get("results")
    ]


# === BUILD COMPANY LOOKUP MAP ============================================

def build_company_map(unassoc_contacts: List[Dict]) -> Dict[str, str]:
    netsuite_ids = {
        c["properties"].get(NETSUITE_CONTACT_PROP)
        for c in unassoc_contacts
        if c["properties"].get(NETSUITE_CONTACT_PROP)
    }

    company_map = {}
    ids = list(netsuite_ids)

    ids = [id.strip() for id in ids if id and id.strip()]


    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": NETSUITE_COMPANY_PROP,
                    "operator": "IN",
                    "values": chunk,
                }]
            }],
            "limit": 100,
        }

        r = requests.post(
            f"{HS_BASE}/crm/v3/objects/companies/search",
            headers=HEADERS,
            json=payload,
        )
        r.raise_for_status()

        for comp in r.json().get("results", []):
            ns_id = comp["properties"].get(NETSUITE_COMPANY_PROP)
            if ns_id:
                company_map[ns_id] = comp["id"]
            else:
                print(f"⚠️ Company {comp['id']}, {comp['properties']}")

    return company_map


# === ASSOCIATE CONTACTS → COMPANIES =====================================

def link_contacts_to_companies(unassoc_contacts: List[Dict]) -> int:
    company_map = build_company_map(unassoc_contacts)

    associations = []
    for c in unassoc_contacts:
        ns_id = c["properties"].get(NETSUITE_CONTACT_PROP).strip()
        company_id = company_map.get(ns_id)
        if company_id:
            associations.append({
                "from": {"id": company_id},
                "to": {"id": c["id"]},
                "types": [{
                    "associationCategory": "USER_DEFINED",
                    "associationTypeId": 2
                }]

            })

    total = 0
    for i in range(0, len(associations), 100):
        batch = associations[i:i + 100]
        r = requests.post(
            f"{HS_BASE}/crm/v4/associations/company/contact/batch/create",
            headers=HEADERS,
            json={"inputs": batch},
        )
        if r.status_code in (200, 201, 204):
            total += len(batch)
        else:
            print(f"❌ Batch failed: {r.text[:200]}")

    return total


# === LAMBDA HANDLER ======================================================

def lambda_handler(event=None, context=None):
    print("🚀 Fetching contacts...")
    contacts = get_contacts()

    print(f"📊 Contacts fetched: {len(contacts)}")
    # unassoc = filter_unassociated(contacts)
    # print(f"🔎 Unassociated contacts: {len(unassoc)}")

    linked = link_contacts_to_companies(contacts)
    print(f"✅ Linked {linked} contacts")

    return {
        "contacts_checked": len(contacts),
        "results_completed": True,
        "statusCode": 200,
        "body": json.dumps({
                "message": f"Processed  cards across  stores",
                "results_completed": True
            }),
        "linked": linked,
        "completed": True
    }


if __name__ == "__main__":
    lambda_handler()
