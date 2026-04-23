from mapping import map_deal, map_line_results
import os
import requests
from upsert_functions import send_batch_upsert, prepare_deals_batch_payload, prepare_line_items_batch_payload
from association_functions import prepare_line_item_deal_associations, send_batch_associations
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
# from supporting_functions import get_page, get_bc_token
from typing import Dict, Any, Optional, List, Tuple
import json

from datetime import date, timedelta
from urllib.parse import quote

def load_jackielondon_creds() -> Dict[str, Any]:
    if is_running_in_lambda():
        return load_secrets("JACKIE_LONDON_KEYS")
    return load_secrets_locally("JACKIE_LONDON_KEYS")


HUBSPOT_API = "https://api.hubapi.com"
SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/companies/search"
DEFAULT_MAP_PATH = "object_properties.json"

CREDS = load_jackielondon_creds()
HUBSPOT_TOKEN: str = CREDS.get("HUBSPOT_TOKEN", "")
USERNAME: str = CREDS.get("GRAPHQL_USERNAME", "")
PASSWORD: str = CREDS.get("GRAPHQL_PASSWORD", "")
DAYS = 365*3 # fetch records from the past 2 years; adjust as needed
batch_size = 500


def get_payload(event: dict) -> dict:
    return event.get("input", {}).get("lambdaResult", {}).get("Payload", {})

def load_mapping(path: str = DEFAULT_MAP_PATH) -> Dict[str, Any]:
    """
    Expected JSON:
    {
      "hubspot_unique_property": "email",
      "backup_unique_property": "epicor_contact_number",
      "field_map": {
        "id": "epicor_contact_number",
        "displayName": "fullname",
        "email": "email",
        "phoneNumber": "phone"
      },
      "static_properties": {
        "lifecycle_stage": "subscriber"
      }
    }
    """
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    data.setdefault("hubspot_unique_property", "email")
    data.setdefault("backup_unique_property", "epicor_contact_number")
    data.setdefault("field_map", {})
    data.setdefault("static_properties", {})
    return data


def get_all_hubspot_users():
    url = f"{HUBSPOT_API}/crm/v3/owners"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    users = []
    after = None

    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after

        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()

        for owner in data.get("results", []):
            users.append({
                "id": owner.get("id"),
                "email": owner.get("email"),
                "firstName": owner.get("firstName"),
                "lastName": owner.get("lastName"),
                "userId": owner.get("userId")  # HS internal user ID
            })

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return users

def hubspot_associate_line_2_deal(hs_token: str, line_id, deal_id: str):
    """Link a deal to a HubSpot company"""
    url = "https://api.hubapi.com/crm/v4/associations/line_item/deal/batch/create"
    headers = {"Authorization": f"Bearer {hs_token}", "Content-Type": "application/json"}
    payload = {
        "inputs": [
            {
                "from": {"id": line_id},
                "to": {"id": deal_id},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 20  # 1 = Deal ↔ Company
                    }
                ]
            }
        ]
    }
    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        print(f"❌ Association failed for deal {deal_id}: {resp.text}")

def get_owners_dict(headers) -> Dict[str, str]:
    """Fetch HubSpot owners as dict: Full Name -> ID"""
    url = "https://api.hubapi.com/crm/v3/owners/"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    owners = resp.json().get("results", [])
    return {
        f"{o.get('firstName','').strip()} {o.get('lastName','').strip()}".strip(): o["id"]
        for o in owners if o.get("id")
    }

def get_page(
    url: str, headers: Dict[str, str], top: int
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value", []), data.get("@odata.nextLink")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    deal_owners = get_all_hubspot_users()

    tenant_id = CREDS.get("tenant_id")
    client_id = CREDS.get("client_id")
    client_secret = CREDS.get("client_secret")
    api_base_url = CREDS.get("api_base_url")
    payload = get_payload(event)
    mapping = load_mapping()
    field_map = mapping["field_map"]
    # --- Load line item mapping ---
    with open("line_properties.json", "r") as f:
        line_map = json.load(f)

    tenant_id = ""
    static_props = mapping.get("static_properties", {})
    primary_id_prop = "sales_order_id"
    RUN_TAG = os.getenv("RUN_TAG", "Dynamics BC Opportunity Import")

    hs_token = CREDS.get("HUBSPOT_TOKEN")
    token_url = f"https://login.microsoftonline.com/{CREDS['tenant_id']}/oauth2/v2.0/token"

    # 2. Get access token
    resp = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": CREDS["client_id"],
        "client_secret": CREDS["client_secret"],
        "scope": "https://api.businesscentral.dynamics.com/.default",
    })
    resp.raise_for_status()
    token = resp.json()["access_token"]
    base = (
        api_base_url
        or (
            "https://api.businesscentral.dynamics.com/v2.0/"
            f"{tenant_id}/Production/api/v2.0"
        )
    )

    bc_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Prefer": f'odata.maxpagesize={batch_size}',
    }
    base = f"https://api.businesscentral.dynamics.com/v2.0/{CREDS['tenant_id']}/Production/api/v2.0"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    request_url = f"{base}/companies"
    # request_url = f"{base}/customers"

    company_account = requests.get(request_url, headers=headers).json()["value"]
    print("Companies:", company_account)

    company_id = company_account[0]["id"]

    # next_link: Optional[str] = payload.get("next_link")

    # Two years ago (approx — 365*2 days). For calendar-accurate, use dateutil.relativedelta.
    cutoff = (date.today() - timedelta(days=DAYS)).isoformat()
    # Or, calendar-accurate:
    # from dateutil.relativedelta import relativedelta
    # cutoff = (date.today() - relativedelta(years=2)).isoformat()

    filter_expr = f"invoiceDate ge {cutoff}"

    # if not next_link:
    #     next_link = f"{base}/companies({company_id})/salesInvoices"

    next_link: Optional[str] = payload.get("next_link")
    if not next_link:
        next_link = (
            f"{base}/companies({company_id})/salesInvoices"
            f"?$filter={quote(filter_expr)}"
            f"&$orderby=invoiceDate desc"
        )
        next_link = (
            f"{base}/companies({company_id})/salesInvoices"
            f"?$filter={quote(filter_expr)}"
            f"&$expand=salesInvoiceLines"
            f"&$orderby=invoiceDate desc"
        )

    processed: int = int(payload.get("processed", 0))
    print(f"Fetching ALL sales invoices for company {company_id}")

    # --- Fetch ALL invoices before processing ---
    all_invoices: List[Dict[str, Any]] = []
    while next_link:
        # contacts_resp = requests.get(next_link, headers=headers, timeout=30)
        invoices, next_link = get_page(next_link, bc_headers, top=batch_size)
        # if not invoices:
        #     break
        all_invoices.extend(invoices)
        # if len(all_invoices) >= 2500:
        #     print("Reached 500 invoices, stopping fetch to avoid overload.")
        #     break
        print(f"Fetched {len(invoices)} invoices, total so far: {len(all_invoices)}")

    if not all_invoices:
        print("No invoices found.")
        return {"company_id": company_id, "done": True, "processed": processed}

    print(f"✅ Retrieved {len(all_invoices)} total invoices from Business Central.")


    # --- Ready to create in HubSpot ---
    HEADERS = {"Authorization": f"Bearer {hs_token}"}
    owners = get_owners_dict(HEADERS)
    line_item_results = []
    for invoice in all_invoices:
        lines = invoice.get("salesInvoiceLines", [])
        line_item_results.extend(lines)
        # print()

    line_item_list = [map_line_results(company) for company in line_item_results]
    line_item_list = [d for d in line_item_list if d]

    all_invoices = [map_deal(inv, field_map) for inv in all_invoices]


    deal_url, deal_payload = prepare_deals_batch_payload(all_invoices, primary_id_prop)
    deal_ids = send_batch_upsert(deal_url, deal_payload, hs_token, batch_size=100)
    deal_results = []
    for deal_bundle in deal_ids:
        deal_results.extend(deal_bundle.get("results", []))

    url, payload = prepare_line_items_batch_payload(line_item_list, unique_prop="bc_line_id")
    line_item_ids = send_batch_upsert(url, payload, HUBSPOT_TOKEN, 100)
    line_results = []
    for line_bundle in line_item_ids:
        line_results.extend(line_bundle.get("results", []))
    print("Starting associations")
    if line_item_ids and deal_ids:
        assoc_url, assoc_payload = prepare_line_item_deal_associations(  # 🧠 link between objects
            deal_results,  # email → hs id
            line_results  # dealname → hs id
        )
        assoc_response = send_batch_associations(assoc_url, assoc_payload, HUBSPOT_TOKEN)
    # --- Build and create line items ---
    print("🔄 Creating HubSpot line items...")
    line_item_payloads: List[Dict[str, Any]] = []
    return {
        "company_id": company_id,
        "results_completed": True,
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Processed  cards across  stores",
            "results_completed": True
        })
        # "next_link": next_link,
        # "processed": processed,
        # "total_processed": processed + event.get("total_processed", 0),
        # "done": next_link is None,
    }


if __name__ == "__main__":
    print(lambda_handler({}, None))