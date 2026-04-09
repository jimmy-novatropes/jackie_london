from mapping import map_deal
import os
import requests
from upsert_functions import prepare_companies_batch_payload,send_batch_upsert, prepare_deals_batch_payload
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
# from supporting_functions import get_page, get_bc_token
from typing import Dict, Any, Optional, List, Tuple
import json

HUBSPOT_API = "https://api.hubapi.com"

DEFAULT_MAP_PATH = "object_properties.json"
def load_jackielondon_creds() -> Dict[str, Any]:
    if is_running_in_lambda():
        return load_secrets("JACKIE_LONDON_KEYS")
    return load_secrets_locally("JACKIE_LONDON_KEYS")
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




CREDS = load_jackielondon_creds()
HUBSPOT_TOKEN: str = CREDS.get("HUBSPOT_TOKEN", "")
USERNAME: str = CREDS.get("GRAPHQL_USERNAME", "")
PASSWORD: str = CREDS.get("GRAPHQL_PASSWORD", "")

SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/companies/search"
def search_company_by_name(name: str, hubspot_token: str):
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "name",
                        "operator": "EQ",  # exact match; use CONTAINS_TOKEN for partial
                        "value": name,
                    }
                ]
            }
        ],
        "properties": ["name", "domain", "hs_object_id"],  # add more props if needed
        "limit": 5
    }

    resp = requests.post(
        SEARCH_URL,
        headers={
            "Authorization": f"Bearer {hubspot_token}",
            "Content-Type": "application/json",
        },
        json=payload,
    )

    if resp.status_code != 200:
        print(f"❌ Error: {resp.status_code} {resp.text}")
        return []

    results = resp.json().get("results", [])
    return [
        {
            "id": c.get("id"),
            "name": c["properties"].get("name"),
            "domain": c["properties"].get("domain"),
        }
        for c in results
    ]


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

def search_companies_by_names(names: list[str], hubspot_token: str):
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "dynamics_company_id",
                        "operator": "IN",   # multiple values
                        "values": names,    # pass the list here
                    }
                ]
            }
        ],
        "properties": ["dynamics_company_id", "domain", "hs_object_id"],
        "limit": len(names)  # adjust limit so you can get them all
    }

    resp = requests.post(
        SEARCH_URL,
        headers={
            "Authorization": f"Bearer {hubspot_token}",
            "Content-Type": "application/json",
        },
        json=payload,
    )

    if resp.status_code != 200:
        print(f"❌ Error: {resp.status_code} {resp.text}")
        return []

    results = resp.json().get("results", [])
    data_2_return = {}
    for result in results:
        data_2_return[result["properties"].get("dynamics_company_id")] = result.get("id")

    return data_2_return


# ------------------------ Helpers for Linking ------------------------
def hubspot_associate_deal_company(hs_token: str, deal_id: str, company_id: str):
    """Link a deal to a HubSpot company"""
    url = "https://api.hubapi.com/crm/v4/associations/deal/company/batch/create"
    headers = {"Authorization": f"Bearer {hs_token}", "Content-Type": "application/json"}
    payload = {
        "inputs": [
            {
                "from": {"id": deal_id},
                "to": {"id": company_id},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 341  # 1 = Deal ↔ Company
                    }
                ]
            }
        ]
    }
    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        print(f"❌ Association failed for deal {deal_id}: {resp.text}")

def upsert_line_items_batch(hs_token: str, inputs: List[Dict[str, Any]]):
    """Upsert (create or update) HubSpot line items in batches"""
    url = "https://api.hubapi.com/crm/v3/objects/line_items/batch/upsert"
    headers = {"Authorization": f"Bearer {hs_token}", "Content-Type": "application/json"}
    payload = {"inputs": inputs}

    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        print(f"❌ Line item upsert error: {resp.text}")
    else:
        print(f"✅ Upserted {len(inputs)} line items")
        data = resp.json()["results"]
        line_2_deal_mapping = {item["id"]: item["properties"].get("deal_hs_id") for item in data if item["properties"].get(
                "deal_hs_id")}
        return line_2_deal_mapping

def hubspot_batch_upsert_deals(hs_token, deals, unique_property):
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch/upsert"
    headers = {"Authorization": f"Bearer {hs_token}", "Content-Type": "application/json"}
    payload = {"inputs": [{"properties": d["properties"], "idProperty": unique_property, "id": d["properties"][
        "dynamics_deal_id"]} for d in
                          deals]}
    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        print("❌ HubSpot error:", resp.text)
    return resp.json()


def map_opportunity(bc_rec, field_map):
    props = {}
    for src, dest in field_map.items():
        if src in bc_rec and dest:
            props[dest] = bc_rec[src]
    return props
def get_page(
    url: str, headers: Dict[str, str], top: int
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value", []), data.get("@odata.nextLink")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    deal_owners = get_all_hubspot_users()
    batch_size = 500
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

    # company_id: Optional[str] = payload.get("company_id")
    #
    # # --- Resolve BC company ID ---
    # if not company_id:
    #     resp = requests.get(f"{base}/companies", headers=bc_headers, timeout=30)
    #     resp.raise_for_status()
    #     companies = resp.json().get("value", [])
    #     if not companies:
    #         return {"done": True, "processed": 0}
    #     company_id = companies[0]["id"]

    # After getting `companies` and `company_id`...
    company_id = company_account[0]["id"]
    # 4. Fetch all contacts in the company
    # companies_resp = requests.get(f"{base}/companies({company_id})/customers", headers=headers, timeout=30)
    # companies_resp.raise_for_status()
    # customers = companies_resp.json().get("value", [])

    next_link: Optional[str] = payload.get("next_link")
    if not next_link:
        next_link = f"{base}/companies({company_id})/salesInvoices"

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
        if len(all_invoices) >= 500:
            print("Reached 500 invoices, stopping fetch to avoid overload.")
            break
        # print(f"Fetched {len(invoices)} invoices, total so far: {len(all_invoices)}")

    if not all_invoices:
        print("No invoices found.")
        return {"company_id": company_id, "done": True, "processed": processed}

    print(f"✅ Retrieved {len(all_invoices)} total invoices from Business Central.")

    # --- Now gather all line items ---
    # def get_invoice_lines(invoice_id: str) -> List[Dict[str, Any]]:
    #     url = f"{base}/companies({company_id})/salesInvoices({invoice_id})/salesInvoiceLines"
    #     lines, _ = get_page(url, bc_headers, top=500)
    #     return lines or []

    # all_invoice_lines = {}
    # for inv in all_invoices:
    #     inv_id = inv.get("id")
    #     if not inv_id:
    #         continue
    #     lines = get_invoice_lines(inv_id)
    #     all_invoice_lines[inv_id] = lines
    #     print(f"🧾 Invoice {inv_id}: {len(lines)} lines")

    # print(f"✅ Retrieved all line items for {len(all_invoice_lines)} invoices")

    # --- Ready to create in HubSpot ---
    HEADERS = {"Authorization": f"Bearer {hs_token}"}
    owners = get_owners_dict(HEADERS)

    # companies_to_check = list({inv.get("customerId") for inv in all_invoices if inv.get("customerId")})
    # existing_companies = search_companies_by_names(companies_to_check, hs_token)

    # # --- Build deals payloads ---
    # deal_payloads: List[Dict[str, Any]] = []
    # for inv in all_invoices:
    #     props = map_opportunity(inv, field_map)
    #     props["import_source"] = RUN_TAG
    #     for k, v in static_props.items():
    #         if v:
    #             props[k] = v
    #     sales_rep = props.get("sales_person_code")
    #     if sales_rep and sales_rep in owners:
    #         props["hubspot_owner_id"] = owners[sales_rep]
    #     deal_payloads.append({"properties": props})
    #
    # # --- Upload all deals ---
    # print(f"🚀 Creating {len(deal_payloads)} deals in HubSpot...")
    # created_deals = []
    all_invoices = [map_deal(inv, field_map) for inv in all_invoices]

    deal_url, deal_payload = prepare_deals_batch_payload(all_invoices, primary_id_prop)
    response = send_batch_upsert(deal_url, deal_payload, hs_token, batch_size=25)


    # for i in range(0, len(deal_payloads), 25):
    #     chunk = deal_payloads[i:i+25]
    #     resp = hubspot_batch_upsert_deals(hs_token, chunk, primary_id_prop)
    #     if "results" in resp:
    #         created_deals.extend(resp["results"])
    #
    # print(f"✅ Created {len(created_deals)} deals in HubSpot.")

    # # --- Link deals to companies ---
    # for deal in created_deals:
    #     deal_props = deal.get("properties", {})
    #     company_name = deal_props.get("dynamics_customer_id")
    #     if company_name and company_name in existing_companies:
    #         hubspot_associate_deal_company(
    #             hs_token, deal["id"], existing_companies[company_name]
    #         )

    # --- Build and create line items ---
    print("🔄 Creating HubSpot line items...")
    line_item_payloads: List[Dict[str, Any]] = []

    # for deal in created_deals:
    #     deal_props = deal.get("properties", {})
    #     bc_invoice_id = deal_props.get(primary_id_prop)
    #     if not bc_invoice_id:
    #         continue
    #     lines = all_invoice_lines.get(bc_invoice_id, [])
    #     for line in lines:
    #         # Apply mapping from line_properties.json
    #         props = {}
    #         for bc_field, hs_field in line_map.items():
    #             props[hs_field] = line.get(bc_field)
    #         props["import_source"] = RUN_TAG
    #         props["dynamics_invoice_id"] = bc_invoice_id
    #         props["deal_hs_id"] = deal["id"]
    #
    #
    #         line_item_payloads.append({
    #             "id": line["id"],
    #             "idProperty": "dynamics_line_id",
    #             "properties": props
    #         })

    # for i in range(0, len(line_item_payloads), 100):
    #     chunk = line_item_payloads[i:i+100]
    #     line_2_deal = upsert_line_items_batch(hs_token, chunk)
    #     if line_2_deal:
    #         for line_id, deal_id in line_2_deal.items():
    #             hubspot_associate_line_2_deal(hs_token, line_id, deal_id)
    #
    # print(f"✅ Created {len(line_item_payloads)} line items in HubSpot.")
    # return {"company_id": company_id, "processed": len(all_invoices), "done": True}

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