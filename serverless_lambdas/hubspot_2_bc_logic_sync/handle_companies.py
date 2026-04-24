from typing import Dict, Any
import traceback
import requests
from hubspot_helpers import get_company, _load_creds, invert_dict, load_json_file, _get_bc_token


CREDS = _load_creds()
TENANT_ID: str = CREDS["tenant_id"]
HUBSPOT_TOKEN: str = CREDS["HUBSPOT_TOKEN"]
HUBSPOT_API = "https://api.hubapi.com"
BC_ROOT = "https://api.businesscentral.dynamics.com/v2.0"
BC_V2_BASE = f"{BC_ROOT}/{TENANT_ID}/Production/api/v2.0"


# def handle_company(event: Dict[str, Any]) -> str:
#
#     try:
#         property_2_update = event.get("propertyName")
#         company_props_map = invert_dict(load_json_file("company_properties.json"))
#         bc_property_2_update = company_props_map.get(property_2_update)
#         bc_value_2_update = event.get("propertyValue")
#
#         properties_2_get = {
#             "bc_unique_id_2": "bc_unique_id_2",
#             property_2_update: property_2_update
#         }
#         company_data = get_company(event['objectId'], HUBSPOT_TOKEN, properties_2_get)
#         token = _get_bc_token(TENANT_ID, CREDS)
#         headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#         company_id = requests.get(f"{BC_V2_BASE}/companies", headers=headers).json()["value"][0]["id"]
#         # 1. Fetch the existing record to get its etag
#         # (entity could be customers, items, salesOrders, journalLines, etc.)
#         record_id = company_data["properties"]["bc_unique_id_2"]  # the GUID of the record you want to update
#         entity = "customers"
#         get_url = f"{BC_V2_BASE}/companies({company_id})/{entity}({record_id})"
#         record = requests.get(get_url, headers=headers).json()
#         etag = record["@odata.etag"]
#
#         # 2. PATCH with If-Match
#         patch_headers = {
#             **headers,
#             "If-Match": etag,  # use "*" to skip concurrency check, but etag is safer
#         }
#         # deal_props_map = invert_dict(load_json_file("deal_properties.json"))
#         # contact_props_map = invert_dict(load_json_file("contact_properties.json"))
#         payload = {
#             bc_property_2_update: bc_value_2_update,
#             # only include fields you want to change
#         }
#         resp = requests.patch(get_url, headers=patch_headers, json=payload)
#         resp.raise_for_status()
#         updated = resp.json()
#         return f"🔄 Company updated: {event['objectId']} with {payload}, {updated}"
#
#     except Exception as e:
#         print(f"❌ Exception in handle_company: {e}")
#         traceback.print_exc()
#         return f"❌ Exception in handle_company: {e}"

PROP_MISSING = "does not exist on type"  # substring in BC's 400 body when a field isn't on the entity


def handle_company(event: Dict[str, Any]) -> str:

    try:
        property_2_update = event.get("propertyName")
        company_props_map = invert_dict(load_json_file("company_properties.json"))
        bc_property_2_update = company_props_map.get(property_2_update)
        bc_value_2_update = event.get("propertyValue")
        bc_value_2_update = int(bc_value_2_update) if isinstance(bc_value_2_update, str) and bc_value_2_update.isdigit() else bc_value_2_update

        properties_2_get = {
            "bc_unique_id_2": "bc_unique_id_2",
            property_2_update: property_2_update,
        }
        company_data = get_company(event['objectId'], HUBSPOT_TOKEN, properties_2_get)

        token = _get_bc_token(TENANT_ID, CREDS)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # BC company identifiers — need both GUID (for v2) and name (for OData V4)
        bc_company   = requests.get(f"{BC_V2_BASE}/companies", headers=headers).json()["value"][0]
        company_id   = bc_company["id"]
        company_name = bc_company["name"]

        # 1. Fetch v2 customer — gives us the etag AND the "No." needed for the OData fallback
        record_id = company_data["properties"]["bc_unique_id_2"]
        entity    = "customers"
        get_url   = f"{BC_V2_BASE}/companies({company_id})/{entity}({record_id})"
        record    = requests.get(get_url, headers=headers).json()
        record_no = record.get("number")
        # get_url_2 = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE%20LONDON')/customerscardapi?$filter=No eq 'WEB-7098'"
        # get_url_2 = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE%20LONDON')/customerscardapi"
        # get_url_2 = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE%20LONDON')/test"
        # record_2   = requests.get(get_url_2, headers=headers, params={"$top": 1}).json()
        etag      = record["@odata.etag"]

        payload = {bc_property_2_update: bc_value_2_update}
        # --- Attempt 1: v2.0 REST (customers) ---
        patch_headers = {**headers, "If-Match": etag}

        odata_url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/api/openflow3/integration/v1.0/companies(d87c3c9f-8458-ee11-be6d-0022481d221d)/customerscrm1({record_id})"
        resp = requests.patch(odata_url, headers={**headers, "If-Match": "*"}, json=payload)

        if resp.ok:
            return f"🔄 Company updated via OData CustomerCard: {event['objectId']} with {payload}, {resp.json()}"

        if resp.status_code == 400 and PROP_MISSING in resp.text:
            return (f"❌ Property '{bc_property_2_update}' not found on v2 customer "
                    f"OR CustomerCard for {event['objectId']}")

        resp.raise_for_status()

    except Exception as e:
        print(f"❌ Exception in handle_company: {e}")
        traceback.print_exc()
        return f"❌ Exception in handle_company: {e}"

handle_company(

    {'eventId': 138695501, 'subscriptionId': 6282117, 'portalId': 244377491, 'appId': 30918371, 'occurredAt': 1776970149260, 'subscriptionType': 'company.propertyChange', 'attemptNumber': 0,
     'objectId': 296259049166, 'propertyName': 'responsibility_center', 'propertyValue': 'ECOMMERCE', 'changeSource': 'CRM_UI', 'sourceId': 'userId:52530071'}

)
