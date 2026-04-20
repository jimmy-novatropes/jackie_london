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


def handle_company(event: Dict[str, Any]) -> str:

    try:
        property_2_update = event.get("propertyName")
        company_props_map = invert_dict(load_json_file("company_properties.json"))
        bc_property_2_update = company_props_map.get(property_2_update)
        bc_value_2_update = event.get("propertyValue")

        properties_2_get = {
            "bc_unique_id_2": "bc_unique_id_2",
            property_2_update: property_2_update
        }
        company_data = get_company(event['objectId'], HUBSPOT_TOKEN, properties_2_get)
        token = _get_bc_token(TENANT_ID, CREDS)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        company_id = requests.get(f"{BC_V2_BASE}/companies", headers=headers).json()["value"][0]["id"]
        # 1. Fetch the existing record to get its etag
        # (entity could be customers, items, salesOrders, journalLines, etc.)
        record_id = company_data["properties"]["bc_unique_id_2"]  # the GUID of the record you want to update
        entity = "customers"
        get_url = f"{BC_V2_BASE}/companies({company_id})/{entity}({record_id})"
        record = requests.get(get_url, headers=headers).json()
        etag = record["@odata.etag"]

        # 2. PATCH with If-Match
        patch_headers = {
            **headers,
            "If-Match": etag,  # use "*" to skip concurrency check, but etag is safer
        }
        # deal_props_map = invert_dict(load_json_file("deal_properties.json"))
        # contact_props_map = invert_dict(load_json_file("contact_properties.json"))
        payload = {
            bc_property_2_update: bc_value_2_update,
            # only include fields you want to change
        }
        resp = requests.patch(get_url, headers=patch_headers, json=payload)
        resp.raise_for_status()
        updated = resp.json()
        return f"🔄 Company updated: {event['objectId']} with {payload}, {updated}"

    except Exception as e:
        print(f"❌ Exception in handle_company: {e}")
        traceback.print_exc()
        return f"❌ Exception in handle_company: {e}"


handle_company(
    {'eventId': 2758053767, 'subscriptionId': 6277452, 'portalId': 244377491, 'appId': 30918371, 'occurredAt': 1776694302920,
        'subscriptionType': 'company.propertyChange', 'attemptNumber': 0, 'objectId': 296348360382,
        'propertyName': 'phone', 'propertyValue': '12346', 'changeSource': 'CRM_UI', 'sourceId': 'userId:52530071'}
)
