from typing import Dict, Any
import requests
from hubspot_helpers import get_contact, _load_creds, invert_dict, load_json_file, _get_bc_token
import traceback


CREDS = _load_creds()
TENANT_ID: str = CREDS["tenant_id"]
HUBSPOT_TOKEN: str = CREDS["HUBSPOT_TOKEN"]
HUBSPOT_API = "https://api.hubapi.com"
BC_ROOT = "https://api.businesscentral.dynamics.com/v2.0"
BC_V2_BASE = f"{BC_ROOT}/{TENANT_ID}/Production/api/v2.0"


def handle_contact(event: Dict[str, Any]) -> str:
    try:

        property_2_update = event.get("propertyName")
        contact_props_map = invert_dict(load_json_file("contact_properties.json"))
        bc_property_2_update = contact_props_map.get(property_2_update)
        bc_value_2_update = event.get("propertyValue")

        properties_2_get = {
            "bc_unique_id": "bc_unique_id",
            property_2_update: property_2_update
        }
        contact_data = get_contact(event['objectId'], HUBSPOT_TOKEN, properties_2_get)
        token = _get_bc_token(TENANT_ID, CREDS)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        contact_id = requests.get(f"{BC_V2_BASE}/companies", headers=headers).json()["value"][0]["id"]
        record_id = contact_data["properties"]["bc_unique_id"]  # the GUID of the record you want to update
        entity = "contacts"
        get_url = f"{BC_V2_BASE}/companies({contact_id})/{entity}({record_id})"
        record = requests.get(get_url, headers=headers).json()
        etag = record["@odata.etag"]

        # 2. PATCH with If-Match
        patch_headers = {
            **headers,
            "If-Match": etag,  # use "*" to skip concurrency check, but etag is safer
        }
        payload = {
            bc_property_2_update: bc_value_2_update,
            # only include fields you want to change
        }
        resp = requests.patch(get_url, headers=patch_headers, json=payload)
        resp.raise_for_status()
        updated = resp.json()
        print(f"✅ Contact updated successfully: {updated}")
    except Exception as e:
        print(f"❌ Exception in handle_company: {e}")
        traceback.print_exc()
        return f"❌ Exception in handle_company: {e}"


handle_contact(
    {'eventId': 2758053767, 'subscriptionId': 6277452, 'portalId': 244377491, 'appId': 30918371, 'occurredAt': 1776694302920,
        'subscriptionType': 'contact.propertyChange', 'attemptNumber': 0, 'objectId': 296348360382,
        'propertyName': 'phone', 'propertyValue': '12346', 'changeSource': 'CRM_UI', 'sourceId': 'userId:52530071'}
               )
