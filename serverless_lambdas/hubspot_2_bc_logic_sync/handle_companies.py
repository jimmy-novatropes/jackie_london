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


PROP_MISSING = "does not exist on type"  # substring in BC's 400 body when a field isn't on the entity
from datetime import datetime

def timestamp_to_bc_date(timestamp_ms):
    """Convert millisecond timestamp to ISO date string"""
    return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')


def handle_company(event: Dict[str, Any]) -> str:

    try:
        property_2_update = event.get("propertyName")
        company_props_map = invert_dict(load_json_file("company_properties.json"))
        bc_property_2_update = company_props_map.get(property_2_update)
        bc_value_2_update = event.get("propertyValue")

        if property_2_update in ["createdate", "lastmodifieddate", "out_of_business_date", "startdate", "end_date"]:

            if isinstance(bc_value_2_update, str) and bc_value_2_update.isdigit():
                bc_value_2_update = timestamp_to_bc_date(int(bc_value_2_update))
            else:
                bc_value_2_update = None
        elif property_2_update not in ["zip_code", "address2", "address"]:
            if "," in bc_value_2_update:
                bc_value_2_update = bc_value_2_update.replace(",", "")
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

        etag      = record["@odata.etag"]

        payload = {bc_property_2_update: bc_value_2_update}
        # --- Attempt 1: v2.0 REST (customers) ---
        patch_headers = {**headers, "If-Match": etag}

        odata_url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/api/openflow3/integration/v1.0/companies(d87c3c9f-8458-ee11-be6d-0022481d221d)/customerscrm1({record_id})"
        resp = requests.patch(odata_url, headers={**headers, "If-Match": "*"}, json=payload)

        if resp.ok:
            return f"🔄 Company updated via OData CustomerCard: {event['objectId']} with {payload}, {resp.json()}"

        else:
            print(resp.status_code, resp.text)

        if resp.status_code == 400 and PROP_MISSING in resp.text:
            return (f"❌ Property '{bc_property_2_update}' not found on v2 customer "
                    f"OR CustomerCard for {event['objectId']}")

        resp.raise_for_status()

    except Exception as e:
        print(f"❌ Exception in handle_company: {e}")
        traceback.print_exc()
        return f"❌ Exception in handle_company: {e}"

handle_company(

    {'eventId': 1457647294, 'subscriptionId': 6282131, 'portalId': 244377491, 'appId': 30918371, 'occurredAt': 1777031874024, 'subscriptionType': 'company.propertyChange',
     'attemptNumber': 0, 'objectId': 296298940107, 'propertyName': 'tiktok', 'propertyValue': 'test1', 'changeSource': 'CRM_UI', 'sourceId': 'userId:52530071'}
)
