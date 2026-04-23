import json
import requests
from typing import List, Dict, Tuple, Any
HUBSPOT_API = "https://api.hubapi.com"


# =========================================================
# 🔗 ASSOCIATION BATCH PREPARATION & SEND
# =========================================================

def prepare_associations_batch_payload(
    associations: List[Dict[str, Any]]
) -> Tuple[str, Dict]:
    """
    Prepares the payload and URL for batch-creating associations between HubSpot objects.

    Args:
        associations (list[dict]): Each association must have:
            {
              "from": {"id": "123", "type": "deal"},
              "to": {"id": "456", "type": "company"},
              "associationTypeId": 280  # see HubSpot association type IDs
            }

    Returns:
        tuple: (url, payload)
            - url (str): HubSpot associations batch endpoint
            - payload (dict): JSON body formatted for HubSpot API

    Example:
        associations = [
            {"from": {"id": "101", "type": "deals"}, "to": {"id": "501", "type": "companies"}, "associationTypeId": 280},
            {"from": {"id": "101", "type": "deals"}, "to": {"id": "601", "type": "contacts"}, "associationTypeId": 3}
        ]
        url, payload = prepare_associations_batch_payload(associations)
    """
    url = f"{HUBSPOT_API}/crm/v4/associations/batch/create"
    return url, {"inputs": associations}


def send_batch_associations(url: str, payload: Dict, hs_token: str, batch_size=100) -> Dict:
    """
    Sends a prepared batch of associations to HubSpot.

    Args:
        url (str): HubSpot associations endpoint (from prepare_associations_batch_payload)
        payload (dict): JSON payload
        hs_token (str): HubSpot private app token

    Returns:
        dict: HubSpot API response (success or error)
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {hs_token}",
    }

    inputs = payload.get("inputs", [])
    results = []
    # batch_size = 100
    # Chunk into 100
    for i in range(0, len(inputs), batch_size):
        batch = inputs[i:i + batch_size]
        batch_payload = {"inputs": batch}

        resp = requests.post(url, headers=headers, data=json.dumps(batch_payload))

        if not resp.ok:
            print(f"❌ Error {resp.status_code}: {resp.text}")

        try:
            results.append(resp.json())
        except Exception:
            results.append({"error": "Invalid JSON response", "raw": resp.text})

    return results


    # resp = requests.post(url, headers=headers, data=json.dumps(payload))
    # if not resp.ok:
    #     print(f"❌ Error {resp.status_code}: {resp.text}")
    # return resp.json()


def extract_contact_id_map(response: dict) -> dict:
    out = {}
    for r_hs in response:
        for r in r_hs.get("results", []):
            props = r.get("properties", {})
            email = props.get("email")
            hs_id = r.get("id")
            if email and hs_id:
                out[email] = hs_id
    return out


def extract_deal_id_map(response: dict) -> dict:
    out = {}
    for r_hs in response:
        for r in r_hs.get("results", []):
            props = r.get("properties", {})
            dealname = props.get("dealname")
            hs_id = r.get("id")
            if dealname and hs_id:
                out[dealname] = hs_id
    return out

def extract_line_item_id_map(response: dict) -> dict:
    out = {}
    for r_hs in response:
        for r in r_hs.get("results", []):
            props = r.get("properties", {})
            dealname = props.get("dealname")
            hs_id = r.get("id")
            if dealname and hs_id:
                out[dealname] = hs_id
    return out


def prepare_line_item_deal_associations(
    deal_ids: list,
    line_item_ids: list
):
    """
    Creates association payload between contact and deal objects in HubSpot.

    deal_map example:
    {12345: {"contact": {...}, "deal": {...}}}

    contact_results example:
    {"john@example.com": "101"}

    deal_results example:
    {"A-987654": "501"}
    """

    url = f"{HUBSPOT_API}/crm/v4/associations/line_items/deals/batch/create"
    inputs = []

    for deal in deal_ids:
        deal_id = deal.get("id")
        netsuite_transaction = deal.get("properties", {}).get("sales_order_id")
        for line_item in line_item_ids:
            line_iteid = line_item.get("id")
            netsuite_transaction_line = line_item.get("properties", {}).get("sales_order_id")

            if netsuite_transaction == netsuite_transaction_line:
                line_item_id = line_iteid
                inputs.append(
                    {
                        "from": {"id": line_item_id},
                        "to": {"id": deal_id},
                        "types": [
                            {
                                "associationCategory": "HUBSPOT_DEFINED",
                                "associationTypeId": 20
                            }
                        ],
                    })





        # c = obj.get("contact", {})
        # d = obj.get("deal", {})
        #
        # email = c.get("email")
        # dealname = d.get("dealname")
        #
        # if not email or not dealname:
        #     continue
        #
        # contact_id = contact_id_map.get(email)
        # deal_id = deal_id_map.get(dealname)
        #
        # if not contact_id or not deal_id:
        #     continue



    return url, {"inputs": inputs}
