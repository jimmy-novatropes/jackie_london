from mapping import map_contact
import os
import requests
from upsert_functions import prepare_contacts_batch_payload, send_batch_upsert
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
from supporting_functions import get_page, get_bc_token
from typing import Dict, Any, Optional
import json

HUBSPOT_API = "https://api.hubapi.com"


def load_jackielondon_creds() -> Dict[str, Any]:
    if is_running_in_lambda():
        return load_secrets("JACKIE_LONDON_KEYS")
    return load_secrets_locally("JACKIE_LONDON_KEYS")


CREDS = load_jackielondon_creds()
HUBSPOT_TOKEN: str = CREDS.get("HUBSPOT_TOKEN", "")
USERNAME: str = CREDS.get("GRAPHQL_USERNAME", "")
PASSWORD: str = CREDS.get("GRAPHQL_PASSWORD", "")


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


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    try:
        deal_owners = get_all_hubspot_users()
        batch_size = 500
        tenant_id = CREDS.get("tenant_id")
        client_id = CREDS.get("client_id")
        client_secret = CREDS.get("client_secret")
        api_base_url = CREDS.get("api_base_url")

        tenant_id = ""

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

        # After getting `companies` and `company_id`...
        company_id = company_account[0]["id"]
        from datetime import datetime, timedelta, timezone

        # example: last 24 hours
        since = (datetime.now(timezone.utc) - timedelta(days=6)).isoformat()

        params = {
            "$select": "*",
            "$filter": f"lastModifiedDateTime ge {since}"
        }

        contacts_resp = requests.get(f"{base}/companies({company_id})/contacts", headers=headers, timeout=30, params=params)
        contacts_resp.raise_for_status()
        contacts = contacts_resp.json().get("value", [])

        HEADERS = {"Authorization": f"Bearer {hs_token}"}

        bc_data = [map_contact(cust, deal_owners) for cust in contacts]
        #[id.strip() for id in ids if id and id.strip()] strip company name
        bc_data = [{**d, "company": d["company"].strip().title()} if "company" in d else d for d in bc_data]
        bc_data = [{**d, "firstname": d["firstname"].strip().title()} if "firstname" in d else d for d in bc_data]
        bc_data = [{**d, "lastname": d["lastname"].strip().title()} if "lastname" in d else d for d in bc_data]
        bc_data = [{**d, "lifecyclestage": "customer"} for d in bc_data]

        payload_url, payload = prepare_contacts_batch_payload(bc_data, unique_prop="bc_unique_id")

        send_batch_upsert(payload_url, payload, hs_token)

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
    except Exception as e:
        print("❌ Error in lambda_handler:", e)
        return {"error": str(e)}


if __name__ == "__main__":
    print(lambda_handler({}, None))