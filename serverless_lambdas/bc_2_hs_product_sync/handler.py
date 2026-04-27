from mapping import map_products
import requests
from upsert_functions import prepare_contacts_batch_payload, send_batch_upsert, prepare_products_batch_payload
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)

from typing import Dict, Any
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

from collections import defaultdict
from typing import Dict, Any, List


def fetch_products_with_variants(
    base: str,
    company_id: str,
    headers: Dict[str, str],
    include_items_without_variants: bool = True,
    variant_field_prefix: str = "variant_",
) -> List[Dict[str, Any]]:
    """
    Fetch all items and all item variants from Business Central, then return a
    flat list of one merged record per variant (or per item, if it has no
    variants and `include_items_without_variants=True`).

    Each record contains:
      - all item fields (preserved with original keys)
      - all variant fields, prefixed with `variant_field_prefix` to avoid
        clobbering overlapping keys (e.g. `id`, `number`, `description`)
      - convenience fields for HubSpot:
          bc_item_id, bc_variant_id, bc_unique_id, sku, name
    """

    def _paginate(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        page = 0
        next_url = url
        while next_url:
            page += 1
            resp = requests.get(
                next_url,
                headers=headers,
                params=params if page == 1 else None,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("value", [])
            results.extend(batch)
            print(f"  page {page}: {len(batch)} (running total: {len(results)})")
            next_url = data.get("@odata.nextLink")
        return results

    # 1. Fetch all items
    print("Fetching items...")
    items = _paginate(f"{base}/companies({company_id})/items", {"$select": "*"})
    print(f"Total items: {len(items)}")

    # 2. Fetch all item variants
    print("Fetching item variants...")
    variants = _paginate(f"{base}/companies({company_id})/itemVariants", {"$select": "*"})
    print(f"Total variants: {len(variants)}")

    # 3. Group variants by their parent itemId
    variants_by_item: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for v in variants:
        item_id = v.get("itemId")
        if item_id:
            variants_by_item[item_id].append(v)

    # 4. Flatten: one record per variant (or per item if it has none)
    flattened: List[Dict[str, Any]] = []
    for item in items:
        item_variants = variants_by_item.get(item.get("id"), [])
        item_number = item.get("number") or ""
        item_name = item.get("displayName") or item.get("description") or ""

        if not item_variants:
            if not include_items_without_variants:
                continue
            merged = dict(item)
            merged["bc_item_id"] = item.get("id")
            merged["bc_variant_id"] = None
            merged["bc_unique_id"] = item_number
            merged["sku"] = item_number
            merged["name"] = item_name
            flattened.append(merged)
            continue

        for variant in item_variants:
            merged = dict(item)
            for k, val in variant.items():
                merged[f"{variant_field_prefix}{k}"] = val

            variant_code = variant.get("code") or ""
            variant_desc = variant.get("description") or ""

            merged["bc_item_id"] = item.get("id")
            merged["bc_variant_id"] = variant.get("id")
            merged["bc_unique_id"] = f"{item_number}::{variant_code}" if variant_code else item_number
            merged["sku"] = merged["bc_unique_id"]
            merged["name"] = f"{item_name} - {variant_desc}".strip(" -") if variant_desc else item_name

            flattened.append(merged)

    print(f"Flattened item+variant records: {len(flattened)}")
    return flattened


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:

    try:
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
        since = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
        # until = (datetime.now(timezone.utc) - timedelta(days=600)).isoformat()

        # days_since = 400
        # days_range = 15
        # days_until = days_since - days_range

        #
        # while days_until > 0:
        #     since = (datetime.now(timezone.utc) - timedelta(days=days_since)).isoformat()
        #     until = (datetime.now(timezone.utc) - timedelta(days=days_until)).isoformat()


        params = {
            "$select": "*",
            "$filter": f"lastModifiedDateTime ge {since}"
        }

        # params = {
        #     "$select": "*",
        #     "$filter": f"lastModifiedDateTime ge {since} and lastModifiedDateTime le {until}"
        # }

        # contacts_resp = requests.get(f"{base}/companies({company_id})/items", headers=headers, timeout=30, params=params)
        # contacts_resp = requests.get(f"{base}/companies({company_id})/itemVariants", headers=headers, timeout=30, params=params)
        # contacts_resp.raise_for_status()

        products = fetch_products_with_variants(base, company_id, headers)


        # contacts = contacts_resp.json().get("value", [])
        print("Contacts:", len(products))

        HEADERS = {"Authorization": f"Bearer {hs_token}"}

        bc_data = [map_products(cust) for cust in products]
        print(len(bc_data), "mapped products")


        payload_url, payload = prepare_products_batch_payload(bc_data, unique_prop="hs_sku")

        send_batch_upsert(payload_url, payload, hs_token, batch_size=100)
        # days_since -= days_range
        # days_until -= days_range

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