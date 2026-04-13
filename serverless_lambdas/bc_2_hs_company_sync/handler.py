from mapping import map_company
import os
import requests
from upsert_functions import prepare_companies_batch_payload,send_batch_upsert
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
from supporting_functions import get_page, get_bc_token
from typing import Dict, Any, Optional
import math
from urllib.parse import quote
import random
import json

HUBSPOT_API = "https://api.hubapi.com"

def safe_get_value(response):
    """
    Returns (json_data, value_list)
    """
    try:
        data = response.json()
        return data.get("value", [])
    except Exception:
        print("Invalid JSON response:")
        print("Status:", response.status_code)
        print("Text:", response.text[:500])
        return []


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


from datetime import datetime
from statistics import mean

from datetime import datetime
from statistics import mean
from collections import defaultdict

# def calculate_collection_metrics(entries, credit_limit):
#
#     invoices = []
#     payments = []
#
#     for e in entries:
#         e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
#
#         if e["Document_Type"] == "Invoice":
#             invoices.append(e)
#
#         elif e["Document_Type"] == "Payment":
#             payments.append(e)
#
#     collection_days = []
#     late_days = []
#
#     for inv in invoices:
#         amount = inv["Amount"]
#         inv_date = inv["Posting_Date"]
#         due = datetime.strptime(inv["Due_Date"], "%Y-%m-%d")
#
#         # find matching payment
#         candidates = [
#             p for p in payments
#             if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date
#         ]
#
#         if not candidates:
#             continue
#
#         payment = min(candidates, key=lambda x: x["Posting_Date"])
#         pay_date = payment["Posting_Date"]
#
#         collection_days.append((pay_date - inv_date).days)
#
#         late = (pay_date - due).days
#         late_days.append(max(late, 0))
#
#     average_collection_days = mean(collection_days) if collection_days else 0
#     average_late_days = mean(late_days) if late_days else 0
#     return {
#         "average_collection_period": average_collection_days,
#         "average_late_payments": average_late_days
#     }
from datetime import datetime
from statistics import mean

def calculate_collection_metrics(entries, credit_limit=0):

    invoices = []
    payments = []

    for e in entries:
        e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
        e["Due_Date"] = datetime.strptime(e["Due_Date"], "%Y-%m-%d")

        if e["Document_Type"] == "Invoice":
            invoices.append(e)
        elif e["Document_Type"] == "Payment":
            payments.append(e)

    collection_days = []
    late_days = []

    total_sales = 0
    total_sales_fy = 0
    balance = 0

    current_year = datetime.utcnow().year

    for inv in invoices:
        amount = inv["Amount"]
        inv_date = inv["Posting_Date"]
        due = inv["Due_Date"]

        total_sales += amount

        if inv_date.year == current_year:
            total_sales_fy += amount

        balance += inv.get("Remaining_Amount", 0)

        candidates = [
            p for p in payments
            if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date
        ]

        if not candidates:
            continue

        payment = min(candidates, key=lambda x: x["Posting_Date"])
        pay_date = payment["Posting_Date"]

        collection_days.append((pay_date - inv_date).days)

        late = (pay_date - due).days
        late_days.append(max(late, 0))

    avg_collection = mean(collection_days) if collection_days else 0
    avg_collection = math.ceil(avg_collection)  # round up to nearest whole day
    avg_late = mean(late_days) if late_days else 0
    avg_late = math.ceil(avg_late)  # round up to nearest whole day

    usage_credit_limit = balance / credit_limit if credit_limit else 0

    return {
        # "balance": round(balance, 2),
        "total_sales": round(total_sales, 2),
        "total_sales_fiscal_year": round(total_sales_fy, 2),
        "average_collection_period": avg_collection,
        "average_late_payments": avg_late,
        "usage_of_credit_limit": round(usage_credit_limit, 2)
    }

    # return {
    #         # "total_sales": total_sales,
    #         # "total_sales_fiscal_year": total_sales_fy,
    #         "average_collection_period": avg_collection,
    #         "average_late_payments": avg_late,
    #         # "usage_of_credit_limit": usage_credit_limit,
    #         # "balance": balance
    #     }


# CRM extension
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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
    # 4. Fetch all contacts in the company

    customer_url = f"https://api.businesscentral.dynamics.com/v2.0/{CREDS['tenant_id']}/Production/ODataV4/Company('JACKIE LONDON')/Customer_Card/"
    # customer_url = f"https://api.businesscentral.dynamics.com/v2.0/{CREDS['tenant_id']}/Production/ODataV4/Customer_Card/"
    customer_url = "https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE LONDON')/Customer_Master"
    # customer_url = "https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/"
    # customer_url = "https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE LONDON')/Cust_ledgerEntries"
    # customer_url = "https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE LONDON')/Power_BI_Customer_List"
    # customer_url = "https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE LONDON')/powerbifinance"

    new_url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Feb_27_2026/api/openflow/integration/v1.0/companies({company_id})/customers"
    new_url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/JAN_04_2026/api/openflow/integration/v1.0/companies({company_id})/customers"
    new_url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/DEC_16_2025/api/openflow/integration/v1.0/companies({company_id})/customers"
    new_url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/api/openflow/integration/v1.0/companies({company_id})/customers"
    from datetime import datetime, timedelta, timezone

    # example: last 24 hours
    since = (datetime.now(timezone.utc) - timedelta(days=1200)).isoformat()
    params = {
        "$select": "*",
        "$filter": f"lastModifiedDateTime ge {since}"
    }
    companies_resp = requests.get(f"{base}/companies({company_id})/customers", headers=headers, params=params,timeout=30)
    dba_data = requests.get(new_url, headers=headers, timeout=30)
    dba_data_records = safe_get_value(dba_data)


    # dba_data_records = dba_data.json().get("value", [])
    companies_resp.raise_for_status()
    # customers_1 = companies_resp.json().get("value", [])
    customers_1 = safe_get_value(companies_resp)
    customers_2 = random.sample(customers_1, len(customers_1))
    customers = random.sample(customers_2, len(customers_2))
    # customers = customers_1[::-1]  # process oldest customers first
    companies = []
    for cust_ind, customer in enumerate(customers):
        customer_number = customer.get("number")
        customer_id = customer.get("id")

        result_dba = next((c for c in dba_data_records if c.get("id") == customer_id), None)

        url = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production/ODataV4/Company('JACKIE LONDON')/Cust_LedgerEntries?$filter=Customer_No eq '{customer_number}'"
        data = requests.get(url, headers=headers, timeout=30)

        #
        url_fields = f"https://api.businesscentral.dynamics.com/v2.0/55e10fec-4486-496b-842d-cc54c37e7d74/Production//ODataV4/Company('JACKIE LONDON')/Customer_Master?$filter=No eq '{customer_number}'"
        data_fields = requests.get(url_fields, headers=headers, timeout=30)
        #
        try:
            full_fields = safe_get_value(data_fields)[0]

        except Exception:
            full_fields = data_fields.json().get("value", [{}])[0]
        customer_merged = {**customer, **full_fields}
        customer_merged = {**customer_merged, **(result_dba or {})}
        # customer_merged = customer
        # customer_merged = dict(sorted(customer_merged.items()))
        #
        # transactions = data.json().get("value", [])
        transactions = safe_get_value(data)
        company_metrics = calculate_collection_metrics(transactions, credit_limit=customer.get("Credit_Limit", 0))
        hubspot_company_data = map_company(customer_merged, deal_owners)
        # hubspot_company_data = map_company(customer, deal_owners)
        merged = {**hubspot_company_data, **company_metrics}
        # merged = hubspot_company_data

        filter_str = f"'No.' IS '{customer_number}'"
        encoded_filter = quote(filter_str, safe="")

        merged["business_central_url"] = (
            "https://businesscentral.dynamics.com/"
            "55e10fec-4486-496b-842d-cc54c37e7d74/Production"
            "?company=JACKIE%20LONDON"
            "&page=21"
            f"&filter={encoded_filter}"
        )
        merged["customer_name"] = merged["customer_name"].strip().title() if merged.get("customer_name") else None
        merged["name"] = merged["name"].strip().title() if merged.get("name") else None
        merged["lifecyclestage"] = "customer"
        # merged[]
        # merged["business_central_url"] = "https://businesscentral.dynamics.com/55e10fec-4486-496b-842d-cc54c37e7d74/Production?company=JACKIE%20LONDON&page=22&filter="
        companies.append(merged)
        # if len(companies) >= 25 or cust_ind == len(customers) - 1:
        if len(companies) >= 200:
            HEADERS = {"Authorization": f"Bearer {hs_token}"}
            payload_url, payload = prepare_companies_batch_payload(companies, unique_prop="bc_unique_id_2")
            results = send_batch_upsert(payload_url, payload, hs_token)
            # reset companies list after sending batch
            companies = []
            print(f"Processed batch of {len(results)} companies. Last customer processed: {customer_number} (index {cust_ind + 1}/{len(customers)})")
    if companies:
        payload_url, payload = prepare_companies_batch_payload(companies, unique_prop="bc_unique_id_2")
        results = send_batch_upsert(payload_url, payload, hs_token)

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

    "Extension Settings Saved Customer CRM Integration Fields"
    "Adecuaciones Bc"