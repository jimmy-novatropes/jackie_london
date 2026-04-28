import json
import math
import random
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict
from urllib.parse import quote

import requests

from credentials_load import is_running_in_lambda, load_secrets, load_secrets_locally
from mapping import map_company
from upsert_functions import prepare_companies_batch_payload, send_batch_upsert

# ── Constants ──────────────────────────────────────────────────────────────────
HUBSPOT_API = "https://api.hubapi.com"
BC_ROOT = "https://api.businesscentral.dynamics.com/v2.0"
COMPANY_NAME = "JACKIE LONDON"
BATCH_SIZE = 50
PAST_DAYS = 5
DAY_RANGE = 5

# ── Credentials ────────────────────────────────────────────────────────────────
def _load_creds() -> Dict[str, Any]:
    loader = load_secrets if is_running_in_lambda() else load_secrets_locally
    return loader("JACKIE_LONDON_KEYS")

CREDS = _load_creds()
TENANT_ID: str = CREDS["tenant_id"]
HUBSPOT_TOKEN: str = CREDS["HUBSPOT_TOKEN"]

# ── BC URL bases (built once, reused everywhere) ───────────────────────────────
BC_V2_BASE    = f"{BC_ROOT}/{TENANT_ID}/Production/api/v2.0"
BC_ODATA_BASE = f"{BC_ROOT}/{TENANT_ID}/Production/ODataV4/Company('{COMPANY_NAME}')"
BC_CUSTOM_BASE = f"{BC_ROOT}/{TENANT_ID}/Production/api/openflow3/integration/v1.0"


# ── Helpers ────────────────────────────────────────────────────────────────────
def safe_get_value(response) -> list:
    try:
        return response.json().get("value", [])
    except Exception:
        print(f"Bad JSON — status {response.status_code}: {response.text[:500]}")
        return []


def _get_bc_token() -> str:
    """Obtain a Business Central OAuth2 access token."""
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CREDS["client_id"],
            "client_secret": CREDS["client_secret"],
            "scope": "https://api.businesscentral.dynamics.com/.default",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_all_hubspot_users() -> list:
    url = f"{HUBSPOT_API}/crm/v3/owners"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    users, after = [], None

    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        for owner in data.get("results", []):
            users.append({k: owner.get(k) for k in ("id", "email", "firstName", "lastName", "userId")})
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return users


# def calculate_collection_metrics(entries: list, credit_limit: float = 0) -> Dict[str, Any]:
#     invoices, payments = [], []
#     for e in entries:
#         e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
#         e["Due_Date"]     = datetime.strptime(e["Due_Date"],     "%Y-%m-%d")
#         if e["Document_Type"] == "Invoice":
#             invoices.append(e)
#         elif e["Document_Type"] == "Payment":
#             payments.append(e)
#
#     collection_days, late_days = [], []
#     total_sales = total_sales_fy = balance = 0.0
#     current_year = datetime.utcnow().year
#
#     for inv in invoices:
#         amount, inv_date, due = inv["Amount"], inv["Posting_Date"], inv["Due_Date"]
#         total_sales += amount
#         if inv_date.year == current_year:
#             total_sales_fy += amount
#         balance += inv.get("Remaining_Amount", 0)
#
#         # Find the earliest matching payment (avoids building an intermediate list)
#         match = min(
#             (p for p in payments if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date),
#             key=lambda p: p["Posting_Date"],
#             default=None,
#         )
#         if match:
#             pay_date = match["Posting_Date"]
#             collection_days.append((pay_date - inv_date).days)
#             late_days.append(max((pay_date - due).days, 0))
#
#     return {
#         "total_sales":              round(total_sales, 2),
#         "total_sales_fiscal_year":  round(total_sales_fy, 2),
#         "average_collection_period": math.ceil(mean(collection_days)) if collection_days else 0,
#         "average_late_payments":     math.ceil(mean(late_days))       if late_days       else 0,
#         "usage_of_credit_limit":    round(balance / credit_limit, 2)  if credit_limit    else 0,
#     }


# def calculate_collection_metrics(
#     entries: list,
#     credit_limit: float = 0,
#     fiscal_year_start: datetime = None,
# ) -> Dict[str, Any]:
#
#     # Default fiscal year start to Jan 1 of current year if not provided
#     if fiscal_year_start is None:
#         fiscal_year_start = datetime(datetime.utcnow().year, 1, 1)
#
#     invoices, payments = [], []
#     for e in entries:
#         e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
#         e["Due_Date"]     = datetime.strptime(e["Due_Date"],     "%Y-%m-%d")
#         if e["Document_Type"] == "Invoice":
#             invoices.append(e)
#         elif e["Document_Type"] == "Payment":
#             payments.append(e)
#
#     # Build a lookup from Document_No -> list of payments (handles partial/split payments)
#     payment_lookup: Dict[str, list] = {}
#     for p in payments:
#         doc_no = p.get("Applies_to_Doc_No", "")
#         if doc_no:
#             payment_lookup.setdefault(doc_no, []).append(p)
#
#     # Payments this year: sum all payment entries in the current fiscal year
#     payments_fy = sum(
#         abs(p["Amount"])
#         for p in payments
#         if p["Posting_Date"] >= fiscal_year_start
#     )
#
#     collection_days, late_days = [], []
#     total_sales = total_sales_fy = balance = 0.0
#
#     for inv in invoices:
#         amount   = inv["Amount"]
#         inv_date = inv["Posting_Date"]
#         due      = inv["Due_Date"]
#         doc_no   = inv.get("Document_No", "")
#
#         total_sales += amount
#         if inv_date >= fiscal_year_start:
#             total_sales_fy += amount
#         balance += inv.get("Remaining_Amount", 0)
#
#         # Match via Applies_to_Doc_No; fall back to amount+date if field is absent
#         matched_payments = payment_lookup.get(doc_no) or [
#             p for p in payments
#             if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date
#         ]
#
#         if matched_payments:
#             # Use the earliest payment date (first application)
#             pay_date = min(p["Posting_Date"] for p in matched_payments)
#             collection_days.append((pay_date - inv_date).days)
#             late_days.append(max((pay_date - due).days, 0))
#
#     return {
#         "total_sales":               round(total_sales, 2),
#         "total_sales_fiscal_year":   round(total_sales_fy, 2),
#         "payments_this_year":        round(payments_fy, 2),
#         "average_collection_period": math.ceil(mean(collection_days)) if collection_days else 0,
#         "average_late_payments":     math.ceil(mean(late_days))       if late_days       else 0,
#         "usage_of_credit_limit":     round((balance / credit_limit) * 100, 2) if credit_limit else 0,
#     }


# def calculate_collection_metrics(
#     entries: list,
#     credit_limit: float = 0,
#     fiscal_year_start: datetime = None,
# ) -> Dict[str, Any]:
#
#     # Default fiscal year start to Jan 1 of current year if not provided
#     if fiscal_year_start is None:
#         fiscal_year_start = datetime(datetime.utcnow().year, 1, 1)
#
#     invoices, payments = [], []
#     for e in entries:
#         e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
#         e["Due_Date"]     = datetime.strptime(e["Due_Date"],     "%Y-%m-%d")
#         if e["Document_Type"] == "Invoice":
#             invoices.append(e)
#         elif e["Document_Type"] == "Payment":
#             payments.append(e)
#         else:
#             print(e)
#
#     # Build a lookup from Document_No -> list of payments (handles partial/split payments)
#     payment_lookup: Dict[str, list] = {}
#     for p in payments:
#         doc_no = p.get("Document_No", "")
#         if doc_no:
#             payment_lookup.setdefault(doc_no, []).append(p)
#
#     # Payments this year: sum all payment entries in the current fiscal year
#     payments_fy = sum(
#         abs(p["Amount"])
#         for p in payments
#         if p["Posting_Date"] >= fiscal_year_start
#     )
#
#     collection_days, late_days = [], []
#     total_sales = total_sales_fy = balance = overdue_amount = 0.0
#     today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
#
#     for inv in invoices:
#         amount    = inv["Amount"]
#         inv_date  = inv["Posting_Date"]
#         due       = inv["Due_Date"]
#         doc_no    = inv.get("Document_No", "")
#         remaining = inv.get("Remaining_Amount", 0)
#
#         total_sales += amount
#         if inv_date >= fiscal_year_start:
#             total_sales_fy += amount
#         balance += remaining
#         print(balance)
#
#         # Overdue: still has an open balance and due date has passed
#         if remaining > 0 and due < today:
#             overdue_amount += remaining
#
#         # Match via Applies_to_Doc_No; fall back to amount+date if field is absent
#         matched_payments = payment_lookup.get(doc_no) or [
#             p for p in payments
#             if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date
#         ]
#
#         if matched_payments:
#             # Use the earliest payment date (first application)
#             pay_date = min(p["Posting_Date"] for p in matched_payments)
#             collection_days.append((pay_date - inv_date).days)
#             late_days.append(max((pay_date - due).days, 0))
#
#     return {
#         "total_sales":               round(total_sales, 2),
#         "total_sales_fiscal_year":   round(total_sales_fy, 2),
#         "payments_this_year":        round(payments_fy, 2),
#         "overdue_payment":          round(overdue_amount, 2),
#         "average_collection_period": math.ceil(mean(collection_days)) if collection_days else 0,
#         "average_late_payments":     math.ceil(mean(late_days))       if late_days       else 0,
#         "usage_of_credit_limit":     round((balance / credit_limit) * 100, 2) if credit_limit else 0,
#     }

# def calculate_collection_metrics(
#     entries: list,
#     credit_limit: float = 0,
#     fiscal_year_start: datetime = None,
#     cap_credit_limit: bool = True,
# ) -> Dict[str, Any]:
#
#     if fiscal_year_start is None:
#         fiscal_year_start = datetime(datetime.utcnow().year, 1, 1)
#
#     today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
#
#     invoices, payments = [], []
#     for e in entries:
#         e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d") if isinstance(e["Posting_Date"], str) else e["Posting_Date"]
#         e["Due_Date"]     = datetime.strptime(e["Due_Date"],     "%Y-%m-%d") if isinstance(e["Due_Date"], str) else e["Due_Date"]
#
#         raw_closed = e.get("Closed_at_Date")
#         if isinstance(raw_closed, str) and raw_closed and not raw_closed.startswith("0001"):
#             e["Closed_at_Date"] = datetime.strptime(raw_closed, "%Y-%m-%d")
#         elif not isinstance(raw_closed, datetime):
#             e["Closed_at_Date"] = None
#
#         if e["Document_Type"] == "Invoice":
#             invoices.append(e)
#         elif e["Document_Type"] == "Payment":
#             payments.append(e)
#
#     # Build amount -> list of payments lookup for fallback matching
#     # Key: rounded amount, Value: list of payment entries sorted by date
#     from collections import defaultdict
#     payment_by_amount: dict = defaultdict(list)
#     for p in payments:
#         key = round(abs(p["Amount"]), 2)
#         payment_by_amount[key].append(p)
#     for lst in payment_by_amount.values():
#         lst.sort(key=lambda p: p["Posting_Date"])
#
#     # Track which payments have been "used" to avoid double-counting
#     used_payment_entries: set = set()
#
#     payments_fy = sum(
#         abs(p["Amount"])
#         for p in payments
#         if p["Posting_Date"] >= fiscal_year_start
#     )
#
#     collection_days, late_days = [], []
#     collection_days_1, late_days_1 = [], []  # for fallback matching strategy
#     total_sales = total_sales_fy = balance = overdue_amount = 0.0
#     overdue_count = 0
#
#     for inv in invoices:
#         amount    = inv["Amount"]
#         inv_date  = inv["Posting_Date"]
#         due       = inv["Due_Date"]
#         remaining = inv.get("Remaining_Amount", 0)
#         is_open   = inv.get("Open", remaining > 0)
#         closed_at = inv.get("Closed_at_Date")
#
#         total_sales += amount
#         if inv_date >= fiscal_year_start:
#             total_sales_fy += amount
#         balance += remaining
#
#         if is_open and remaining > 0 and due < today:
#             overdue_amount += remaining
#             overdue_count  += 1
#
#         pay_date = None
#
#         # Strategy 1: use Closed_at_Date if available
#         if not is_open and closed_at:
#             pay_date = closed_at
#
#         # Strategy 2: find a payment with exact amount match posted on or after invoice date
#         elif not is_open and amount > 0:
#             key = round(amount, 2)
#             candidates = [
#                 p for p in payment_by_amount.get(key, [])
#                 if p["Posting_Date"] >= inv_date
#                 and id(p) not in used_payment_entries
#             ]
#             if candidates:
#                 match = candidates[0]  # earliest matching payment
#                 used_payment_entries.add(id(match))
#                 pay_date = match["Posting_Date"]
#
#         if pay_date:
#             collection_days.append((pay_date - inv_date).days)
#             late_days.append(max((pay_date - due).days, 0))
#
#         # Find the earliest matching payment (avoids building an intermediate list)
#         match = min(
#             (p for p in payments if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date),
#             key=lambda p: p["Posting_Date"],
#             default=None,
#         )
#         if match:
#             pay_date = match["Posting_Date"]
#             collection_days_1.append((pay_date - inv_date).days)
#             late_days_1.append(max((pay_date - due).days, 0))
#
#     raw_credit_usage = round((balance / credit_limit) * 100, 2) if credit_limit else 0
#     credit_usage = min(raw_credit_usage, 100.0) if cap_credit_limit else raw_credit_usage
#
#
#     return {
#         "total_sales":               round(total_sales, 2),
#         "total_sales_fiscal_year":   round(total_sales_fy, 2),
#         "payments_this_year":        round(payments_fy, 2),
#         "overdue_payments":          round(overdue_amount, 2),
#         "overdue_payments_count":    overdue_count,
#         "average_collection_period": round(mean(collection_days), 1) if collection_days else 0,
#         "average_late_payments":     math.ceil(mean(late_days))      if late_days       else 0,
#         "usage_of_credit_limit":     credit_usage,
#     }


# def calculate_collection_metrics(
#     entries: list,
#     credit_limit: float = 0,
#     fiscal_year_start: datetime = None,
#     cap_credit_limit: bool = True,
# ) -> Dict[str, Any]:
#
#     if fiscal_year_start is None:
#         fiscal_year_start = datetime(datetime.utcnow().year, 1, 1)
#
#     today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
#
#     invoices, payments = [], []
#     for e in entries:
#         e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
#         e["Due_Date"]     = datetime.strptime(e["Due_Date"],     "%Y-%m-%d")
#
#         # Parse Closed_at_Date only if present and non-empty
#         raw_closed = e.get("Closed_at_Date", "")
#         e["Closed_at_Date"] = (
#             datetime.strptime(raw_closed, "%Y-%m-%d") if raw_closed else None
#         )
#
#         if e["Document_Type"] == "Invoice":
#             invoices.append(e)
#         elif e["Document_Type"] == "Payment":
#             payments.append(e)
#
#     # Payments this year: sum all payment entries in the current fiscal year
#     payments_fy = sum(
#         abs(p["Amount"])
#         for p in payments
#         if p["Posting_Date"] >= fiscal_year_start
#     )
#
#     collection_days, late_days = [], []
#     collection_days_1, late_days_1 = [], []  # for fallback matching strategy
#     total_sales = total_sales_fy = balance = overdue_amount = 0.0
#     overdue_count = 0
#
#     for inv in invoices:
#         amount    = inv["Amount"]
#         inv_date  = inv["Posting_Date"]
#         due       = inv["Due_Date"]
#         remaining = inv.get("Remaining_Amount", 0)
#         is_open   = inv.get("Open", remaining > 0)
#         closed_at = inv.get("Closed_at_Date")  # populated by BC when invoice is fully paid
#
#         total_sales += amount
#         if inv_date >= fiscal_year_start:
#             total_sales_fy += amount
#         balance += remaining
#
#         # Overdue: open balance past due date
#         if is_open and remaining > 0 and due < today:
#             overdue_amount += remaining
#             overdue_count  += 1
#
#         # Use Closed_at_Date for paid invoices — much more reliable than matching payments
#         if not is_open and closed_at:
#             collection_days.append((closed_at - inv_date).days)
#             late_days.append(max((closed_at - due).days, 0))
#         # Find the earliest matching payment (avoids building an intermediate list)
#         match = min(
#             (p for p in payments if abs(p["Amount"]) == amount and p["Posting_Date"] >= inv_date),
#             key=lambda p: p["Posting_Date"],
#             default=None,
#         )
#         if match:
#             pay_date = match["Posting_Date"]
#             collection_days_1.append((pay_date - inv_date).days)
#             late_days_1.append(max((pay_date - due).days, 0))
#
#     raw_credit_usage = round((balance / credit_limit) * 100, 2) if credit_limit else 0
#     credit_usage = min(raw_credit_usage, 100.0) if cap_credit_limit else raw_credit_usage
#     test = math.ceil(mean(collection_days_1))
#
#     return {
#         "total_sales":               round(total_sales, 2),
#         "total_sales_fiscal_year":   round(total_sales_fy, 2),
#         "payments_this_year":        round(payments_fy, 2),
#         "overdue_payments":          round(overdue_amount, 2),
#         "overdue_payments_count":    overdue_count,
#         "average_collection_period": round(mean(collection_days), 1) if collection_days else 0,
#         "average_late_payments":     math.ceil(mean(late_days))      if late_days       else 0,
#         "usage_of_credit_limit":     credit_usage,
#     }

import math
from collections import defaultdict
from datetime import datetime
from statistics import mean
from typing import Any, Dict


def calculate_collection_metrics(
    entries: list,
    credit_limit: float = 0,
    fiscal_year_start: datetime = None,
    cap_credit_limit: bool = True,
) -> Dict[str, Any]:

    if fiscal_year_start is None:
        fiscal_year_start = datetime(datetime.utcnow().year, 1, 1)

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    invoices, payments = [], []
    for e in entries:
        e["Posting_Date"] = datetime.strptime(e["Posting_Date"], "%Y-%m-%d")
        e["Due_Date"]     = datetime.strptime(e["Due_Date"],     "%Y-%m-%d")
        if e["Document_Type"] == "Invoice":
            invoices.append(e)
        elif e["Document_Type"] == "Payment":
            payments.append(e)

    # Index payments by amount for O(1) lookup; sort each bucket by date (earliest first)
    payment_by_amount: dict = defaultdict(list)
    for p in payments:
        key = round(abs(p["Amount"]), 2)
        payment_by_amount[key].append(p)
    for lst in payment_by_amount.values():
        lst.sort(key=lambda p: p["Posting_Date"])

    payments_fy = sum(
        abs(p["Amount"])
        for p in payments
        if p["Posting_Date"] >= fiscal_year_start
    )

    collection_days, late_days = [], []
    total_sales = total_sales_fy = balance = overdue_amount = 0.0
    overdue_count = 0
    used_payment_entries: set = set()

    for inv in invoices:
        amount    = inv["Amount"]
        inv_date  = inv["Posting_Date"]
        due       = inv["Due_Date"]
        remaining = inv.get("Remaining_Amount", 0)
        is_open   = inv.get("Open", remaining > 0)

        total_sales += amount
        if inv_date >= fiscal_year_start:
            total_sales_fy += amount
        balance += remaining

        if is_open and remaining > 0 and due < today:
            overdue_amount += remaining
            overdue_count  += 1

        # Only try to match closed (paid) invoices with positive amounts
        if not is_open and amount > 0:
            key = round(amount, 2)
            candidates = [
                p for p in payment_by_amount.get(key, [])
                if p["Posting_Date"] >= inv_date
                and id(p) not in used_payment_entries
            ]
            if candidates:
                match = candidates[0]  # earliest qualifying payment
                used_payment_entries.add(id(match))
                pay_date = match["Posting_Date"]
                collection_days.append((pay_date - inv_date).days)
                late_days.append(max((pay_date - due).days, 0))

    raw_credit_usage = round((balance / credit_limit) * 100, 2) if credit_limit else 0
    credit_usage = min(raw_credit_usage, 100.0) if cap_credit_limit else raw_credit_usage

    return {
        "total_sales":               round(total_sales, 2),
        "total_sales_fiscal_year":   round(total_sales_fy, 2),
        "payments_this_year":        round(payments_fy, 2),
        "overdue_payments":          round(overdue_amount, 2),
        "overdue_payments_count":    overdue_count,
        # "average_collection_period": round(mean(collection_days), 1) if collection_days else 0,
        # "average_late_payments":     math.ceil(mean(late_days))      if late_days       else 0,
        "usage_of_credit_limit":     credit_usage,
    }
# ── Lambda entry point ─────────────────────────────────────────────────────────
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    deal_owners = get_all_hubspot_users()
    token   = _get_bc_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Resolve BC company ID
    company_id = requests.get(f"{BC_V2_BASE}/companies", headers=headers).json()["value"][0]["id"]

    # Fetch recently modified customers
    since = (datetime.now(timezone.utc) - timedelta(days=PAST_DAYS)).isoformat()

    days_since = PAST_DAYS
    days_range = DAY_RANGE
    days_until = days_since - days_range

    #
    # while days_until > 0:
    since = (datetime.now(timezone.utc) - timedelta(days=days_since)).isoformat()
    until = (datetime.now(timezone.utc) - timedelta(days=days_until)).isoformat()

    params = {
        "$select": "*",
        "$filter": f"lastModifiedDateTime ge {since}"
    }
    #
    params = {
        "$select": "*",
        "$filter": f"lastModifiedDateTime ge {since} and lastModifiedDateTime le {until}"
    }
    resp = requests.get(
        f"{BC_V2_BASE}/companies({company_id})/customers",
        headers=headers,
        # params={"$select": "*", "$filter": f"lastModifiedDateTime ge {since}"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    customers = safe_get_value(resp)
    random.shuffle(customers)  # single in-place shuffle (was two redundant random.sample calls)

    # Pre-index DBA/CRM supplement records for O(1) lookup per customer
    dba_records = safe_get_value(
        requests.get(f"{BC_CUSTOM_BASE}/companies({company_id})/customerscrm", headers=headers, timeout=30)
    )
    dba_by_id = {r["id"]: r for r in dba_records if "id" in r}

    companies = []
    for cust_ind, customer in enumerate(customers):
        customer_number = customer.get("number")
        customer_id     = customer.get("id")

        # if customer_id not in ["603cea26-8473-ee11-8def-0022481c5bb1"]:
        #     continue

        # Extended fields from Customer_Master
        fields_resp = requests.get(
            f"{BC_ODATA_BASE}/Customer_Master?$filter=No eq '{customer_number}'",
            headers=headers,
            timeout=30,
        )
        extra_fields = (safe_get_value(fields_resp) or [{}])[0]

        # Merge: base customer < extended fields < DBA supplement
        customer_merged = {**customer, **extra_fields, **(dba_by_id.get(customer_id) or {})}

        # Ledger entries for payment metrics
        transactions = safe_get_value(
            requests.get(
                f"{BC_ODATA_BASE}/Cust_LedgerEntries?$filter=Customer_No eq '{customer_number}'",
                headers=headers,
                timeout=30,
            )
        )
        # transactions = safe_get_value(
        #     requests.get(
        #         f"{BC_ODATA_BASE}/detailedCustomerLedgEntries?$filter=Customer_No eq '{customer_number}'",
        #         headers=headers,
        #         timeout=30,
        #     )
        # )

        metrics = calculate_collection_metrics(transactions, credit_limit=customer.get("creditLimit", 0))
        merged  = {**map_company(customer_merged, deal_owners), **metrics}

        encoded_filter = quote(f"'No.' IS '{customer_number}'", safe="")
        merged["business_central_url"] = (
            f"https://businesscentral.dynamics.com/{TENANT_ID}/Production"
            f"?company={quote(COMPANY_NAME)}&page=21&filter={encoded_filter}"
        )
        merged["customer_name"] = (merged.get("customer_name") or "").strip().title() or None
        merged["name"]          = (merged.get("name")          or "").strip().title() or None
        merged["lifecyclestage"] = "customer"

        companies.append(merged)

        if len(companies) >= BATCH_SIZE:
            payload_url, payload = prepare_companies_batch_payload(companies, unique_prop="bc_unique_id_2")
            results = send_batch_upsert(payload_url, payload, HUBSPOT_TOKEN)
            print(f"Batch sent: {BATCH_SIZE} companies. Last: {customer_number} ({cust_ind + 1}/{len(customers)})")
            companies = []

    # Flush remaining records
    if companies:
        payload_url, payload = prepare_companies_batch_payload(companies, unique_prop="bc_unique_id_2")
        send_batch_upsert(payload_url, payload, HUBSPOT_TOKEN)

    return {
        "company_id":        company_id,
        "results_completed": True,
        "statusCode":        200,
        "body": json.dumps({"message": "Processed companies", "results_completed": True}),
    }


if __name__ == "__main__":
    print(lambda_handler({}, None))
