import json
import requests
from typing import List, Dict, Tuple, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

HUBSPOT_API = "https://api.hubapi.com"


# =========================================================
# 1️⃣  CONTACT BATCH PREPARATION
# =========================================================
def prepare_contacts_batch_payload(
    contact_list: List[Dict[str, str]], unique_prop: str = "email"
) -> Tuple[str, Dict]:
    """
    Prepares the payload and URL for upserting multiple contacts in HubSpot.

    Args:
        contact_list (list[dict]): List of contact records, each with properties.
                                   Each record must include the unique_prop (default: "email").
        unique_prop (str): The unique property to identify existing records (e.g., "email").

    Returns:
        tuple: (url, payload)
            - url (str): HubSpot batch upsert endpoint for contacts
            - payload (dict): JSON body formatted for HubSpot API

    Example:
        contacts = [
            {"email": "alice@example.com", "firstname": "Alice", "lastname": "Smith"},
            {"email": "bob@example.com", "firstname": "Bob", "lastname": "Jones"}
        ]
        url, payload = prepare_contacts_batch_payload(contacts)
    """
    url = f"{HUBSPOT_API}/crm/v3/objects/contacts/batch/upsert"
    inputs = [
        {"idProperty": unique_prop, "id": c[unique_prop], "properties": c}
        for c in contact_list
    ]
    return url, {"inputs": inputs}



def prepare_deals_batch_payload(
    deal_list: List[Dict[str, str]],
    unique_prop: str = "tracker_pro_id"
) -> Tuple[str, Dict]:

    def convert_datetimes(props: Dict) -> Dict:
        new_props = {}
        for k, v in props.items():
            if isinstance(v, datetime):
                new_props[k] = int(v.timestamp() * 1000)
            else:
                new_props[k] = v
        return new_props

    url = f"{HUBSPOT_API}/crm/v3/objects/deals/batch/upsert"

    inputs = []
    for d in deal_list:
        clean_props = convert_datetimes(d)
        inputs.append({
            "idProperty": unique_prop,
            "id": d[unique_prop],
            "properties": clean_props
        })

    return url, {"inputs": inputs}



# =========================================================
# 3️⃣  LINE ITEM BATCH PREPARATION
# =========================================================
def prepare_line_items_batch_payload(
    line_item_list: List[Dict[str, str]], unique_prop: str = "name"
) -> Tuple[str, Dict]:
    """
    Prepares the payload and URL for upserting multiple line items in HubSpot.

    Args:
        line_item_list (list[dict]): List of line item records, each with properties.
                                     Each record must include the unique_prop (default: "name" or "hs_sku").
        unique_prop (str): The unique property used for upsert matching (e.g., "hs_sku" for SKU-based logic).

    Returns:
        tuple: (url, payload)
            - url (str): HubSpot batch upsert endpoint for line items
            - payload (dict): JSON body formatted for HubSpot API

    Example:
        line_items = [
            {"name": "Widget-A", "price": "100", "quantity": "2"},
            {"name": "Widget-B", "price": "250", "quantity": "1"}
        ]
        url, payload = prepare_line_items_batch_payload(line_items)
    """
    url = f"{HUBSPOT_API}/crm/v3/objects/line_items/batch/upsert"
    inputs = [
        {"idProperty": unique_prop, "id": li[unique_prop], "properties": li}
        for li in line_item_list
    ]
    return url, {"inputs": inputs}


# =========================================================
# 4️⃣  SHARED SEND FUNCTION
# =========================================================
def send_batch_upsert(url: str, payload: Dict, hs_token: str, batch_size=100) -> List[Dict]:
    """
    Sends batch upserts to HubSpot in chunks of 100.

    Args:
        url (str): HubSpot batch upsert endpoint.
        payload (dict): Must contain an "inputs" list.
        hs_token (str): HubSpot private app token.

    Returns:
        list[dict]: List of responses, one per batch.
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


# =========================================================
# 🏢 COMPANY BATCH PREPARATION
# =========================================================
def prepare_companies_batch_payload(
    company_list: List[Dict[str, str]], unique_prop: str = "domain"
) -> Tuple[str, Dict]:
    """
    Prepares the payload and URL for upserting multiple companies in HubSpot.

    Args:
        company_list (list[dict]):
            List of company records, each as a dict of HubSpot properties.
            Each record must include the `unique_prop` key (default: "domain").
        unique_prop (str):
            The unique property used to identify existing companies, typically "domain"
            or a custom unique field (like "p21_customer_id" or "epicor_company_id").

    Returns:
        tuple: (url, payload)
            - url (str): HubSpot batch upsert endpoint for companies
            - payload (dict): JSON body formatted for HubSpot API

    Example:
        companies = [
            {"domain": "acme.com", "name": "Acme Corp", "industry": "Manufacturing"},
            {"domain": "orbitinteractive.com", "name": "Orbit Interactive", "city": "Phoenix"}
        ]
        url, payload = prepare_companies_batch_payload(companies)
        # url  → 'https://api.hubapi.com/crm/v3/objects/companies/batch/upsert'
        # payload → {'inputs': [{...}, {...}]}
    """
    url = f"{HUBSPOT_API}/crm/v3/objects/companies/batch/upsert"
    inputs = [
        {"idProperty": unique_prop, "id": c[unique_prop], "properties": c}
        for c in company_list
        if unique_prop in c and c[unique_prop]  # skip invalid rows
    ]
    return url, {"inputs": inputs}


# =========================================================
# 🧠 QUERY POSTGRESQL SERVER
# =========================================================
def query_postgres(
    host: str,
    database: str,
    user: str,
    password: str,
    query: str,
    params: tuple = ()
) -> List[Dict[str, Any]]:
    """
    Executes a SQL query against a PostgreSQL database using psycopg2.

    Args:
        host (str): Hostname or IP of the PostgreSQL server (e.g., 'localhost', '10.0.0.15').
        database (str): Database name.
        user (str): Username for the database.
        password (str): Password for the database.
        query (str): SQL query to execute.
        params (tuple, optional): Query parameters for safe substitution (%s placeholders).

    Returns:
        list[dict]: List of query results where each row is a dict mapping column names to values.

    Example:
        rows = query_postgres(
            host='localhost',
            database='crm_data',
            user='postgres',
            password='mypassword',
            query='SELECT id, name FROM customers WHERE country = %s',
            params=('USA',)
        )
        print(rows)
    """
    conn = psycopg2.connect(
        host=host,
        dbname=database,
        user=user,
        password=password
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            # Only fetch if it's a SELECT
            if cursor.description:
                return cursor.fetchall()
            conn.commit()
            return []
    finally:
        conn.close()

