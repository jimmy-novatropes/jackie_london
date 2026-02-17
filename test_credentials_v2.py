import json, requests

# 1. Load credentials
creds = json.load(open("credentials2.json"))
token_url = f"https://login.microsoftonline.com/{creds['tenant_id']}/oauth2/v2.0/token"

# 2. Get access token
resp = requests.post(token_url, data={
    "grant_type": "client_credentials",
    "client_id": creds["client_id"],
    "client_secret": creds["client_secret"],
    "scope": "https://api.businesscentral.dynamics.com/.default",
})
resp.raise_for_status()
token = resp.json()["access_token"]


# 3. Call the API
base = creds.get("api_base_url") or f"https://api.businesscentral.dynamics.com/v2.0/{creds['tenant_id']}/Production/api/v2.0"
base = f"https://api.businesscentral.dynamics.com/v2.0/{creds['tenant_id']}/Production/api/v2.0"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
request_url = f"{base}/companies"
# request_url = f"{base}/customers"

company_account = requests.get(request_url, headers=headers).json()["value"]
print("Companies:", company_account)

# After getting `companies` and `company_id`...
company_id = company_account[0]["id"]
# 4. Fetch all contacts in the company
companies_resp = requests.get(f"{base}/companies({company_id})/customers", headers=headers, timeout=30)
companies_resp.raise_for_status()
companies = companies_resp.json().get("value", [])
print("Contacts:", companies)

contacts_resp = requests.get(f"{base}/companies({company_id})/contacts", headers=headers, timeout=30)
contacts_resp.raise_for_status()
contacts = contacts_resp.json().get("value", [])
print("Contacts:", contacts)


if contacts:
    contact_id = contacts[0]["id"]
    single = requests.get(f"{base}/companies({company_id})/contacts({contact_id})", headers=headers, timeout=30)
    single.raise_for_status()
    print("Single Contact:", single.json())

customer_resp = requests.get(f"{base}/companies({company_id})/customers", headers=headers, timeout=30)
customer_resp.raise_for_status()
customers = customer_resp.json().get("value", [])
if customers:
    customer_id = customers[0]["id"]
    single = requests.get(f"{base}/companies({company_id})/customers({customer_id})", headers=headers, timeout=30)
    single.raise_for_status()
    print("Single Customer:", single.json())

print()

