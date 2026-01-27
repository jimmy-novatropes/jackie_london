import os
import requests
import pandas as pd

# =============================
# CONFIG
# =============================

HUBSPOT_TOKEN = ""
EXCEL_FILE = "properties.xlsx"

# sheet name -> hubspot object
SHEET_OBJECT_MAP = {
    "Deals-Invoices": "deals",
    "Companies": "companies",
    "Contacts": "contacts",
    # "Custom Object": "2-12345678"
}


BASE = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json"
}

# =============================
# TYPE NORMALIZATION
# =============================

TYPE_MAP = {
    "text": ("string", "text"),
    "single line": ("string", "text"),
    "single line text": ("string", "text"),
    "textarea": ("string", "textarea"),
    "multi line": ("string", "textarea"),

    "number": ("number", "number"),
    "int": ("number", "number"),
    "float": ("number", "number"),

    "bool": ("bool", "booleancheckbox"),
    "boolean": ("bool", "booleancheckbox"),

    "date": ("date", "date"),
    "datetime": ("datetime", "date"),

    "dropdown": ("enumeration", "select"),
    "select": ("enumeration", "select"),
    "radio": ("enumeration", "radio"),

    "multi-select": ("enumeration", "checkbox"),
    "multiselect": ("enumeration", "checkbox"),
    "checkbox": ("enumeration", "checkbox"),
}

# =============================
# HUBSPOT HELPERS
# =============================

def get_existing_properties(object_type):
    r = requests.get(f"{BASE}/crm/v3/properties/{object_type}", headers=HEADERS)
    r.raise_for_status()
    return {p["name"]: p for p in r.json()["results"]}


def delete_property(object_type, name):
    requests.delete(f"{BASE}/crm/v3/properties/{object_type}/{name}", headers=HEADERS)


def create_property(object_type, payload):
    r = requests.post(f"{BASE}/crm/v3/properties/{object_type}", headers=HEADERS, json=payload)
    if not r.ok:
        print("❌ CREATE FAIL:", payload["name"], r.text[:200])
    return r.ok


def update_property(object_type, name, payload):
    r = requests.patch(f"{BASE}/crm/v3/properties/{object_type}/{name}", headers=HEADERS, json=payload)
    if not r.ok:
        print("❌ UPDATE FAIL:", name, r.text[:200])
    return r.ok


# =============================
# EXCEL HELPERS
# =============================

def normalize_type(val):
    v = str(val).strip().lower().replace("_", " ").replace("-", " ")
    return TYPE_MAP.get(v, ("string", "text"))


def parse_options(raw):
    if pd.isna(raw) or not str(raw).strip():
        return None

    text = str(raw)
    for sep in ["\n", ";", ",", "/"]:
        text = text.replace(sep, "|")

    opts = []
    for opt in text.split("|"):
        val = opt.strip()
        if val:
            opts.append({
                "label": val,
                "value": val.lower().replace(" ", "_")
            })
    return opts


def is_true(val):
    return str(val).strip().lower() in ("yes", "true", "1", "y")


# =============================
# CORE LOGIC
# =============================

def process_sheet(sheet_name, object_type):
    print(f"\n===== {sheet_name} → {object_type} =====")

    df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
    existing = get_existing_properties(object_type)

    for _, row in df.iterrows():
        label = str(row.get("Property Label HubSpot", "")).strip()
        name = str(row.get("Internal Name HubSpot", "")).strip()
        desc = str(row.get("Description", "")).strip()
        group = str(row.get("Group name", "dealinformation")).strip()

        if not name or not label:
            continue
        if name == "nan" or label == "nan":
            continue

        hs_type, field_type = normalize_type(row.get("Type"))
        unique = is_true(row.get("Unique"))
        options = parse_options(row.get("Options"))

        base_payload = {
            "label": label,
            "description": desc,
            "groupName": group,
            "fieldType": field_type
        }

        create_payload = {
            **base_payload,
            "name": name,
            "type": hs_type,
            "hasUniqueValue": unique
        }

        if options:
            create_payload["options"] = options

        # -------------------
        # EXISTING PROPERTY
        # -------------------
        if name in existing:
            old = existing[name]
            old_type = old.get("type")
            old_unique = old.get("hasUniqueValue", False)

            # must recreate
            if old_type != hs_type or old_unique != unique:
                print(f"🔁 RECREATE: {name}")
                delete_property(object_type, name)
                create_property(object_type, create_payload)
                continue

            # safe updates
            needs_update = False
            update_payload = {}

            for k in ["label", "description", "groupName", "fieldType"]:
                if base_payload.get(k) and base_payload.get(k) != old.get(k):
                    update_payload[k] = base_payload[k]
                    needs_update = True

            # dropdown options append
            if options and hs_type == "enumeration":
                old_opts = {o["value"] for o in old.get("options", [])}
                new_opts = [o for o in options if o["value"] not in old_opts]
                if new_opts:
                    update_payload["options"] = old.get("options", []) + new_opts
                    needs_update = True
                    print(f"➕ Adding options to {name}: {[o['label'] for o in new_opts]}")

            if needs_update:
                print(f"✏️ Updating: {name}")
                update_property(object_type, name, update_payload)
            else:
                print(f"⏭️ No change: {name}")

        # -------------------
        # NEW PROPERTY
        # -------------------
        else:
            print(f"✅ Creating: {name}")
            create_property(object_type, create_payload)


def run():
    xls = pd.ExcelFile(EXCEL_FILE)

    for sheet, obj in SHEET_OBJECT_MAP.items():
        if sheet not in xls.sheet_names:
            print(f"⚠️ Sheet not found: {sheet}")
            continue
        process_sheet(sheet, obj)


if __name__ == "__main__":
    run()
