from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from supporting_functions import load_json_file
import requests

MISSING = object()

# Load once
PROPERTY_MAP: Dict[str, Dict[str, str]] = load_json_file("object_properties.json") or {}


def get_nested_value(data: Dict[str, Any], path: str, default=MISSING) -> Any:
    """
    Get a nested value from a dict using 'a.b.c' paths.
    Returns `default` if any part is missing.
    """
    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def generic_map(source: Dict[str, Any]):
    """
    Generic mapper using PROPERTY_MAP[map_key] to map fields:
    {source_path: target_prop}
    """
    field_map = PROPERTY_MAP
    return {
        target_key: value
        for source_path, target_key in field_map.items()
        if (value := get_nested_value(source, source_path)) is not MISSING
    }


# --------------------------------------------------------------------
# Email normalization for contacts
# --------------------------------------------------------------------
def normalize_emails(mapped: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cleans up a raw email string and splits into primary/work/member emails.
    Uses the value already in mapped["email"] if present.
    """
    import re

    raw = (mapped.get("email") or "").strip()
    if not raw:
        return mapped

    # 1) Normalize separators ; , / | \ "
    raw = re.sub(r'[;,/|\\"]+', ";", raw)

    # 2) Strip display-name format: "John <john@x.com>" → "john@x.com"
    raw = re.sub(r".*<([^>]+)>", r"\1", raw)
    raw = raw.replace("<", "").replace(">", "")

    # 3) Remove garbage text after the email
    raw = re.split(r"[^a-zA-Z0-9@._+-]+", raw)[0].strip()

    # 4) Fix common domain typo: @oxy.xom → @oxy.com
    raw = re.sub(r"@(\w+)\.xom$", r"@\1.com", raw, flags=re.IGNORECASE)

    # 5) Split multi-email strings
    parts = [
        p.strip().replace(" ", "")
        for p in raw.split(";")
        if p.strip()
    ]

    mapped["email"] = parts[0] if len(parts) > 0 else None
    mapped["work_email"] = parts[1] if len(parts) > 1 else None
    mapped["member_email"] = parts[2] if len(parts) > 2 else None

    # 6) Validate primary email
    if mapped["email"] and "@" not in mapped["email"]:
        mapped["email"] = None

    return mapped


# --------------------------------------------------------------------
# Date helpers
# --------------------------------------------------------------------
def _simplify_date_numeric(date_str: str | None) -> Tuple[str | None, str | None, str | None]:
    """
    Convert '2023-11-27T00:00:00.000Z' -> ('11', '27', '2023')
    Returns (None, None, None) on failure.
    """
    if not date_str:
        return None, None, None

    try:
        if date_str.endswith("Z"):
            date_str = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%Y")
    except Exception:
        try:
            s = date_str.split("T")[0]  # '2023-11-27'
            y, m, d = s.split("-")
            return m.zfill(2), d.zfill(2), y
        except Exception:
            return None, None, None


def combine_dates_numeric(start: str | None, end: str | None) -> str | None:
    """
    Build a compact range like:
      same year, same month:  11/27–28/2023
      same year, diff month:  12/02–01/27/2025
      diff years:             12/02/2025–01/27/2026
    """
    m1, d1, y1 = _simplify_date_numeric(start)
    m2, d2, y2 = _simplify_date_numeric(end)

    if m1 and d1 and y1 and m2 and d2 and y2:
        return f"{m1}/{d1}/{y1}–{m2}/{d2}/{y2}"


    return None


# --------------------------------------------------------------------
# Object-specific mappers
# --------------------------------------------------------------------
def map_company(company: Dict[str, Any], deal_owners) -> Dict[str, Any]:

    mapped = generic_map(company)
    mapped["name"] = mapped.get("customer_name", "").strip()
    return mapped


def map_apartment(group: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps stayable_group to an apartment-like HS object, plus address_1 from first stayable.
    """
    mapped = generic_map(group, "stayable_group")

    for stayable in group.get("stayables", []):
        addr = stayable.get("address1")
        addr2 = stayable.get("address2")
        addr3 = stayable.get("address3")
        mapped["hs_address_1"] = addr
        mapped["hs_address_2"] = addr2
        mapped["address_3"] = addr3



    return mapped


def map_contact(person: Dict[str, Any], deal_owners) -> Dict[str, Any]:
    mapped = generic_map(person)
    mapped["firstname"] = person["displayName"].split(" ")[0].strip() if person.get("displayName") else ""
    mapped["lastname"] = " ".join(person["displayName"].split(" ")[1:]).strip() if person.get("displayName") and len(person["displayName"].split(" ")) > 1 else ""
    return mapped


def map_stay(stay: Dict[str, Any], deal_owners) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Map a stay to a "stay contact" HS service object + a list of resident links:
    [{codeone_person_uid, codeone_stay_uid}, ...]
    """
    mapped = generic_map(stay, "contact_from_stay")

    if "sales_representative_firstname" in mapped and "sales_representative_lastname" in mapped:
        for owner_index, owner in enumerate(deal_owners):
            if (
                owner.get("firstName") == mapped["sales_representative_firstname"].strip()
                and owner.get("lastName") == mapped["sales_representative_lastname"].strip()
            ):
                mapped["hubspot_owner_id"] = owner.get("userId")
                break
            if owner_index == len(deal_owners) - 1:
                print()
    # Dealname-like hs_name
    total_amount = 0
    for charge in stay.get("charges", []):
        total_amount += charge.get("amount", 0)

    unit_name = get_nested_value(stay, "stayableU.name")
    group_name = get_nested_value(stay, "stayableU.stayableGroupU.name")
    start_raw = get_nested_value(stay, "start")
    end_raw = get_nested_value(stay, "end")
    residents = stay.get("residents", [])
    resident_data = residents[0].get("personU", {}) if residents else {}
    first_name = resident_data.get("firstName", "")
    last_name = resident_data.get("lastName", "")
    # stay_rate = stay["stays"][0].get("rate", {}) if stay.get("stays") else {}
    # mapped["booking_amount"] = stay_rate
    mapped["stay_start_date"] = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
    mapped["stay_end_date"] = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
    mapped["hs_amount_paid"] = total_amount
    # mapped["amount"] = stay_rate
    mapped["codeone_primary_resident_firstname"] = first_name
    mapped["codeone_primary_resident_lastname"] = last_name
    mapped["codeone_stay_primary_resident_uid"] = resident_data.get("personUid", "")
    if resident_data.get("primaryEmailAddressU") is not None:
        mapped["codeone_primary_resident_email"] = resident_data.get("primaryEmailAddressU", {}).get("address", "")

    # if mapped["codeone_stay_uid"] == "a4ca0880-2e09-4bd4-8226-6c43e0b688e0":
    #     print()

     # Dealname-like hs_name

    parts: List[str] = []
    if unit_name not in (None, MISSING):
        parts.append(str(unit_name).strip())
    if group_name not in (None, MISSING):
        parts.append(str(group_name).strip())

    date_part = combine_dates_numeric(start_raw, end_raw)
    # date_start = start_raw.split("T")[0].replace("-", "/")
    # date_end = end_raw.split("T")[0].replace("-", "/")
    # date_part = f"{date_start} - {date_end}"
    if date_part:
        parts.append(date_part)

    if parts:
        mapped["hs_name"] = " | ".join(parts)

    # Static pipeline stage
    mapped["hs_pipeline_stage"] = "8e2b21d0-7a90-4968-8f8c-a8525cc49c70"

    # Collect residents from the stay
    residents: List[Dict[str, Any]] = []
    uid_resident_list = ""
    for resident in stay.get("residents", []):
        resident_uid = resident.get("personU", {}).get("personUid")
        if resident_uid:
            residents.append(
                {
                    "codeone_person_uid": resident_uid,
                    "codeone_stay_uid": stay.get("stayUid"),
                }
            )
            if resident_uid != mapped["codeone_stay_primary_resident_uid"]:
                uid_resident_list += resident_uid + ","
    if uid_resident_list.endswith(","):
        uid_resident_list = uid_resident_list[:-1]
    mapped["codeone_secondary_residents_uid"] = uid_resident_list
    return mapped, residents


def map_deal(booking: Dict[str, Any], deal_owners) -> Dict[str, Any]:
    """
    Maps booking to a HS deal-like object.
    Expects PROPERTY_MAP["contact_from_booking"] mapping.
    """
    mapped = generic_map(booking)
    mapped["dealname"] = mapped["customer_name"] + " - " + mapped["client_status"] + " - " + mapped["invoice_date"]
    if "closedate" in mapped and "000" in mapped["closedate"]:
        mapped["closedate"] = None
        print()

    return mapped
