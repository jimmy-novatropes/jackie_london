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


def generic_map(source: Dict[str, Any]) -> Dict[str, Any]:
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

    if ";" in mapped.get("email", ""):
        mapped["email"] = mapped["email"].split(";")[0].strip()
        mapped["work_email"] = mapped["email"].split(";")[1].strip() if len(mapped["email"].split(";")) > 1 else None
    try:
        if "phone" in mapped and mapped["phone"]:
            mapped["mobile_phone_number"] = mapped["phone"]
            mapped["phone"], country = format_phone_number(mapped["phone"])
            mapped["hs_country_region_code"] = country
            # print(mapped["phone"], country)

        if "mobilephone" in mapped and mapped["mobilephone"]:
            mapped["mobile_phone_number"] = mapped["mobilephone"]
            mapped["mobilephone"], country = format_phone_number(mapped["mobilephone"])
            mapped["hs_country_region_code"] = country
            # print(mapped["mobilephone"], country)

            if "phone" not in mapped or not mapped["phone"]:
                mapped["phone"] = mapped["mobilephone"]
    except Exception as e:
        print(f"Error formatting phone number '{mapped.get('phone')}' for contact '{mapped.get('displayName', '')}': {e}")
        print()

    return mapped


import re

from typing import Callable

def format_phone_number(phone: str, default_country: str = "US") -> tuple[str, str]:
    """
    Returns (formatted_number, country_name).
    Defaults to US if no country code is detected.
    """
    phone = phone.strip()
    has_plus = phone.startswith('+')
    digits = re.sub(r'\D', '', phone)

    # code -> (country_name, expected_local_digits, format_fn)
    country_formats: dict[str, tuple[str, int, Callable[[str], str]]] = {
        # "1":   ("US/CA", 10, lambda d: f"({d[:3]}) {d[3:6]}-{d[6:]}"),
        "1": ("US/CA", 10, lambda d: f"+1 ({d[:3]}) {d[3:6]}-{d[6:]}"),
        "44":  ("UK",    10, lambda d: f"+44 {d[:4]} {d[4:7]} {d[7:]}"),
        "61":  ("AU",     9, lambda d: f"+61 {d[:1]} {d[1:5]} {d[5:]}"),
        "64":  ("NZ",     9, lambda d: f"+64 {d[:2]} {d[2:5]} {d[5:]}"),
        "49":  ("DE",    10, lambda d: f"+49 {d[:3]} {d[3:7]} {d[7:]}"),
        "33":  ("FR",    10, lambda d: f"+33 {d[:1]} {d[1:3]} {d[3:5]} {d[5:7]} {d[7:]}"),
        "34":  ("ES",     9, lambda d: f"+34 {d[:3]} {d[3:6]} {d[6:]}"),
        "39":  ("IT",    10, lambda d: f"+39 {d[:3]} {d[3:7]} {d[7:]}"),
        "81":  ("JP",    10, lambda d: f"+81 {d[:2]} {d[2:6]} {d[6:]}"),
        "86":  ("CN",    11, lambda d: f"+86 {d[:3]} {d[3:7]} {d[7:]}"),
        "91":  ("IN",    10, lambda d: f"+91 {d[:5]} {d[5:]}"),
        "27":  ("ZA",     9, lambda d: f"+27 {d[:2]} {d[2:5]} {d[5:]}"),
        "31":  ("NL",     9, lambda d: f"+31 {d[:2]} {d[2:5]} {d[5:]}"),
        # "32":  ("BE",     9, lambda d: f"+32 {d[:3]} {d[3:6]} {d[6:]}"),
        "41":  ("CH",     9, lambda d: f"+41 {d[:2]} {d[2:5]} {d[5:7]} {d[7:]}"),
        "45":  ("DK",     8, lambda d: f"+45 {d[:2]} {d[2:4]} {d[4:6]} {d[6:]}"),
        "46":  ("SE",     9, lambda d: f"+46 {d[:2]} {d[2:5]} {d[5:]}"),
        "47":  ("NO",     8, lambda d: f"+47 {d[:3]} {d[3:5]} {d[5:]}"),
        # --- North / Central America ---
        "52":  ("MX",    10, lambda d: f"+52 {d[:2]} {d[2:6]} {d[6:]}"),
        "501": ("BZ",     7, lambda d: f"+501 {d[:3]} {d[3:]}"),
        "502": ("GT",     8, lambda d: f"+502 {d[:4]} {d[4:]}"),
        "503": ("SV",     8, lambda d: f"+503 {d[:4]} {d[4:]}"),
        "504": ("HN",     8, lambda d: f"+504 {d[:4]} {d[4:]}"),
        "505": ("NI",     8, lambda d: f"+505 {d[:4]} {d[4:]}"),
        "506": ("CR",     8, lambda d: f"+506 {d[:4]} {d[4:]}"),
        "507": ("PA",     8, lambda d: f"+507 {d[:4]} {d[4:]}"),
        # --- South America ---
        "51":  ("PE",     9, lambda d: f"+51 {d[:3]} {d[3:6]} {d[6:]}"),
        "54":  ("AR",    10, lambda d: f"+54 {d[:2]} {d[2:6]} {d[6:]}"),
        "55":  ("BR",    11, lambda d: f"+55 {d[:2]} {d[2:7]} {d[7:]}"),
        "56":  ("CL",     9, lambda d: f"+56 {d[:1]} {d[1:5]} {d[5:]}"),
        "57":  ("CO",    10, lambda d: f"+57 {d[:3]} {d[3:7]} {d[7:]}"),
        "58":  ("VE",    10, lambda d: f"+58 {d[:3]} {d[3:7]} {d[7:]}"),
        "591": ("BO",     8, lambda d: f"+591 {d[:1]} {d[1:4]} {d[4:]}"),
        # "592": ("GY",     7, lambda d: f"+592 {d[:3]} {d[3:]}"),
        "593": ("EC",     9, lambda d: f"+593 {d[:2]} {d[2:6]} {d[6:]}"),
        "594": ("GF",     9, lambda d: f"+594 {d[:3]} {d[3:6]} {d[6:]}"),
        "595": ("PY",     9, lambda d: f"+595 {d[:3]} {d[3:6]} {d[6:]}"),
        "597": ("SR",     7, lambda d: f"+597 {d[:3]} {d[3:]}"),
        "598": ("UY",     8, lambda d: f"+598 {d[:4]} {d[4:]}"),
        # Add to country_formats:
        "372": ("EE", 8, lambda d: f"+372 {d[:4]} {d[4:]}"),
        "233": ("GH", 9, lambda d: f"+233 {d[:2]} {d[2:5]} {d[5:]}"),
    }

    detected_code = None
    local_digits = digits

    if has_plus or len(digits) > 10:
        for code in sorted(country_formats.keys(), key=len, reverse=True):
            if digits.startswith(code):
                detected_code = code
                local_digits = digits[len(code):]
                break

    if not detected_code:
        if default_country == "US":
            if len(digits) == 11 and digits.startswith('1'):
                detected_code = "1"
                local_digits = digits[1:]
            else:
                detected_code = "1"
                local_digits = digits
        else:
            raise ValueError(f"Could not detect country code for: '{phone}'")

    if detected_code not in country_formats:
        return f"+{detected_code} {local_digits}", f"Unknown (+{detected_code})"

    country_name, expected_len, fmt_fn = country_formats[detected_code]

    if len(local_digits) != expected_len:
        # Wrong length for detected country — try falling back to US before raising
        us_local = digits[1:] if (len(digits) == 11 and digits.startswith('1')) else digits
        if default_country == "US" and len(us_local) == 10:
            _, _, us_fmt = country_formats["1"]
            return us_fmt(us_local), "US/CA (fallback)"
        raise ValueError(
            f"Invalid number length for country +{detected_code} ({country_name}): "
            f"got {len(local_digits)} digits, expected {expected_len}"
        )

    return fmt_fn(local_digits), country_name