from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from supporting_functions import load_json_file
import requests

MISSING = object()

# Load once
PROPERTY_MAP: Dict[str, Dict[str, str]] = load_json_file("object_properties.json") or {}


from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from datetime import datetime, timezone

INVALID_DATES = {"0001-01-01", "0000-00-00", "", None}

def to_hubspot_date_safe(date_str, input_format):
    """
    Convert date string → HubSpot timestamp (ms at midnight UTC)
    """

    if date_str in INVALID_DATES:
        return None

    try:
        dt = datetime.strptime(date_str, input_format)

        if dt.year <= 1900:
            return None

        # Force midnight UTC directly
        dt_utc = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)

        return int(dt_utc.timestamp() * 1000)

    except Exception:
        return None
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
    mapped["payment_terms"] = mapped.get("paymentTerms").lower().replace(" ", "_") if mapped.get("paymentTerms") else None

    if "startdate" in mapped and mapped["startdate"]:
        mapped["startdate"] = to_hubspot_date_safe(mapped.get("startdate"), "%Y-%m-%d")
    if "end_date" in mapped and mapped["end_date"]:
        mapped["end_date"] = to_hubspot_date_safe(mapped.get("end_date"), "%Y-%m-%d")
    if "out_of_business_date" in mapped and mapped["out_of_business_date"]:
        mapped["out_of_business_date"] = to_hubspot_date_safe(mapped.get("out_of_business_date"), "%Y-%m-%d")

    if "language_code" in mapped and mapped["language_code"]:
        print()
    return mapped


