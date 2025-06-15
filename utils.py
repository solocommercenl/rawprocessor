"""
utils.py

Shared helpers for rawprocessor (date parsing, Mongo lookups, hash logic, etc).
All helpers are modular, async-ready, robust, and documented.
"""

import hashlib
import re
import logging
from datetime import datetime
from typing import Tuple, Optional, Dict, Any, List, Union
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger("rawprocessor.utils")

# --- Hash Group Calculation ---
def calculate_hash_groups(doc: Dict[str, Any]) -> Dict[str, str]:
    """
    Compute SHA-256 hashes for each group of fields used in partial update checks.
    Groups: pricing, leasing, gallery.
    """
    groups = {
        "pricing": ["im_price", "im_nett_price", "im_bpm_rate", "im_vat_amount"],
        "leasing": ["im_monthly_payment", "im_down_payment", "im_desired_remaining_debt"],
        "gallery": ["im_gallery"]
    }
    result = {}
    for group, fields in groups.items():
        payload = {field: doc.get(field) for field in fields if field in doc}
        payload_str = str(sorted(payload.items())).encode("utf-8")
        result[group] = hashlib.sha256(payload_str).hexdigest()
    return result

# --- Normalize Make/Model ---
def normalize_make_model(make: str, model: str) -> Tuple[str, str]:
    make_clean = make.strip()
    model_clean = model.strip()
    return make_clean, model_clean

# --- Power Extraction ---
def extract_power_values(power_str: str) -> Tuple[Optional[int], Optional[int]]:
    kw_match = re.search(r"(\d+)\s*kW", power_str)
    hp_match = re.search(r"\((\d+)\s*PK\)", power_str)
    kw = int(kw_match.group(1)) if kw_match else None
    hp = int(hp_match.group(1)) if hp_match else None
    return kw, hp

# --- Flexible Date Parsing ---
def parse_registration_date(registration_date: Optional[str], registration_year: Optional[int]) -> Tuple[int, int]:
    if registration_date:
        try:
            if "/" in registration_date:
                parts = registration_date.split("/")
                if len(parts) == 2:
                    return int(parts[0]), int(parts[1])
                elif len(parts) == 3:
                    return int(parts[1]), int(parts[2])
        except Exception as ex:
            logger.warning("Failed to parse registration_date %s: %s", registration_date, ex)
    if registration_year:
        return 1, int(registration_year)
    return 0, 0

# --- Age in Months Calculation ---
def calculate_age_in_months(today: datetime, reg_month: int, reg_year: int) -> int:
    if reg_year == 0:
        return 0
    months = (today.year - reg_year) * 12 + (today.month - reg_month)
    if months < 0:
        months = 0
    return months

# --- Mongo Depreciation Lookup ---
async def get_depreciation_percentage(db: AsyncIOMotorDatabase, age_in_months: int) -> float:
    doc = await db.depreciation_table.find_one({"start": {"$lte": age_in_months}, "end": {"$gt": age_in_months}})
    if not doc:
        return 0.0
    base = doc["base_percent"]
    monthly = doc["monthly_percent"]
    return base + (age_in_months - doc["start"]) * monthly

# --- Mongo BPM Lookup ---
async def get_bpm_entry(db: AsyncIOMotorDatabase, reg_year: int, raw_emissions: float, reg_month: int, fuel_type: str) -> Optional[dict]:
    query = {"year": reg_year}
    if reg_year == 2020:
        query["half"] = "H1" if reg_month < 7 else "H2"
    doc = await db.bpm_tables.find_one(query)
    if not doc:
        return None
    for entry in doc["entries"]:
        if entry["lower"] <= raw_emissions < entry["upper"]:
            return entry
    return None

# --- Mongo PHEV Lookup ---
async def get_phev_entry(db: AsyncIOMotorDatabase, reg_year: int, raw_emissions: float, reg_month: int) -> Optional[dict]:
    query = {"year": reg_year}
    if reg_year == 2020:
        query["half"] = "H1" if reg_month < 7 else "H2"
    doc = await db.phev_tables.find_one(query)
    if not doc:
        return None
    for entry in doc["entries"]:
        if entry["lower"] <= raw_emissions < entry["upper"]:
            return entry
    return None

# --- Mongo Diesel Surcharge Lookup ---
async def get_diesel_surcharge(db: AsyncIOMotorDatabase, reg_year: int) -> Optional[dict]:
    return await db.diesel_surcharges.find_one({"year": reg_year})

# --- Normalize Gallery ---
def normalize_gallery(images: Union[str, List[str]]) -> List[str]:
    """
    Normalize the gallery by converting a string of image URLs into a list of URLs.
    It handles both `str` and `list` inputs.
    """
    if isinstance(images, list):
        return [url.strip() for url in images if url.strip()]
    if isinstance(images, str):
        if "|" in images:
            return [url.strip() for url in images.split("|") if url.strip()]
        elif "," in images:
            return [url.strip() for url in images.split(",") if url.strip()]
        return [images.strip()] if images.strip() else []
    return []
