"""
utils.py

Shared helpers for rawprocessor (date parsing, Mongo lookups, hash logic, etc).
All helpers are modular, async-ready, robust, and documented.

FIXED: Corrected function names and field mapping.
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
        "pricing": ["im_price", "im_nett_price", "im_bmp_rate", "im_vat_amount"],  # Fixed: bmp_rate
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
    """
    Parse registration date from various formats.
    Returns (month, year) tuple.
    """
    if registration_date:
        try:
            if "/" in registration_date:
                parts = registration_date.split("/")
                if len(parts) == 2:  # MM/YYYY
                    return int(parts[0]), int(parts[1])
                elif len(parts) == 3:  # DD/MM/YYYY
                    return int(parts[1]), int(parts[2])
        except (ValueError, IndexError) as ex:
            logger.warning("Failed to parse registration_date %s: %s", registration_date, ex)
    
    if registration_year:
        return 1, int(registration_year)  # Default to January
    
    return 0, 0  # Invalid date

# --- Age in Months Calculation ---
def calculate_age_in_months(today: datetime, reg_month: int, reg_year: int) -> int:
    """
    Calculate age in months between registration date and today.
    """
    if reg_year == 0:
        return 0
    
    months = (today.year - reg_year) * 12 + (today.month - reg_month)
    return max(0, months)  # Ensure non-negative

# --- Mongo Depreciation Lookup ---
async def get_depreciation_percentage(db: AsyncIOMotorDatabase, age_in_months: int) -> float:
    """
    Get depreciation percentage based on vehicle age from MongoDB lookup table.
    """
    try:
        doc = await db.depreciation_table.find_one({
            "start": {"$lte": age_in_months}, 
            "end": {"$gt": age_in_months}
        })
        
        if not doc:
            logger.warning("No depreciation data found for age %s months", age_in_months)
            return 0.0
        
        base = doc["base_percent"]
        monthly = doc["monthly_percent"]
        extra_months = age_in_months - doc["start"]
        
        return base + (extra_months * monthly)
        
    except Exception as ex:
        logger.error("Error getting depreciation percentage: %s", ex)
        return 0.0

# --- Mongo BMP Lookup (Fixed function name) ---
async def get_bmp_entry(db: AsyncIOMotorDatabase, reg_year: int, raw_emissions: float, reg_month: int, fuel_type: str) -> Optional[dict]:
    """
    Get BMP entry from MongoDB lookup tables based on registration year and emissions.
    """
    try:
        query = {"year": reg_year}
        
        # Handle 2020 half-year split
        if reg_year == 2020:
            query["half"] = "H1" if reg_month < 7 else "H2"
        
        doc = await db.bmp_tables.find_one(query)  # Fixed: bmp_tables
        if not doc:
            logger.warning("No BMP table found for year %s", reg_year)
            return None
        
        # Find the emissions bracket
        for entry in doc["entries"]:
            lower = entry["lower"]
            upper = entry["upper"]
            
            # Handle infinity upper bound
            if isinstance(upper, dict) and "$numberDouble" in upper:
                upper = float('inf')
            
            if lower <= raw_emissions < upper:
                return entry
        
        logger.warning("No BMP entry found for emissions %s in year %s", raw_emissions, reg_year)
        return None
        
    except Exception as ex:
        logger.error("Error getting BMP entry: %s", ex)
        return None

# --- Mongo PHEV Lookup ---
async def get_phev_entry(db: AsyncIOMotorDatabase, reg_year: int, raw_emissions: float, reg_month: int) -> Optional[dict]:
    """
    Get PHEV entry from MongoDB lookup tables for hybrid vehicles.
    """
    try:
        query = {"year": reg_year}
        
        # Handle 2020 half-year split
        if reg_year == 2020:
            query["half"] = "H1" if reg_month < 7 else "H2"
        
        doc = await db.phev_tables.find_one(query)
        if not doc:
            logger.warning("No PHEV table found for year %s", reg_year)
            return None
        
        # Find the emissions bracket
        for entry in doc["entries"]:
            lower = entry["lower"]
            upper = entry["upper"]
            
            # Handle infinity upper bound
            if isinstance(upper, dict) and "$numberDouble" in upper:
                upper = float('inf')
            
            if lower <= raw_emissions < upper:
                return entry
        
        logger.warning("No PHEV entry found for emissions %s in year %s", raw_emissions, reg_year)
        return None
        
    except Exception as ex:
        logger.error("Error getting PHEV entry: %s", ex)
        return None

# --- Mongo Diesel Surcharge Lookup ---
async def get_diesel_surcharge(db: AsyncIOMotorDatabase, reg_year: int) -> Optional[dict]:
    """
    Get diesel surcharge data for a specific registration year.
    """
    try:
        doc = await db.diesel_surcharges.find_one({"year": reg_year})
        if not doc:
            logger.debug("No diesel surcharge data found for year %s", reg_year)
        return doc
        
    except Exception as ex:
        logger.error("Error getting diesel surcharge: %s", ex)
        return None

# --- Normalize Gallery ---
def normalize_gallery(images: Union[str, List[str]]) -> List[str]:
    """
    Normalize the gallery by converting various image formats into a list of URLs.
    Handles both string and list inputs with various separators.
    """
    if isinstance(images, list):
        return [url.strip() for url in images if url and url.strip()]
    
    if isinstance(images, str) and images.strip():
        # Try different separators
        if "|" in images:
            return [url.strip() for url in images.split("|") if url.strip()]
        elif "," in images:
            return [url.strip() for url in images.split(",") if url.strip()]
        else:
            return [images.strip()]
    
    return []

# --- Collection Name Helpers ---
def get_processed_collection_name(site: str) -> str:
    """Get the processed collection name for a site."""
    return f"processed_{site}"

def get_queue_collection_name(site: str) -> str:
    """Get the queue collection name for a site."""
    return f"wp_sync_queue_{site}"

# --- Validation Helpers ---
def is_valid_price(price: Any) -> bool:
    """Check if a price value is valid."""
    try:
        return float(price) > 0
    except (ValueError, TypeError):
        return False

def is_valid_year(year: Any) -> bool:
    """Check if a year value is valid."""
    try:
        year_int = int(year)
        return 2000 <= year_int <= 2030
    except (ValueError, TypeError):
        return False

def is_valid_mileage(mileage: Any) -> bool:
    """Check if a mileage value is valid."""
    try:
        return 0 <= int(mileage) <= 500000
    except (ValueError, TypeError):
        return False