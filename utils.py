"""
utils.py

Shared helpers for rawprocessor (date parsing, Mongo lookups, etc).

All helpers are modular, async-ready, robust, and documented.
"""

from typing import Tuple, Optional
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase

import logging

logger = logging.getLogger("rawprocessor.utils")


def parse_registration_date(registration_date: Optional[str], registration_year: Optional[int]) -> Tuple[int, int]:
    """
    Parse registration month/year from fields or fallback.

    Returns:
        (month, year)
    """
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


def calculate_age_in_months(today: datetime, reg_month: int, reg_year: int) -> int:
    """
    Returns the age in months between registration and today.
    """
    if reg_year == 0:
        return 0
    months = (today.year - reg_year) * 12 + (today.month - reg_month)
    if months < 0:
        months = 0
    return months


async def get_depreciation_percentage(db: AsyncIOMotorDatabase, age_in_months: int) -> float:
    """
    Looks up the depreciation percentage (base + monthly*months) for the given vehicle age.
    """
    doc = await db.depreciation_table.find_one({
        "start": {"$lte": age_in_months},
        "end": {"$gt": age_in_months}
    })
    if not doc:
        return 0.0
    base = doc["base_percent"]
    monthly = doc["monthly_percent"]
    percentage = base + (age_in_months - doc["start"]) * monthly
    return percentage


async def get_bpm_entry(
    db: AsyncIOMotorDatabase, reg_year: int, raw_emissions: float, reg_month: int, fuel_type: str
) -> Optional[dict]:
    """
    Finds the BPM bracket for the given registration year and emissions (and month for 2020+).
    """
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


async def get_phev_entry(
    db: AsyncIOMotorDatabase, reg_year: int, raw_emissions: float, reg_month: int
) -> Optional[dict]:
    """
    Finds the PHEV bracket for the given registration year and emissions (and month for 2020+).
    """
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


async def get_diesel_surcharge(
    db: AsyncIOMotorDatabase, reg_year: int
) -> Optional[dict]:
    """
    Gets the diesel surcharge for a registration year.
    """
    return await db.diesel_surcharges.find_one({"year": reg_year})
