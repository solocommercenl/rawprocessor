"""
site_settings.py
----------------
Async module to load, validate, and expose per-site settings for the rawprocessor middleware.

Responsibilities:
- Load per-site config (filters, financials, translation_profile, WP root, etc.) from MongoDB.
- Validate structure and required fields.
- Expose as an async singleton or per-site cache.
- Raise/log clear errors on missing or invalid configs.

Dependencies:
    - motor (async MongoDB)
    - loguru (logging)
    - os, dotenv (for DB connection)
"""

import os
from typing import Any, Dict, Optional, List, Union
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from dotenv import load_dotenv

# Load .env variables for MongoDB connection
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "autodex")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set in .env")

client = AsyncIOMotorClient(MONGO_URI)
db: AsyncIOMotorDatabase = client[MONGO_DB]

REQUIRED_FIELDS = [
    "site_url",
    "wp_root_path",
    "licence_plate_fee",
    "rdw_inspection",
    "transport_cost",
    "unforeseen_percentage",
    "annual_interest_rate",
    "filter_criteria",
    "translation_profile"
]

class SiteSettingsError(Exception):
    """Custom error for settings failures."""

async def fetch_site_settings(site_url: str) -> Dict[str, Any]:
    """
    Fetch and validate settings for a specific site from the site_settings collection.
    :param site_url: The domain/URL of the site to fetch settings for.
    :raises SiteSettingsError: On missing or invalid settings.
    :return: Settings dict.
    """
    logger.debug(f"Loading site settings for {site_url}")
    doc = await db.site_settings.find_one({"site_url": site_url})
    if not doc:
        logger.error(f"Site settings not found for {site_url}")
        raise SiteSettingsError(f"Settings not found for site: {site_url}")

    missing = [field for field in REQUIRED_FIELDS if field not in doc]
    if missing:
        logger.error(f"Missing fields in settings for {site_url}: {missing}")
        raise SiteSettingsError(f"Settings for {site_url} missing required fields: {missing}")

    # Optionally validate filter_criteria structure
    filter_criteria = doc["filter_criteria"]
    if not isinstance(filter_criteria, dict):
        logger.error(f"filter_criteria must be a dict for {site_url}")
        raise SiteSettingsError(f"filter_criteria invalid in {site_url} settings")

    # Validate price_margins if present
    if "price_margins" in doc and not isinstance(doc["price_margins"], list):
        logger.warning(f"price_margins should be a list in {site_url}")

    logger.info(f"Site settings loaded for {site_url}")
    return doc

# Optionally: Simple in-memory cache for site settings (to avoid repeat lookups during batch runs)
_site_settings_cache: Dict[str, Dict[str, Any]] = {}

async def get_site_settings(site_url: str, use_cache: bool = True) -> Dict[str, Any]:
    """
    Get (optionally cached) settings for a site.
    :param site_url: Site domain/URL.
    :param use_cache: Use in-memory cache if available.
    :return: Site settings dict.
    """
    if use_cache and site_url in _site_settings_cache:
        return _site_settings_cache[site_url]
    settings = await fetch_site_settings(site_url)
    if use_cache:
        _site_settings_cache[site_url] = settings
    return settings

# Test stub
async def _test():
    """
    Test stub for site_settings.py.
    Set up a test document for 'solostaging.nl' and validate retrieval/fields.
    """
    import asyncio
    try:
        settings = await get_site_settings("solostaging.nl", use_cache=False)
        assert isinstance(settings, dict)
        assert all(f in settings for f in REQUIRED_FIELDS)
        logger.success("site_settings.py test passed.")
    except Exception as e:
        logger.exception(f"site_settings.py test failed: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(_test())
