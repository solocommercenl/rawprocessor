"""
site_settings.py
----------------
Async module to load, validate, and expose per-site settings for the rawprocessor middleware.

Responsibilities:
- Load per-site config (filters, financials, translation_profile, WP root, etc.) from MongoDB.
- Validate structure and required fields.
- Expose as an async class with optional in-memory cache.
- Raise/log clear errors on missing or invalid configs.

FIXED: Added cache clearing functionality and loan_term_months to required fields.
"""

from typing import Any, Dict, Optional
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

REQUIRED_FIELDS = [
    "site_url",
    "wp_root_path",
    "licence_plate_fee",
    "rdw_inspection",
    "transport_cost",
    "unforeseen_percentage",
    "annual_interest_rate",
    "loan_term_months",  # FIXED: Added to required fields
    "filter_criteria",
    "translation_profile"
]

class SiteSettingsError(Exception):
    """Custom error for settings failures."""

class SiteSettings:
    """
    Loads and caches per-site settings from MongoDB (site_settings collection).
    Usage:
        settings = await SiteSettings(db).get(site_key)
    """
    _cache: Dict[str, Dict[str, Any]] = {}

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def get(self, site_key: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Fetch settings for a given site_key (e.g. 'solostaging'), with optional in-memory cache.
        :param site_key: The domain/unique key for the site (e.g. "solostaging" or domain).
        :param use_cache: If True, uses in-memory cache (per process).
        """
        cache_key = site_key.lower()
        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]
        
        # Find on "site_url" field, supporting both exact and regex/substring matching
        doc = await self.db.site_settings.find_one({
            "$or": [
                {"site_url": site_key},
                {"site_url": {"$regex": site_key, "$options": "i"}}
            ]
        })
        if not doc:
            logger.error(f"Site settings not found for site_key: {site_key}")
            raise SiteSettingsError(f"Settings not found for site: {site_key}")

        missing = [field for field in REQUIRED_FIELDS if field not in doc]
        if missing:
            logger.error(f"Missing fields in settings for {site_key}: {missing}")
            raise SiteSettingsError(f"Settings for {site_key} missing required fields: {missing}")

        filter_criteria = doc["filter_criteria"]
        if not isinstance(filter_criteria, dict):
            logger.error(f"filter_criteria must be a dict for {site_key}")
            raise SiteSettingsError(f"filter_criteria invalid in {site_key} settings")

        if "price_margins" in doc and not isinstance(doc["price_margins"], list):
            logger.warning(f"price_margins should be a list in {site_key}")

        logger.info(f"Site settings loaded for {site_key}")
        if use_cache:
            self._cache[cache_key] = doc
        return doc

    @classmethod
    def clear_cache(cls, site_key: str = None):
        """
        Clear cache for specific site or all sites.
        
        :param site_key: Site key to clear cache for. If None, clears all cache.
        """
        if site_key:
            cache_key = site_key.lower()
            if cache_key in cls._cache:
                del cls._cache[cache_key]
                logger.info(f"Cleared site settings cache for {site_key}")
            else:
                logger.debug(f"No cache entry found for {site_key}")
        else:
            cache_count = len(cls._cache)
            cls._cache.clear()
            logger.info(f"Cleared all site settings cache ({cache_count} entries)")

    @classmethod
    def get_cache_info(cls) -> Dict[str, Any]:
        """
        Get information about current cache state for debugging.
        
        :return: Dictionary with cache statistics
        """
        return {
            "cache_size": len(cls._cache),
            "cached_sites": list(cls._cache.keys()),
            "memory_usage_estimate": sum(len(str(v)) for v in cls._cache.values())
        }