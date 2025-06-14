"""
cleaner.py
----------
Module for cleaning and validating raw vehicle data for the rawprocessor middleware.

Responsibilities:
- Clean/validate each raw record (dict) per project rules.
- Enforce required fields (price, reg. year/month, emissions unless electric, >=4 images if non-electric+missing emissions).
- Log all reasons for exclusion/skipping.
- Expose: `clean_raw_record(record, log_prefix) -> Union[Dict, None]`
- Designed for async batch processing.

Dependencies:
    - loguru (logging)
"""

from typing import Dict, Any, Optional, List, Tuple, Union
from loguru import logger

REQUIRED_FIELDS = [
    "im_price_org", "im_registration_year", "im_first_registration",
    "im_fuel_type", "im_raw_emissions", "im_gallery"
]

def is_electric(fuel_type: Optional[str]) -> bool:
    if not fuel_type:
        return False
    return fuel_type.lower().strip() in {"electric", "elektrisch"}

def parse_gallery(gallery: Union[str, List[str]]) -> List[str]:
    """Normalize gallery field to list of URLs (comma or pipe separated)."""
    if isinstance(gallery, list):
        return gallery
    if isinstance(gallery, str):
        if "|" in gallery:
            return [url.strip() for url in gallery.split("|") if url.strip()]
        elif "," in gallery:
            return [url.strip() for url in gallery.split(",") if url.strip()]
        else:
            return [gallery.strip()] if gallery.strip() else []
    return []

def clean_raw_record(
    record: Dict[str, Any],
    log_prefix: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Clean and validate a raw scraped record according to project rules.
    :param record: Raw record (dict) from MongoDB.
    :param log_prefix: String prefix for log messages (e.g. site name).
    :return: Cleaned record (dict) if valid, None if excluded.
    """
    reasons = []

    # 1. Must have original price
    if "im_price_org" not in record or not record["im_price_org"]:
        reasons.append("Missing im_price_org (original price)")

    # 2. Registration year and month required
    if not record.get("im_registration_year"):
        reasons.append("Missing im_registration_year")
    if not record.get("im_first_registration"):
        reasons.append("Missing im_first_registration")

    # 3. Must have fuel type (non-electric: must have emissions or >=4 images)
    fuel_type = record.get("im_fuel_type") or ""
    electric = is_electric(fuel_type)
    emissions = record.get("im_raw_emissions")
    gallery = parse_gallery(record.get("im_gallery", []))

    if not electric:
        # Not electric: must have emissions or >=4 images
        if not emissions:
            if len(gallery) < 4:
                reasons.append(
                    "Non-electric car missing emissions and has less than 4 images"
                )
    # 4. Images field must be present and parsable
    if not gallery:
        reasons.append("No images found in gallery field")

    # If any reason to skip, log and return None
    if reasons:
        logger.warning(
            f"{log_prefix} Record excluded: {', '.join(reasons)} | _id={record.get('_id')}"
        )
        return None

    # Clean up gallery field to list for downstream processing
    record["im_gallery"] = gallery

    # If passed, return record
    return record