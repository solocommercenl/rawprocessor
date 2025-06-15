"""
cleaner.py
----------
Module for cleaning and validating raw vehicle data for the rawprocessor middleware.

Responsibilities:
- Clean/validate each raw record (dict) per project rules.
- Normalize registration format and gallery/images.
- Validate raw fields (price, registration, emissions, image count, Fueltype, Equipment).
- Expose: `clean_raw_record(record, log_prefix) -> Union[Dict, None]`

Dependencies:
    - loguru (logging)
"""

from typing import Dict, Any, Optional, List, Union
from loguru import logger

REQUIRED_FIELDS = ["price", "registration", "Fueltype", "raw_emissions", "Images"]

def is_electric(fuel_type: Optional[str]) -> bool:
    if not fuel_type:
        return False
    return fuel_type.strip().lower() in {"electric", "elektrisch"}

def normalize_gallery(images: Union[str, List[str]]) -> List[str]:
    if isinstance(images, list):
        return [url.strip() for url in images if url.strip()]
    if isinstance(images, str):
        if "|" in images:
            return [url.strip() for url in images.split("|") if url.strip()]
        elif "," in images:
            return [url.strip() for url in images.split(",") if url.strip()]
        return [images.strip()] if images.strip() else []
    return []

def clean_raw_record(
    record: Dict[str, Any],
    log_prefix: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Clean and validate a raw scraped record according to real raw field structure.
    :param record: Raw record (dict) from MongoDB.
    :param log_prefix: String prefix for log messages (e.g. site name).
    :return: Cleaned record (dict) if valid, None if excluded.
    """
    reasons = []

    # 1. Price must exist and be > 0
    if not record.get("price"):
        reasons.append("Missing or invalid price")

    # 2. Registration string must be present (we don't parse here, just require it)
    if not record.get("registration"):
        reasons.append("Missing registration")

    # 3. Fueltype must be known, and emissions must exist if not electric
    fuel = record.get("Fueltype", "").strip()
    electric = is_electric(fuel)
    emissions = record.get("raw_emissions")
    images = normalize_gallery(record.get("Images", []))

    if not electric:
        if emissions in [None, ""]:
            if len(images) < 4:
                reasons.append("Non-electric car missing emissions and has less than 4 images")

    if not images:
        reasons.append("No images in gallery")

    if reasons:
        logger.warning(f"{log_prefix} Record excluded: {', '.join(reasons)} | _id={record.get('_id')}")
        return None

    record["Images"] = images  # cleaned gallery
    return record
