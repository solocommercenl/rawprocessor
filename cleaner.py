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

# Test stub
def _test():
    """
    Test stub for cleaner.py
    """
    # 1. Complete non-electric record (should pass)
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": 122,
        "im_gallery": "url1|url2|url3|url4"
    }
    assert clean_raw_record(rec, "TEST") is not None

    # 2. Missing price (should exclude)
    rec2 = rec.copy()
    del rec2["im_price_org"]
    assert clean_raw_record(rec2, "TEST") is None

    # 3. Non-electric missing emissions, <4 images (should exclude)
    rec3 = rec.copy()
    rec3["im_raw_emissions"] = None
    rec3["im_gallery"] = "url1|url2|url3"
    assert clean_raw_record(rec3, "TEST") is None

    # 4. Electric with missing emissions, <4 images (should pass)
    rec4 = rec.copy()
    rec4["im_fuel_type"] = "electric"
    rec4["im_raw_emissions"] = None
    rec4["im_gallery"] = "url1|url2"
    assert clean_raw_record(rec4, "TEST") is not None

    logger.success("cleaner.py test passed.")

if __name__ == "__main__":
    _test()