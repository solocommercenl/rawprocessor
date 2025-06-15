from typing import Dict, Any, Optional
from loguru import logger

async def clean_raw_record(
    record: Dict[str, Any],
    db: AsyncIOMotorDatabase,  # Add db parameter to delete from MongoDB
    log_prefix: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Clean and validate a raw scraped record according to real raw field structure.
    :param record: Raw record (dict) from MongoDB.
    :param db: Database instance to delete the invalid record from MongoDB.
    :param log_prefix: String prefix for log messages (e.g. site name).
    :return: Cleaned record (dict) if valid, None if excluded.
    """
    reasons = []

    # 1. Fueltype must be "Electric" for raw_emissions to be allowed as 0 or null
    fuel = record.get("Fueltype", "").strip()
    emissions = record.get("raw_emissions")
    
    # Check if raw_emissions is invalid (0, null, or empty) and Fueltype is not "Electric"
    if emissions in [None, ""] or emissions == 0:
        if fuel != "Electric":  # If it's not Electric, delete the record
            reasons.append("Invalid raw_emissions value (0, null, or empty)")

    # 2. Images count must be >= 4
    images = normalize_gallery(record.get("Images", []))
    if len(images) < 4:
        reasons.append("Insufficient images (less than 4)")

    if reasons:
        logger.warning(f"{log_prefix} Record excluded: {', '.join(reasons)} | _id={record.get('_id')}")

        # Delete invalid records from raw collection
        await db.raw.delete_one({"_id": record.get("_id")})  # Deleting the invalid record

        return None

    record["Images"] = images  # cleaned gallery
    return record
