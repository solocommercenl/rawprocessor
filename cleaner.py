from typing import Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase  # Ensure AsyncIOMotorDatabase is imported
from loguru import logger
from utils import normalize_gallery  # Ensure normalize_gallery is imported

async def clean_raw_record(
    record: Dict[str, Any],
    db: AsyncIOMotorDatabase,  # Ensure db parameter is recognized
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
    raw_emissions = record.get("raw_emissions")
    
    # Debugging the raw_emissions value
    logger.info(f"{log_prefix} Checking raw_emissions value: {raw_emissions} | _id={record.get('_id')}")

    # Validate that raw_emissions is a number (either 0 or positive)
    emissions_value = raw_emissions  # raw_emissions is always a number
    
    # Check if raw_emissions is invalid (0 or empty) and Fueltype is not "Electric"
    if emissions_value == 0 or emissions_value is None:  # Only exclude if emissions is 0 or empty
        if "Electric" not in fuel:  # If it's not Electric, delete the record
            reasons.append("Invalid raw_emissions value (0 or empty)")

    # 2. Images count must be >= 4
    images = normalize_gallery(record.get("Images", []))  # Normalize the gallery (image URLs)
    if len(images) < 4:
        reasons.append("Insufficient images (less than 4)")

    if reasons:
        logger.warning(f"{log_prefix} Record excluded: {', '.join(reasons)} | _id={record.get('_id')}")

        # Delete invalid records from raw collection
        await db.raw.delete_one({"_id": record.get("_id")})  # Deleting the invalid record

        return None

    record["Images"] = images  # cleaned gallery
    return record
