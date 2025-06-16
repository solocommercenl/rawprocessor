from typing import Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from utils import normalize_gallery  # Ensure normalize_gallery is imported

class Cleaner:
    def __init__(self, db: AsyncIOMotorDatabase, site: str):
        """
        Initializes the Cleaner with the database and site name.
        :param db: MongoDB database instance.
        :param site: Site name to prefix log messages.
        """
        self.db = db
        self.site = site

    async def clean_raw_record(
        self, record: Dict[str, Any], log_prefix: str = ""
    ) -> Optional[Dict[str, Any]]:
        """
        Clean and validate a raw scraped record according to real raw field structure.
        :param record: Raw record (dict) from MongoDB.
        :param log_prefix: String prefix for log messages (e.g. site name).
        :return: Cleaned record (dict) if valid, None if excluded.
        """
        reasons = []

        # 1. Fueltype must be "Electric" for raw_emissions to be allowed as 0
        fuel = record.get("Fueltype", "").strip()
        raw_emissions = record.get("raw_emissions")

        # Debugging the raw_emissions value
        logger.info(f"{log_prefix} Checking raw_emissions value: {raw_emissions} | _id={record.get('_id')}")

        # Ensure raw_emissions is a valid number (it should always be a number)
        emissions_value = raw_emissions  # raw_emissions is always a number

        # 2. Check if raw_emissions is 0 and Fueltype is not "Electric"
        if emissions_value == 0:
            if "Electric" not in fuel:  # If it's not Electric, delete the record
                reasons.append("Invalid raw_emissions value (0)")

        # 3. Images count must be >= 4
        images = normalize_gallery(record.get("Images", []))  # Normalize the gallery (image URLs)
        if len(images) < 4:
            reasons.append("Insufficient images (less than 4)")

        if reasons:
            logger.warning(f"{log_prefix} Record excluded: {', '.join(reasons)} | _id={record.get('_id')}")

            # Delete invalid records from raw collection
            await self.db.raw.delete_one({"_id": record.get("_id")})  # Deleting the invalid record

            return None

        record["Images"] = images  # cleaned gallery
        return record
