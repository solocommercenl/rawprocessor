from typing import Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from utils import normalize_gallery

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
        Clean and validate a raw scraped record according to actual raw field structure.
        :param record: Raw record (dict) from MongoDB.
        :param log_prefix: String prefix for log messages (e.g. site name).
        :return: Cleaned record (dict) if valid, None if excluded.
        """
        reasons = []

        # Get the actual record ID for logging
        record_id = record.get("_id") or record.get("car_id") or "unknown"

        # 1. Extract fuel type from correct nested path
        fuel_type = record.get("energyconsumption", {}).get("Fueltype", "").strip()
        raw_emissions = record.get("energyconsumption", {}).get("raw_emissions")

        # Log what we found
        logger.debug(f"{log_prefix} Checking fuel_type='{fuel_type}', raw_emissions={raw_emissions} | record_id={record_id}")

        # 2. Validate raw_emissions - must be a number
        if raw_emissions is None:
            reasons.append("Missing raw_emissions value")
        else:
            try:
                emissions_value = float(raw_emissions)
                
                # 3. Check if raw_emissions is 0 and fuel type is not Electric
                if emissions_value == 0 and "Electric" not in fuel_type:
                    reasons.append("Invalid raw_emissions value (0 for non-electric vehicle)")
                    
            except (ValueError, TypeError):
                reasons.append(f"Invalid raw_emissions value (not a number): {raw_emissions}")

        # 4. Check required fields exist
        required_fields = ["price", "registration", "brand", "model"]
        for field in required_fields:
            if not record.get(field):
                reasons.append(f"Missing required field: {field}")

        # 5. Images count must be >= 4
        images = normalize_gallery(record.get("Images", []))
        if len(images) < 4:
            reasons.append(f"Insufficient images ({len(images)}, need >= 4)")

        # 6. Validate price is a positive number
        try:
            price = float(record.get("price", 0))
            if price <= 0:
                reasons.append("Invalid price (must be > 0)")
        except (ValueError, TypeError):
            reasons.append(f"Invalid price value: {record.get('price')}")

        # 7. Validate registration year
        reg_year = record.get("registration_year")
        if reg_year:
            try:
                year = int(reg_year)
                current_year = 2025  # Current year based on your data
                if year < 2000 or year > current_year:
                    reasons.append(f"Invalid registration year: {year}")
            except (ValueError, TypeError):
                reasons.append(f"Invalid registration year format: {reg_year}")

        # If validation failed, log and return None (don't delete automatically)
        if reasons:
            logger.warning(f"{log_prefix} Record validation failed: {', '.join(reasons)} | record_id={record_id}")
            return None

        # Clean and normalize the record
        record["Images"] = images  # Normalized gallery
        
        # Add derived fields for easier access downstream
        record["_fuel_type"] = fuel_type
        record["_raw_emissions"] = float(raw_emissions)
        record["_price"] = float(record.get("price", 0))
        
        logger.debug(f"{log_prefix} Record cleaned successfully | record_id={record_id}")
        return record

    async def is_record_processable_for_site(
        self, record: Dict[str, Any], site_filters: Dict[str, Any], log_prefix: str = ""
    ) -> bool:
        """
        Check if a cleaned record should be processed for a specific site based on site filters.
        :param record: Cleaned raw record.
        :param site_filters: Filter criteria from site_settings.
        :param log_prefix: String prefix for log messages.
        :return: True if record should be processed for this site.
        """
        record_id = record.get("_id") or record.get("car_id") or "unknown"
        
        try:
            # Check price range
            price = record.get("_price", 0)
            price_filter = site_filters.get("im_price_org", {})
            if isinstance(price_filter, dict):
                min_price = price_filter.get("$gte", 0)
                max_price = price_filter.get("$lte", float('inf'))
                if not (min_price <= price <= max_price):
                    logger.debug(f"{log_prefix} Price filter failed: {price} not in [{min_price}, {max_price}] | record_id={record_id}")
                    return False

            # Check registration year
            reg_year = record.get("registration_year")
            if reg_year:
                try:
                    year = int(reg_year)
                    year_filter = site_filters.get("im_registration_year", {})
                    if isinstance(year_filter, dict):
                        min_year = year_filter.get("$gte", 0)
                        if year < min_year:
                            logger.debug(f"{log_prefix} Year filter failed: {year} < {min_year} | record_id={record_id}")
                            return False
                except (ValueError, TypeError):
                    pass

            # Check mileage
            mileage = record.get("milage") or record.get("mileage") or 0
            try:
                mileage = int(mileage)
                mileage_filter = site_filters.get("im_mileage", {})
                if isinstance(mileage_filter, dict):
                    max_mileage = mileage_filter.get("$lte", float('inf'))
                    if mileage > max_mileage:
                        logger.debug(f"{log_prefix} Mileage filter failed: {mileage} > {max_mileage} | record_id={record_id}")
                        return False
            except (ValueError, TypeError):
                pass

            # Check excluded makes
            make = record.get("brand", "")
            excluded_makes = site_filters.get("make", {}).get("$nin", [])
            if make in excluded_makes:
                logger.debug(f"{log_prefix} Make excluded: {make} | record_id={record_id}")
                return False

            # Check excluded models  
            model = record.get("model", "")
            excluded_models = site_filters.get("model", {}).get("$nin", [])
            if model in excluded_models:
                logger.debug(f"{log_prefix} Model excluded: {model} | record_id={record_id}")
                return False

            # Check car type
            car_type = record.get("Basicdata", {}).get("Type", "")
            allowed_types = site_filters.get("cartype", ["Car"])
            if car_type and car_type not in allowed_types:
                logger.debug(f"{log_prefix} Car type not allowed: {car_type} | record_id={record_id}")
                return False

            # Check full service history if required
            if site_filters.get("im_fullservicehistory") == "true":
                has_history = record.get("vehiclehistory", {}).get("Fullservicehistory", False)
                if not has_history:
                    logger.debug(f"{log_prefix} Full service history required but missing | record_id={record_id}")
                    return False

            logger.debug(f"{log_prefix} Record passed all site filters | record_id={record_id}")
            return True

        except Exception as ex:
            logger.error(f"{log_prefix} Error checking site filters: {ex} | record_id={record_id}")
            return False