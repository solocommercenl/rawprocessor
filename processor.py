import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from translator import Translator
from calculator import Calculator
from site_settings import SiteSettings  
from jobqueue import WPQueue
from utils import (
    calculate_hash_groups, normalize_gallery, extract_power_values, 
    extract_numeric_value
)
from utils_filters import is_record_clean, check_raw_against_filters

class Processor:
    def __init__(self, db: AsyncIOMotorDatabase, site: str):
        self.db = db
        self.site = site
        self.translator = Translator(db)
        self.calculator: Optional[Calculator] = None
        self.processed_collection = db[f"processed_{site}"]
        self.queue_collection = db[f"wp_sync_queue_{site}"]

    async def run(self):
        """
        Process all raw records for this site (used for rebuild operations).
        Uses proper filtering with utils_filters instead of MongoDB query filters.
        """
        settings = await SiteSettings(self.db).get(self.site)
        self.calculator = Calculator(self.db, settings)
        
        # Get ALL active records, apply filters in code
        cursor = self.db.raw.find({"listing_status": True})
        processed_count = 0
        skipped_count = 0

        logger.info(f"[{self.site}] Starting bulk processing of raw records")

        async for raw in cursor:
            try:
                processed = await self.process_single_record(raw, settings)
                if processed:
                    processed_count += 1
                    logger.debug(f"[{self.site}] Processed record {processed['im_ad_id']}")
                else:
                    skipped_count += 1
                    
            except Exception as ex:
                logger.error(f"[{self.site}] Error processing record {raw.get('_id')}: {ex}")
                skipped_count += 1

        logger.info(f"[{self.site}] Bulk processing complete: {processed_count} processed, {skipped_count} skipped")

    async def process_single_record(self, raw: dict, site_settings: dict) -> Optional[dict]:
        """
        Process a single raw record through the complete pipeline.
        Uses utils_filters for fast validation instead of cleaner.
        """
        record_id = raw.get("_id") or raw.get("car_id") or "unknown" 
        
        try:
            # Step 1: Fast basic quality check using utils_filters
            if not is_record_clean(raw):
                logger.debug(f"[{self.site}] Skipping dirty record: {record_id}")
                return None

            # Step 2: Check if record should be processed for this site using utils_filters
            site_filters = site_settings.get("filter_criteria", {})
            if not check_raw_against_filters(raw, site_filters):
                logger.debug(f"[{self.site}] Record excluded by site filters: {record_id}")
                return None

            # Step 3: Process the clean, valid record
            processed = await self.process(raw, site_settings)
            if not processed:
                logger.debug(f"[{self.site}] Processing returned None: {record_id}")
                return None

            # Step 4: Store in processed collection and queue sync if needed
            await self._store_and_queue_if_changed(processed)
            
            return processed

        except Exception as ex:
            logger.error(f"[{self.site}] Error in process_single_record for {record_id}: {ex}")
            return None

    async def process(self, raw: dict, site_settings: dict) -> Optional[dict]:
        """
        Transform a cleaned raw record into a processed record ready for WordPress.
        """
        if self.calculator is None:
            self.calculator = Calculator(self.db, site_settings)

        record_id = raw.get("_id") or raw.get("car_id") or "unknown"

        try:
            # Step 1: Translate fields
            translated = await self.translator.translate_fields(raw, site_settings, record_id, self.site)
            
            # Step 2: Calculate financials 
            vated = bool(raw.get("vatded", False))  # vatded field from raw
            financials = await self.calculator.calculate_financials(raw, vated)

            # Step 3: Build the processed document matching your target structure
            doc = await self._build_processed_document(raw, translated, financials)
            
            # Step 4: Check if this is a new record
            existing = await self.processed_collection.find_one({"im_ad_id": doc["im_ad_id"]})
            doc["_is_new"] = not bool(existing)
            
            # Step 5: Calculate hash groups for change detection
            doc["hashes"] = calculate_hash_groups(doc)
            
            logger.debug(f"[{self.site}] Successfully processed record: {record_id}")
            return doc

        except Exception as ex:
            logger.error(f"[{self.site}] Error processing record {record_id}: {ex}")
            return None

    async def _build_processed_document(self, raw: dict, translated: dict, financials: dict) -> dict:
        """
        Build the complete processed document matching the target structure.
        FIXED: JetEngine CLI expects arrays for both checkboxes AND gallery fields.
        """
        doc: Dict[str, Any] = {}

        # === CORE IDENTIFIERS ===
        doc["im_ad_id"] = raw.get("car_id", "")
        
        # === TITLE (Brand + Model + Title) ===
        make = raw.get("brand", "")
        model = raw.get("model", "")  
        title_suffix = raw.get("title", "")
        doc["im_title"] = f"{make} {model} {title_suffix}".strip()
        
        # === GALLERY AND FEATURED IMAGE ===
        images = normalize_gallery(raw.get("Images", []))
        # FIXED: JetEngine gallery field expects array, not comma-separated string
        doc["im_gallery"] = images  # Keep as array for JetEngine
        doc["im_featured_image"] = images[0] if images else ""
        
        # === BASIC VEHICLE DATA ===
        doc["im_price_org"] = round(float(raw.get("price", 0)), 2)
        # registration_year should be integer
        doc["im_registration_year"] = int(raw.get("registration_year", 0) or 0)
        doc["im_first_registration"] = raw.get("registration", "")
        # Handle both 'milage' (typo in source) and 'mileage'
        doc["im_mileage"] = int(raw.get("milage") or raw.get("mileage") or 0)
        doc["im_power"] = raw.get("power", "")
        
        # === SERVICE HISTORY - FIXED: Convert to Dutch Ja/Nee ===
        fullservicehistory = bool(raw.get("vehiclehistory", {}).get("Fullservicehistory", False))
        doc["im_fullservicehistory"] = "Ja" if fullservicehistory else "Nee"
        
        # === POWER EXTRACTION ===
        kw_power, hp_power = extract_power_values(raw.get("power", ""))
        doc["im_kw_power"] = kw_power or 0
        doc["im_hp_power"] = hp_power or 0
        
        # === TECHNICAL DATA ===
        technical_data = raw.get("TechnicalData", {})
        basicdata = raw.get("Basicdata", {})
        
        doc["im_vehicle_type"] = "car"  # Default as per WP data
        doc["im_cylinders"] = str(technical_data.get("Cylinders", ""))
        doc["im_doors"] = str(basicdata.get("Doors", ""))
        doc["im_drivetrain"] = basicdata.get("Drivetrain", "") or ""
        
        # Extract numeric values from formatted strings
        empty_weight = extract_numeric_value(technical_data.get("Emptyweight", ""))
        doc["im_empty_weight"] = str(int(empty_weight)) if empty_weight else ""
        
        engine_size = extract_numeric_value(technical_data.get("Enginesize", ""))
        doc["im_engine_size"] = str(int(engine_size)) if engine_size else ""
        
        # Emissions formatting
        emissions_data = raw.get("energyconsumption", {})
        emissions_str = emissions_data.get("emissions", "")
        if emissions_str:
            doc["im_emissions"] = emissions_str
        else:
            raw_emissions = emissions_data.get("raw_emissions")
            if raw_emissions:
                doc["im_emissions"] = f"{raw_emissions}gram comb"
            else:
                doc["im_emissions"] = ""
        
        # === ADDITIONAL MISSING FIELDS ===
        vehicle_history = raw.get("vehiclehistory", {})
        doc["im_fuel_consumption"] = ""  # Not available in raw data
        doc["im_modelcode"] = basicdata.get("Modelcode", "") or ""
        doc["im_previousowner"] = str(vehicle_history.get("Previousowner", ""))
        
        # === APPEARANCE DATA ===
        colour_data = raw.get("colourandupholstery", {})
        doc["im_upholstery"] = colour_data.get("Upholstery", "")
        doc["im_upholstery_colour"] = ""  # Not available in raw data structure
        doc["im_manufacturer_color"] = colour_data.get("Manufacturercolour", "")
        doc["im_paint_type"] = colour_data.get("Paint", "")
        
        # === DEALER DATA ===  
        doc["im_dealer_company"] = raw.get("dealer_company", "")
        doc["im_dealer_contact"] = raw.get("dealer_contact", "")
        doc["im_dealer_phone"] = raw.get("dealer_phone", "")
        doc["im_dealer_city"] = raw.get("dealer_city", "")
        # Changed im_product_url to im_autoscout
        doc["im_autoscout"] = raw.get("Product_URL", "")
        
        # === VEHICLE SPECIFICATIONS ===
        doc["im_seats"] = int(basicdata.get("Seats", 0) or 0)
        doc["im_body_type"] = basicdata.get("Body", "")
        doc["im_gearbox"] = raw.get("gearbox", "")
        doc["im_make_model"] = f"{make} {model}".strip()
        
        # === EMISSIONS AND VAT ===
        doc["im_raw_emissions"] = raw.get("energyconsumption", {}).get("raw_emissions")
        
        # === VAT DEDUCTIBLE - FIXED: Convert to Dutch text values ===
        vatded = bool(raw.get("vatded", False))
        doc["im_vat_deductible"] = "BTW auto" if vatded else "Marge auto"
        
        # === VAT FILTER - For filtering/taxonomy purposes ===
        doc["im_vat_filter"] = "btw" if vatded else "marge"
        
        # === TAXONOMIES (for WordPress) ===
        doc["make"] = make
        doc["model"] = model  
        doc["color"] = colour_data.get("Colour", "")
        
        # === STATUS AND TIMESTAMPS ===
        doc["im_status"] = "True"  # JetEngine expects string
        doc["updated_at"] = datetime.utcnow()
        
        # === ADD TRANSLATED FIELDS AS ARRAYS (NOT SERIALIZED) ===
        for key, value in translated.items():
            if isinstance(value, list):
                # FIXED: Keep as arrays for JetEngine CLI import
                doc[key] = value
            else:
                doc[key] = value
        
        # === ADD FINANCIAL CALCULATIONS (rounded to integers) ===
        for key, value in financials.items():
            if isinstance(value, float):
                doc[key] = int(round(value, 0))  # Round to integers as per your example
            else:
                doc[key] = value
                
        return doc

    async def should_sync(self, processed_doc: dict) -> Tuple[bool, Dict[str, str], List[str]]:
        """
        Determine if a processed record should be synced to WordPress based on hash comparison.
        """
        current_hashes = processed_doc.get("hashes", {})
        existing = await self.processed_collection.find_one({"im_ad_id": processed_doc["im_ad_id"]})
        
        if not existing:
            # New record - sync all hash groups
            return True, current_hashes, list(current_hashes.keys())

        # Compare hashes to find what changed
        old_hashes = existing.get("hashes", {})
        changed_groups = [
            group for group in current_hashes 
            if old_hashes.get(group) != current_hashes[group]
        ]
        
        return bool(changed_groups), current_hashes, changed_groups

    async def _store_and_queue_if_changed(self, processed: dict) -> None:
        """
        Store processed record ONLY. Job queueing is handled by processed collection change streams.
        """
        try:
            # Store the processed record (this will trigger processed collection change stream)
            await self.processed_collection.update_one(
                {"im_ad_id": processed["im_ad_id"]}, 
                {"$set": processed}, 
                upsert=True
            )
            
            logger.debug(f"[{self.site}] Stored processed record for {processed['im_ad_id']}")
                
        except Exception as ex:
            logger.error(f"[{self.site}] Error storing processed record {processed.get('im_ad_id')}: {ex}")