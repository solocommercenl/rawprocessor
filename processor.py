import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from translator import Translator
from calculator import Calculator
from site_settings import SiteSettings  
from jobqueue import WPQueue
from utils import calculate_hash_groups, normalize_gallery
from cleaner import Cleaner

class Processor:
    def __init__(self, db: AsyncIOMotorDatabase, site: str):
        self.db = db
        self.site = site
        self.cleaner = Cleaner(db, site)
        self.translator = Translator(db)
        self.calculator: Optional[Calculator] = None
        self.processed_collection = db[f"processed_{site}"]
        self.queue_collection = db[f"wp_sync_queue_{site}"]

    async def run(self):
        """
        Process all raw records for this site (used for rebuild operations).
        """
        settings = await SiteSettings(self.db).get(self.site)
        self.calculator = Calculator(self.db, settings)
        filters = settings.get("filter_criteria", {})
        
        # Find all active raw records
        cursor = self.db.raw.find({"listing_status": True, **filters})
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
        """
        record_id = raw.get("_id") or raw.get("car_id") or "unknown" 
        
        try:
            # Step 1: Clean and validate the raw record
            cleaned = await self.cleaner.clean_raw_record(raw, f"[{self.site}]")
            if not cleaned:
                logger.debug(f"[{self.site}] Record failed cleaning: {record_id}")
                return None

            # Step 2: Check if record should be processed for this site
            site_filters = site_settings.get("filter_criteria", {})
            if not await self.cleaner.is_record_processable_for_site(cleaned, site_filters, f"[{self.site}]"):
                logger.debug(f"[{self.site}] Record excluded by site filters: {record_id}")
                return None

            # Step 3: Process the record
            processed = await self.process(cleaned, site_settings)
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
        doc["im_gallery"] = "|".join(images) if images else ""
        doc["im_featured_image"] = images[0] if images else ""
        
        # === BASIC VEHICLE DATA ===
        doc["im_price_org"] = round(float(raw.get("price", 0)), 2)
        doc["im_registration_year"] = str(raw.get("registration_year", ""))
        doc["im_first_registration"] = raw.get("registration", "")
        doc["im_mileage"] = int(raw.get("milage") or raw.get("mileage") or 0)
        doc["im_power"] = raw.get("power", "")
        doc["im_fullservicehistory"] = bool(raw.get("service_history", False))
        
        # === APPEARANCE DATA ===
        doc["im_upholstery"] = raw.get("colourandupholstery", {}).get("Upholstery", "")
        doc["im_manufacturer_color"] = raw.get("colourandupholstery", {}).get("Manufacturercolour", "")
        doc["im_paint_type"] = raw.get("colourandupholstery", {}).get("Paint", "")
        
        # === DEALER DATA ===  
        doc["im_dealer_company"] = raw.get("dealer_company", "")
        doc["im_dealer_contact"] = raw.get("dealer_contact", "")
        doc["im_dealer_phone"] = raw.get("dealer_phone", "")
        doc["im_dealer_city"] = raw.get("dealer_city", "")
        doc["im_product_url"] = raw.get("Product_URL", "")
        
        # === VEHICLE SPECIFICATIONS ===
        doc["im_seats"] = raw.get("Basicdata", {}).get("Seats", 0)
        doc["im_body_type"] = raw.get("Basicdata", {}).get("Body", "")
        doc["im_gearbox"] = raw.get("gearbox", "")
        doc["im_make_model"] = f"{make} {model}".strip()
        
        # === EMISSIONS AND VAT ===
        doc["im_raw_emissions"] = raw.get("energyconsumption", {}).get("raw_emissions")
        doc["im_vat_deductible"] = bool(raw.get("vatded", False))
        
        # === TAXONOMIES (for WordPress) ===
        doc["make"] = make
        doc["model"] = model  
        doc["color"] = raw.get("colourandupholstery", {}).get("Colour", "")
        
        # === STATUS AND TIMESTAMPS ===
        doc["im_status"] = True
        doc["updated_at"] = datetime.utcnow()
        
        # === ADD TRANSLATED FIELDS ===
        for key, value in translated.items():
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