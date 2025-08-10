"""
voorraad_processor.py

Processor for voorraad cars from voorraad_raw collection.
Simplified processing - no VAT/BPM calculations, only leasing.
Routes to processed_voorraad_{site} based on site field.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from jobqueue import WPQueue
from site_settings import SiteSettings
from utils import (
    calculate_hash_groups, normalize_gallery, extract_power_values,
    extract_numeric_value
)

class VoorraadProcessor:
    """Processor for voorraad cars with simplified calculations."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.raw_collection = db["voorraad_raw"]
        self.wp_queues: Dict[str, WPQueue] = {}
        
    async def process_by_site(self, site: str):
        """
        Process all voorraad_raw records for a specific site.
        Used for manual processing or rebuilds.
        """
        settings = await SiteSettings(self.db).get(site)
        
        cursor = self.raw_collection.find({
            "listing_status": True,
            "site": site
        })
        
        processed_count = 0
        skipped_count = 0
        
        logger.info(f"[voorraad_{site}] Starting bulk processing")
        
        async for raw in cursor:
            try:
                # Roep de centrale verwerkingslogica aan
                result = await self.process_single_record(raw, site, settings)
                if result:
                    processed_count += 1
                else:
                    skipped_count +=1
                    
            except Exception as ex:
                logger.error(f"[voorraad_{site}] Error processing {raw.get('car_id')}: {ex}")
                skipped_count += 1
        
        logger.info(f"[voorraad_{site}] Complete: {processed_count} processed, {skipped_count} skipped")
    
    async def process_single_record(self, raw: dict, site: str, site_settings: dict = None) -> Optional[dict]:
        """Process a single voorraad_raw record, store it, and queue for WP."""
        
        record_id = raw.get("car_id", "unknown")
        processed_collection = self.db[f"processed_voorraad_{site}"]
        
        try:
            if not site_settings:
                site_settings = await SiteSettings(self.db).get(site)
            
            processed = await self._process(raw, site, site_settings)
            if not processed:
                return None
            
            existing = await processed_collection.find_one({"st_ad_id": processed["st_ad_id"]})
            processed["_is_new"] = not bool(existing)
            processed["hashes"] = calculate_hash_groups(processed)
            
            await self._store_and_queue_if_changed(processed)
            
            logger.debug(f"[voorraad_{site}] Successfully processed and queued: {record_id}")
            return processed
            
        except Exception as ex:
            logger.error(f"[voorraad_{site}] Error processing single record {record_id}: {ex}")
            return None
    
    async def _process(self, raw: dict, site: str, site_settings: dict) -> Optional[dict]:
        """Build the processed document with translations and calculations."""
        doc: Dict[str, Any] = {}
        
        # Core identifiers en title
        doc["st_ad_id"] = raw.get("car_id", "")
        doc["st_url"] = raw.get("Product_URL", "")
        doc["st_mongo_id"] = str(raw.get("_id", ""))
        doc["st_title"] = f"{raw.get('brand', '')} {raw.get('model', '')} {raw.get('title', '')}".strip()
        
        # Gallery
        images = normalize_gallery(raw.get("Images", []))
        doc["st_gallery"] = images
        doc["st_featured_image"] = images[0] if images else ""
        
        # Basisdata
        doc["st_price"] = float(raw.get("price", 0))
        doc["st_mileage"] = int(raw.get("milage", 0) or raw.get("mileage", 0))
        doc["st_first_registration"] = raw.get("registration", "")
        doc["st_registration_year"] = int(raw.get("registration_year", 0) or 0)
        
        # Power
        doc["st_power"] = raw.get("power", "")
        kw, hp = extract_power_values(raw.get("power", ""))
        doc["st_kw_power"] = kw or raw.get("kw_power", 0)
        doc["st_hp_power"] = hp or raw.get("hp_power", 0)
        
        # Vertalingen
        doc["st_gearbox"] = await self._translate_field(raw.get("gearbox", "") or raw.get("TechnicalData", {}).get("Gearbox", ""), "gearbox")
        doc["st_body_type"] = await self._translate_field(raw.get("Basicdata", {}).get("Body", ""), "body_type")
        doc["st_drivetrain"] = await self._translate_field(raw.get("Basicdata", {}).get("Drivetrain", ""), "drivetrain")
        doc["st_fuel_type"] = await self._translate_field(raw.get("energyconsumption", {}).get("Fueltype", ""), "fuel_type")
        
        # Overige velden
        doc["st_doors"] = str(raw.get("Basicdata", {}).get("Doors", ""))
        doc["st_seats"] = int(raw.get("Basicdata", {}).get("Seats", 0) or 0)
        doc["st_cylinders"] = str(raw.get("TechnicalData", {}).get("Cylinders", ""))
        doc["st_cylinder_capacity"] = extract_numeric_value(raw.get("TechnicalData", {}).get("Enginesize", ""))
        doc["st_raw_emissions"] = raw.get("energyconsumption", {}).get("raw_emissions")
        doc["st_fullservicehistory"] = "Ja" if raw.get("vehiclehistory", {}).get("Fullservicehistory", False) else "Nee"
        doc["st_vat_deductible"] = "BTW auto" if bool(raw.get("vatded", False)) else "Marge auto"
        doc["st_vat_filter"] = "btw" if bool(raw.get("vatded", False)) else "marge"

        # Equipment/features
        equipment = raw.get("Equipment", {})
        doc["st_comfort_convenience"] = await self._translate_equipment(equipment.get("ComfortConvenience", []), "comfort_convenience")
        doc["st_entertainment_media"] = await self._translate_equipment(equipment.get("entertainmentAndMedia", []), "entertainment_media")
        doc["st_safety_security"] = await self._translate_equipment(equipment.get("safetyAndSecurity", []), "safety_security")
        doc["st_extras"] = await self._translate_equipment(equipment.get("extras", []), "extras")
        
        # Taxonomieën voor WordPress
        doc["make"] = raw.get("brand", "")
        doc["model"] = raw.get("model", "")
        doc["color"] = await self._translate_field(raw.get("colourandupholstery", {}).get("Colour", ""), "color")
        
        # Status en timestamps
        doc["st_status"] = "True" if raw.get("listing_status") else "False"
        doc["updated_at"] = datetime.utcnow()
        doc["site"] = site
        
        # Leasing berekening
        leasing = await self._calculate_leasing(doc["st_price"], site_settings)
        doc.update(leasing)
        
        return doc
    
    async def _translate_field(self, value: str, translation_key: str) -> str:
        if not value: return value
        try:
            trans_doc = await self.db.voorraad_translations.find_one({"translation_key": translation_key})
            if trans_doc and "translations" in trans_doc:
                translations = trans_doc["translations"]
                return translations.get(value, value)
            return value
        except Exception as ex:
            logger.error(f"Translation error for {translation_key}.{value}: {ex}")
            return value
    
    async def _translate_equipment(self, items: List[str], translation_key: str) -> List[str]:
        if not items: return []
        try:
            trans_doc = await self.db.voorraad_translations.find_one({"translation_key": translation_key})
            if not trans_doc or "translations" not in trans_doc: return items
            translations = trans_doc["translations"]
            return [translations.get(item, item) for item in items]
        except Exception as ex:
            logger.error(f"Equipment translation error for {translation_key}: {ex}")
            return items
    
    async def _calculate_leasing(self, price: float, site_settings: dict) -> dict:
        try:
            annual_interest = float(site_settings.get("annual_interest_rate", 0.08))
            loan_term_months = int(site_settings.get("loan_term_months", 72))
            down_payment_pct = 0.10
            remaining_debt_pct = 0.20
            
            down_payment = round(price * down_payment_pct, 2)
            remaining_debt = round(price * remaining_debt_pct, 2)
            principal = price - down_payment - remaining_debt
            
            if principal <= 0 or loan_term_months <= 0: return {"st_down_payment": 0, "st_desired_remaining_debt": 0, "st_monthly_payment": 0}
            
            monthly_interest = annual_interest / 12
            if monthly_interest == 0:
                monthly_payment = round(principal / loan_term_months, 2)
            else:
                payment = (principal * monthly_interest) / (1 - (1 + monthly_interest) ** -loan_term_months)
                balloon_interest = remaining_debt * monthly_interest
                monthly_payment = round(payment + balloon_interest, 2)
            
            return { "st_down_payment": int(round(down_payment)), "st_desired_remaining_debt": int(round(remaining_debt)), "st_monthly_payment": int(round(monthly_payment)) }
        except Exception as ex:
            logger.error(f"Leasing calculation error: {ex}")
            return { "st_down_payment": 0, "st_desired_remaining_debt": 0, "st_monthly_payment": 0 }

    async def should_sync(self, processed_doc: dict) -> Tuple[bool, Dict[str, str], List[str]]:
        current_hashes = processed_doc.get("hashes", {})
        processed_collection = self.db[f"processed_voorraad_{processed_doc['site']}"]
        existing = await processed_collection.find_one({"st_ad_id": processed_doc["st_ad_id"]})
        
        if not existing:
            return True, current_hashes, list(current_hashes.keys())

        old_hashes = existing.get("hashes", {})
        changed_groups = [group for group in current_hashes if old_hashes.get(group) != current_hashes[group]]
        
        return bool(changed_groups), current_hashes, changed_groups
    
    async def _store_and_queue_if_changed(self, processed: dict):
        try:
            should_sync, hash_groups, changed_groups = await self.should_sync(processed)

            processed_collection = self.db[f"processed_voorraad_{processed['site']}"]
            await processed_collection.replace_one(
                {"st_ad_id": processed["st_ad_id"]},
                processed,
                upsert=True
            )
            
            if should_sync:
                site = processed.get("site")
                if site not in self.wp_queues:
                    self.wp_queues[site] = WPQueue(self.db, site)
                
                queue = self.wp_queues[site]
                await queue.enqueue_job(
                    action="create" if processed["_is_new"] else "update",
                    ad_id=processed["st_ad_id"],
                    post_id=processed.get("wp_post_id"),
                    changed_fields=changed_groups,
                    hash_groups=hash_groups,
                    reason="voorraad_processed",
                    meta={"source_type": "voorraad"}
                )
                logger.info(f"Queued WP job for voorraad car {processed['st_ad_id']} on site {site}")
        except Exception as ex:
            logger.error(f"Error storing/queueing processed voorraad record {processed.get('st_ad_id')}: {ex}")
            raise