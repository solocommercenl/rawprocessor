"""
processor.py
------------
Main orchestrator module for rawprocessor Stage 1.
- Loads per-site settings (filters, translation profile, etc).
- Cleans and validates raw using only raw field names.
- Translates and calculates, mapping to JetEngine/processed keys only at output.
- Writes eligible records to processed_{site}.
- Logs all processing steps, skips, and reasons.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from translator import Translator
from calculator import Calculator
from site_settings import SiteSettings
from jobqueue import WPQueue
from utils import calculate_hash_groups, normalize_make_model
from cleaner import clean_raw_record

REQUIRED_RAW_FIELDS = ["price", "registration", "Fueltype", "raw_emissions", "Images"]

class Processor:
    def __init__(self, db: AsyncIOMotorDatabase, site: str):
        self.db = db
        self.site = site
        self.translator = Translator(db)
        self.calculator: Optional[Calculator] = None
        self.processed_collection = db[f"processed_{site}"]
        self.queue_collection = db[f"wp_sync_queue_{site}"]

    async def run(self):
        settings = await SiteSettings(self.db).get(self.site)
        self.calculator = Calculator(self.db, settings)
        filters = settings.get("filter_criteria", {})
        cursor = self.db.raw.find({"listing_status": True, **filters})

        async for raw in cursor:
            cleaned = clean_raw_record(raw, f"[{self.site}]")
            if not cleaned:
                continue
            if not self._has_required_fields(cleaned):
                logger.warning(f"[SKIP] {raw.get('_id')} - Missing required raw fields")
                continue
            processed = await self.process(cleaned, settings)
            if not processed:
                continue
            changed, hash_groups, changed_fields = await self.should_sync(processed)
            if not changed:
                logger.info(f"[NOCHANGE] {processed['im_ad_id']} - No field group changed")
                continue

            await self.processed_collection.update_one(
                {"im_ad_id": processed["im_ad_id"]}, {"$set": processed}, upsert=True
            )

            queue = WPQueue(self.db, self.site)
            await queue.enqueue_job(
                action="create" if processed.get("_is_new") else "update",
                ad_id=processed["im_ad_id"],
                post_id=None,
                changed_fields=changed_fields,
                hash_groups=hash_groups,
                meta={"origin": "processor.run"}
            )

    async def process(self, raw: dict, site_settings: dict) -> Optional[dict]:
        if self.calculator is None:
            self.calculator = Calculator(self.db, site_settings)

        translated = await self.translator.translate_fields(raw, site_settings, raw.get("_id"), self.site)
        if not translated:
            logger.warning(f"[SKIP] {raw.get('_id')} - Translation failed or incomplete")
            return None

        vated = str(raw.get("vatded", "false")).lower() == "true"
        financials = await self.calculator.calculate_financials(raw, vated)

        # Explicit field mapping to JetEngine/processed fields
        doc: Dict[str, Any] = {}

        # --- Combined Block for Meta Fields ---
        doc["im_ad_id"] = raw.get("car_id", "")
        doc["im_title"] = raw.get("title", "")
        doc["im_gallery"] = "|".join(raw.get("Images", [])) if isinstance(raw.get("Images"), list) else (raw.get("Images") or "")
        doc["im_featured_image"] = raw.get("Images", [""])[0] if raw.get("Images") else ""  # Featured image
        doc["im_price_org"] = round(float(raw.get("price", 0)), 2)  # Derived from price
        doc["im_registration_year"] = str(raw.get("registration_year", "")) or str(raw.get("im_registration_year", ""))
        doc["im_first_registration"] = raw.get("registration", "")
        doc["im_mileage"] = int(raw.get("milage") or raw.get("mileage") or 0)
        doc["im_power"] = raw.get("power", "")
        doc["im_upholstery"] = raw.get("colourandupholstery", {}).get("Upholstery", "")
        doc["im_manufacturer_color"] = raw.get("colourandupholstery", {}).get("Manufacturercolour", "")
        doc["im_paint_type"] = raw.get("colourandupholstery", {}).get("Paint", "")
        doc["im_dealer_company"] = raw.get("dealer_company", "")
        doc["im_dealer_contact"] = raw.get("dealer_contact", "")
        doc["im_dealer_phone"] = raw.get("dealer_phone", "")
        doc["im_dealer_city"] = raw.get("dealer_city", "")
        doc["im_product_url"] = raw.get("Product_URL", "")
        doc["im_status"] = True
        doc["updated_at"] = datetime.utcnow()

        # --- Handle missing fields ---
        doc["im_seats"] = raw.get("Basicdata", {}).get("Seats", "")  # Get from Basicdata object
        doc["im_body_type"] = raw.get("Basicdata", {}).get("Body", "")  # Body type from Basicdata
        doc["im_gearbox"] = raw.get("gearbox", "")  # Gearbox field from raw
        doc["im_make_model"] = f"{raw.get('brand', '')} {raw.get('model', '')}".strip()  # Combined make/model

        # --- Taxonomy Fields ---
        doc["make"] = raw.get("brand", "")  # Make as taxonomy
        doc["model"] = raw.get("model", "")  # Model as taxonomy
        doc["color"] = raw.get("colourandupholstery", {}).get("Colour", "")  # Color as taxonomy
        
        # --- Add translated and calculated fields ---
        doc.update(translated)
        for k, v in financials.items():
            if isinstance(v, float):
                doc[k] = int(round(v, 0))  # Round all financial fields to integers (no decimals)
            else:
                doc[k] = v

        # --- Ensure make/model normalization for taxonomy ---
        doc["make"] = raw.get("brand", "")  # Make as taxonomy
        doc["model"] = raw.get("model", "")  # Model as taxonomy

        existing = await self.processed_collection.find_one({"im_ad_id": doc["im_ad_id"]})
        doc["_is_new"] = not bool(existing)
        doc["hashes"] = calculate_hash_groups(doc)
        return doc

    async def should_sync(self, processed_doc: dict) -> Tuple[bool, Dict[str, str], List[str]]:
        current_hashes = processed_doc.get("hashes", {})
        existing = await self.processed_collection.find_one({"im_ad_id": processed_doc["im_ad_id"]})
        if not existing:
            return True, current_hashes, list(current_hashes.keys())

        old_hashes = existing.get("hashes", {})
        changed = [k for k in current_hashes if old_hashes.get(k) != current_hashes[k]]
        return (bool(changed), current_hashes, changed)

    def _has_required_fields(self, raw: Dict[str, Any]) -> bool:
        return all(field in raw and raw[field] not in [None, ""] for field in REQUIRED_RAW_FIELDS)
