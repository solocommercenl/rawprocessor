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
        # Translate using raw field names; output is processed keys (im_*)
        translated = await self.translator.translate_fields(raw, site_settings, raw.get("_id"), self.site)
        if not translated:
            logger.warning(f"[SKIP] {raw.get('_id')} - Translation failed or incomplete")
            return None

        vated = str(raw.get("vatded", "false")).lower() == "true"
        financials = await self.calculator.calculate_financials(raw, vated)

        # Build the processed output dict (only now assign to processed/JetEngine field names)
        doc = {**translated, **financials, "im_status": True}
        doc["make"], doc["model"] = normalize_make_model(doc.get("make", ""), doc.get("model", ""))
        doc["updated_at"] = datetime.utcnow()
        doc["im_ad_id"] = raw.get("im_ad_id") or raw.get("ad_id") or raw.get("_id")

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
