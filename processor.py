"""
processor.py
------------
Main orchestrator module for rawprocessor Stage 1.
- Loads per-site settings (filters, translation profile, etc).
- Enforces all filter_criteria and data validation before translation/calculation.
- Calls translator and calculator, checks group hashes for partial update logic.
- Writes eligible records to processed_{site}.
- Logs all processing steps, skips, and reasons.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from translator import Translator
from calculator import calculate_financials
from site_settings import get_site_settings
from queue import enqueue_wp_job
from utils import calculate_hash_groups, normalize_make_model
from cleaner import should_skip

REQUIRED_FIELDS = ["im_price_org", "im_registration_year", "fuel_type", "raw_emissions"]

class Processor:
    def __init__(self, db: AsyncIOMotorDatabase, site: str):
        self.db = db
        self.site = site
        self.translator = Translator(db)
        self.processed_collection = db[f"processed_{site}"]
        self.queue_collection = db[f"wp_sync_queue_{site}"]

    async def run(self):
        settings = await get_site_settings(self.db, self.site)
        filters = settings.get("filter_criteria", {})
        cursor = self.db.raw.find({"listing_status": True, **filters})

        async for raw in cursor:
            if should_skip(raw):
                logger.warning(f"[SKIP] {raw.get('_id')} - Invalid emissions/image count")
                continue

            if not self._has_required_fields(raw):
                logger.warning(f"[SKIP] {raw.get('_id')} - Missing required fields")
                continue

            translated = await self.translator.translate(raw, self.site)
            if translated is None:
                logger.warning(f"[SKIP] {raw.get('_id')} - Translation failed or incomplete")
                continue

            financials = await calculate_financials(raw, settings)
            doc = {**translated, **financials, "im_status": True}
            doc["make"], doc["model"] = normalize_make_model(doc.get("make", ""), doc.get("model", ""))
            doc["updated_at"] = datetime.utcnow()

            prev = await self.processed_collection.find_one({"im_ad_id": doc["im_ad_id"]})
            new_hashes = calculate_hash_groups(doc)
            old_hashes = prev.get("hashes", {}) if prev else {}
            changed_groups = [k for k, v in new_hashes.items() if old_hashes.get(k) != v]

            action = "create" if not prev else ("update" if changed_groups else None)
            if not action:
                logger.info(f"[NOCHANGE] {doc['im_ad_id']} - No field group changed")
                continue

            doc["hashes"] = new_hashes
            await self.processed_collection.update_one(
                {"im_ad_id": doc["im_ad_id"]}, {"$set": doc}, upsert=True
            )

            await enqueue_wp_job(
                self.queue_collection,
                im_ad_id=doc["im_ad_id"],
                action=action,
                changed_fields=changed_groups,
            )

    def _has_required_fields(self, raw: Dict[str, Any]) -> bool:
        return all(field in raw and raw[field] not in [None, ""] for field in REQUIRED_FIELDS)
