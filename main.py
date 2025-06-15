# main.py

"""
Entrypoint and orchestrator for rawprocessor.
Handles all triggers: raw insert/update, status change, site/translation updates, and batch jobs.
"""

import asyncio
import os
from typing import Any, Dict
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from logger import configure_logger, log_exceptions
from site_settings import SiteSettings
from cleaner import Cleaner
from translator import Translator
from calculator import Calculator
from processor import Processor
from jobqueue import WPQueue

load_dotenv()
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "autodex")
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]

@log_exceptions
async def process_trigger(trigger: str, site: str, data: Dict[str, Any]) -> None:
    """
    Handles:
      - raw.insert
      - raw.update (any field)
      - raw.update.im_price
      - raw.update.im_status = false
      - site_settings.filters changed
      - site_settings.pricing changed
      - weekly scheduled job
    """
    configure_logger(site)
    from loguru import logger
    logger.info(f"Processing trigger: {trigger} for site {site}")

    settings = await SiteSettings(db).get(site)
    cleaner = Cleaner(db, site)
    translator = Translator(db)
    calculator = Calculator(db, site)
    processor = Processor(db, site)
    queue = WPQueue(db, site, retry_limit=settings.get("retry_limit", 3))

    if trigger == "raw.insert":
        records = data.get("records", [])
        for raw in records:
            if not await cleaner.is_valid(raw):
                logger.info(f"Excluded raw {raw.get('ad_id', '')} (failed cleaner)")
                continue
            processed = await processor.process(raw, settings)
            if processed:
                changed, hash_groups, changed_fields = await processor.should_sync(processed)
                if changed:
                    await queue.enqueue_job(
                        action="create",
                        ad_id=processed["im_ad_id"],
                        post_id=None,
                        changed_fields=changed_fields,
                        hash_groups=hash_groups,
                        meta={"reason": "raw.insert"}
                    )
            else:
                logger.info(f"Record {raw.get('ad_id', '')} excluded after process step.")

    elif trigger == "raw.update":
        record = data.get("record")
        if not record:
            logger.warning("No record found in data for raw.update")
            return
        if not await cleaner.is_valid(record):
            logger.info(f"Excluded raw {record.get('ad_id', '')} (failed cleaner)")
            return
        processed = await processor.process(record, settings)
        if processed:
            changed, hash_groups, changed_fields = await processor.should_sync(processed)
            if changed:
                await queue.enqueue_job(
                    action="update",
                    ad_id=processed["im_ad_id"],
                    post_id=None,
                    changed_fields=changed_fields,
                    hash_groups=hash_groups,
                    meta={"reason": "raw.update"}
                )
        else:
            logger.info(f"Record {record.get('ad_id', '')} excluded after process step.")

    elif trigger == "raw.update.im_price":
        # Typically treated the same as raw.update (with special audit)
        record = data.get("record")
        if not record:
            logger.warning("No record found in data for raw.update.im_price")
            return
        if not await cleaner.is_valid(record):
            logger.info(f"Excluded raw {record.get('ad_id', '')} (failed cleaner)")
            return
        processed = await processor.process(record, settings)
        if processed:
            changed, hash_groups, changed_fields = await processor.should_sync(processed)
            if changed:
                await queue.enqueue_job(
                    action="update",
                    ad_id=processed["im_ad_id"],
                    post_id=None,
                    changed_fields=changed_fields,
                    hash_groups=hash_groups,
                    meta={"reason": "raw.update.im_price"}
                )
        else:
            logger.info(f"Record {record.get('ad_id', '')} excluded after process step.")

    elif trigger == "raw.update.im_status_false":
        # Mark unpublished, queue unpublish, after 5 days queue delete
        record = data.get("record")
        if not record:
            logger.warning("No record found in data for raw.update.im_status_false")
            return
        processed = await processor.process(record, settings)
        if processed:
            await queue.enqueue_job(
                action="unpublish",
                ad_id=processed["im_ad_id"],
                post_id=None,
                changed_fields=["status"],
                hash_groups={},
                meta={"reason": "raw.update.im_status_false"}
            )
            # The actual delayed delete after 5 days is handled by a scheduled batch elsewhere

    elif trigger == "site_settings.filters_changed":
        # Re-evaluate all raw for site
        raw_cursor = db.raw.find({"cartype": {"$in": settings.get("filter_criteria", {}).get("cartype", ["Car"])}})
        async for raw in raw_cursor:
            if await cleaner.is_valid(raw):
                processed = await processor.process(raw, settings)
                if processed:
                    changed, hash_groups, changed_fields = await processor.should_sync(processed)
                    if changed:
                        await queue.enqueue_job(
                            action="create",
                            ad_id=processed["im_ad_id"],
                            post_id=None,
                            changed_fields=changed_fields,
                            hash_groups=hash_groups,
                            meta={"reason": "site_settings.filters_changed"}
                        )
                # Else: it’s an excluded vehicle; logic to unpublish & delete as below:
            else:
                # Find the processed record and queue unpublish/delete if it was previously included
                processed_doc = await db[f"processed_{site}"].find_one({"im_ad_id": raw.get("im_ad_id")})
                if processed_doc and processed_doc.get("im_status", True):
                    await queue.enqueue_job(
                        action="unpublish",
                        ad_id=processed_doc["im_ad_id"],
                        post_id=None,
                        changed_fields=["status"],
                        hash_groups={},
                        meta={"reason": "site_settings.filters_changed (now excluded)"}
                    )
                    # The actual "delete" after 5 days to be scheduled by a separate batch job

    elif trigger == "site_settings.pricing_changed":
        # Recalculate margins/payments, update if hash changed
        processed_cursor = db[f"processed_{site}"].find({})
        async for doc in processed_cursor:
            processed = await processor.process(doc, settings)
            if processed:
                changed, hash_groups, changed_fields = await processor.should_sync(processed)
                if changed:
                    await queue.enqueue_job(
                        action="update",
                        ad_id=processed["im_ad_id"],
                        post_id=None,
                        changed_fields=changed_fields,
                        hash_groups=hash_groups,
                        meta={"reason": "site_settings.pricing_changed"}
                    )

    elif trigger == "weekly_scheduled_job":
        # Recalc BPM depreciation and update if changed
        processed_cursor = db[f"processed_{site}"].find({})
        async for doc in processed_cursor:
            processed = await processor.process(doc, settings)
            if processed:
                changed, hash_groups, changed_fields = await processor.should_sync(processed)
                if changed:
                    await queue.enqueue_job(
                        action="update",
                        ad_id=processed["im_ad_id"],
                        post_id=None,
                        changed_fields=changed_fields,
                        hash_groups=hash_groups,
                        meta={"reason": "weekly_scheduled_job"}
                    )
    else:
        logger.warning(f"Unknown trigger: {trigger}")