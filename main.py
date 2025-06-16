"""
main.py

Entrypoint and orchestrator for rawprocessor.
Handles all triggers: raw insert/update, status change, site/translation updates, and batch jobs.
Supports CLI for: --trigger --site, --retry-failed, --rebuild-site
"""

import asyncio
import os
import argparse
from typing import Any, Dict
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger

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
    configure_logger(site)
    logger.info(f"Processing trigger: {trigger} for site {site}")

    settings = await SiteSettings(db).get(site)
    cleaner = Cleaner(db, site)
    translator = Translator(db)
    calculator = Calculator(db, site)
    processor = Processor(db, site)
    queue = WPQueue(db, site, retry_limit=settings.get("retry_limit", 3))

    # Debugging: Log if site settings are correctly loaded
    logger.debug(f"Site settings loaded: {settings}")

    if trigger == "raw.insert":
        records = data.get("records", [])
        logger.debug(f"Trigger 'raw.insert': Found {len(records)} raw records.")

        for raw in records:
            logger.debug(f"Processing raw record with ad_id={raw.get('ad_id', 'unknown')}")

            # Validate the raw record
            if not await cleaner.is_valid(raw):
                logger.info(f"Excluded raw {raw.get('ad_id', '')} (failed cleaner)")
                continue

            # Process the raw record
            processed = await processor.process(raw, settings)
            logger.debug(f"Processed data for ad_id={processed.get('im_ad_id', 'unknown')}")

            if processed:
                # Insert processed data into processed_{site} first
                await db[f"processed_{site}"].insert_one(processed)
                logger.info(f"Inserted processed data into processed_{site} for ad_id={processed['im_ad_id']}")

                # Then enqueue the create job for WordPress
                changed, hash_groups, changed_fields = await processor.should_sync(processed)
                if changed:
                    await queue.enqueue_job("create", processed["im_ad_id"], None, changed_fields, hash_groups, meta={"reason": trigger})
                    logger.debug(f"Enqueued create job for ad_id={processed['im_ad_id']}")
            else:
                logger.debug(f"No processed data for ad_id={raw.get('ad_id', 'unknown')}")

    elif trigger == "raw.update" or trigger == "raw.update.im_price":
        record = data.get("record")
        if not record:
            logger.warning(f"No record in payload for trigger {trigger}")
            return
        if not await cleaner.is_valid(record):
            logger.info(f"Excluded raw {record.get('ad_id', '')} (failed cleaner)")
            return
        processed = await processor.process(record, settings)
        if processed:
            # Insert processed data into processed_{site} first
            await db[f"processed_{site}"].insert_one(processed)
            logger.info(f"Inserted processed data into processed_{site} for ad_id={processed['im_ad_id']}")

            # Then enqueue the update job for WordPress
            changed, hash_groups, changed_fields = await processor.should_sync(processed)
            if changed:
                await queue.enqueue_job("update", processed["im_ad_id"], None, changed_fields, hash_groups, meta={"reason": trigger})

    elif trigger == "raw.update.im_status_false":
        record = data.get("record")
        if not record:
            logger.warning("No record found in data for raw.update.im_status_false")
            return
        processed = await processor.process(record, settings)
        if processed:
            await queue.enqueue_job("unpublish", processed["im_ad_id"], None, ["status"], {}, meta={"reason": trigger})

    elif trigger == "site_settings.filters_changed":
        cursor = db.raw.find({"cartype": {"$in": settings.get("filter_criteria", {}).get("cartype", ["Car"])}})
        async for raw in cursor:
            logger.debug(f"Checking raw record with ad_id={raw.get('ad_id', 'unknown')} for filters")
            if await cleaner.is_valid(raw):
                processed = await processor.process(raw, settings)
                if processed:
                    # Insert processed data into processed_{site} first
                    await db[f"processed_{site}"].insert_one(processed)
                    logger.info(f"Inserted processed data into processed_{site} for ad_id={processed['im_ad_id']}")

                    # Then enqueue the create job for WordPress
                    changed, hash_groups, changed_fields = await processor.should_sync(processed)
                    if changed:
                        await queue.enqueue_job("create", processed["im_ad_id"], None, changed_fields, hash_groups, meta={"reason": trigger})
            else:
                processed_doc = await db[f"processed_{site}"].find_one({"im_ad_id": raw.get("im_ad_id")})
                if processed_doc and processed_doc.get("im_status", True):
                    await queue.enqueue_job("unpublish", processed_doc["im_ad_id"], None, ["status"], {}, meta={"reason": "filters_changed → excluded"})

    elif trigger == "site_settings.pricing_changed" or trigger == "weekly_scheduled_job":
        cursor = db[f"processed_{site}"].find({})
        async for doc in cursor:
            processed = await processor.process(doc, settings)
            if processed:
                changed, hash_groups, changed_fields = await processor.should_sync(processed)
                if changed:
                    await queue.enqueue_job("update", processed["im_ad_id"], None, changed_fields, hash_groups, meta={"reason": trigger})

    else:
        logger.warning(f"Unknown trigger: {trigger}")

# --- CLI wrapper for manual triggering ---
async def run_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", required=True)
    parser.add_argument("--trigger")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--rebuild-site", action="store_true")
    args = parser.parse_args()

    if args.retry_failed:
        queue = WPQueue(db, args.site)
        count = await queue.retry_failed_jobs()
        logger.info(f"Retried {count} failed jobs for {args.site}")
    elif args.rebuild_site:
        processor = Processor(db, args.site)
        await processor.run()
        logger.info(f"Re-evaluated all raw listings for site {args.site}")
    elif args.trigger:
        await process_trigger(args.trigger, args.site, {})
    else:
        logger.error("Missing --trigger or --rebuild-site or --retry-failed")

if __name__ == "__main__":
    asyncio.run(run_cli())
