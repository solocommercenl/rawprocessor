# migrate_images_texbijl.py
# One-time migration: Convert image paths to full CDN URLs
# Handles both relative paths and AutoScout URLs with batch processing

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger

MONGO_URI = "mongodb://10.0.0.21:27017"
DB_NAME = "autodex"
SITE_URL = "texbijl.nl"
PROCESSED_COLLECTION = "processed_texbijl"
BATCH_SIZE = 500
CONCURRENT_BATCHES = 10

async def process_record(doc, collection, raw_collection, cdn_url, stats):
    """Process a single record"""
    try:
        ad_id = doc.get("im_ad_id")
        featured = doc.get("im_featured_image", "")
        gallery = doc.get("im_gallery", [])
        
        # Check if any image needs migration
        needs_migration = False
        migration_type = None
        
        if featured.startswith("/"):
            needs_migration = True
            migration_type = "relative"
        elif "autoscout24" in featured or any("autoscout24" in str(img) for img in gallery):
            needs_migration = True
            migration_type = "autoscout"
        
        if not needs_migration:
            stats['skipped_no_migration'] += 1
            return
        
        # Case 1: Relative paths
        if migration_type == "relative":
            new_featured = f"{cdn_url}{featured}" if featured.startswith("/") else featured
            new_gallery = [f"{cdn_url}{img}" if isinstance(img, str) and img.startswith("/") else img for img in gallery]
            
            await collection.update_one(
                {"im_ad_id": ad_id},
                {"$set": {
                    "im_featured_image": new_featured,
                    "im_gallery": new_gallery
                }}
            )
            
            stats['updated_relative'] += 1
        
        # Case 2: AutoScout URLs
        elif migration_type == "autoscout":
            raw_doc = await raw_collection.find_one({"car_id": ad_id})
            
            if not raw_doc:
                logger.warning(f"No raw record found for {ad_id} - marking as inactive")
                
                # Mark as inactive so it gets drafted/removed
                await collection.update_one(
                    {"im_ad_id": ad_id},
                    {"$set": {"im_status": "False"}}
                )
                
                stats['marked_inactive'] += 1
                return
            
            if not raw_doc.get("listing_status", False):
                stats['skipped_inactive_raw'] += 1
                return
            
            raw_images = raw_doc.get("Images", [])
            if not raw_images:
                logger.warning(f"No images in raw for {ad_id}")
                return
            
            new_featured = f"{cdn_url}{raw_images[0]}" if raw_images else ""
            new_gallery = [f"{cdn_url}{img}" for img in raw_images]
            
            await collection.update_one(
                {"im_ad_id": ad_id},
                {"$set": {
                    "im_featured_image": new_featured,
                    "im_gallery": new_gallery
                }}
            )
            
            stats['updated_autoscout'] += 1
            
    except Exception as ex:
        logger.error(f"Error processing {doc.get('im_ad_id')}: {ex}")
        stats['errors'] += 1

async def process_batch(batch, collection, raw_collection, cdn_url, stats):
    """Process a batch of records concurrently"""
    tasks = [process_record(doc, collection, raw_collection, cdn_url, stats) for doc in batch]
    await asyncio.gather(*tasks)

async def migrate():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Get CDN URL from site_settings (note: field has space in your settings)
    site_doc = await db.site_settings.find_one({"site_url": SITE_URL})
    if not site_doc:
        logger.error(f"Site settings not found for {SITE_URL}")
        return
    
    # Try both with and without space
    cdn_url = site_doc.get("cdn_url") or site_doc.get(" cdn_url")
    if not cdn_url:
        logger.error(f"cdn_url not set in site_settings for {SITE_URL}")
        return
    
    if not cdn_url.startswith("http"):
        cdn_url = f"https://{cdn_url}"
    
    logger.info(f"Using CDN URL: {cdn_url}")
    
    collection = db[PROCESSED_COLLECTION]
    raw_collection = db.raw
    
    # Get all active processed records
    total = await collection.count_documents({"im_status": "True"})
    logger.info(f"Processing {total} active records in batches of {BATCH_SIZE}")
    
    stats = {
        'updated_relative': 0,
        'updated_autoscout': 0,
        'skipped_inactive_raw': 0,
        'skipped_no_migration': 0,
        'marked_inactive': 0,
        'errors': 0
    }
    
    # Process in batches
    cursor = collection.find({"im_status": "True"})
    batch = []
    batch_tasks = []
    
    async for doc in cursor:
        batch.append(doc)
        
        if len(batch) >= BATCH_SIZE:
            # Start processing this batch
            batch_tasks.append(process_batch(batch, collection, raw_collection, cdn_url, stats))
            batch = []
            
            # Process batches concurrently
            if len(batch_tasks) >= CONCURRENT_BATCHES:
                await asyncio.gather(*batch_tasks)
                processed = stats['updated_relative'] + stats['updated_autoscout']
                logger.info(f"Progress: {processed}/{total} - {stats['updated_relative']} relative, {stats['updated_autoscout']} autoscout, {stats['marked_inactive']} marked inactive")
                batch_tasks = []
    
    # Process remaining records
    if batch:
        batch_tasks.append(process_batch(batch, collection, raw_collection, cdn_url, stats))
    if batch_tasks:
        await asyncio.gather(*batch_tasks)
    
    logger.info("Migration complete:")
    logger.info(f"  Updated (relative): {stats['updated_relative']}")
    logger.info(f"  Updated (autoscout): {stats['updated_autoscout']}")
    logger.info(f"  Skipped (no migration needed): {stats['skipped_no_migration']}")
    logger.info(f"  Skipped (inactive raw): {stats['skipped_inactive_raw']}")
    logger.info(f"  Marked inactive (no raw): {stats['marked_inactive']}")
    logger.info(f"  Errors: {stats['errors']}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(migrate())