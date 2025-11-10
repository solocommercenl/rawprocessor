# migrate_images_kooymanimport.py
# One-time migration: Convert relative image paths to full CDN URLs

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from datetime import datetime

MONGO_URI = "mongodb://10.0.0.21:27017"
DB_NAME = "autodex"
SITE_URL = "kooymanimport.nl"
PROCESSED_COLLECTION = "processed_kooymanimport"
QUEUE_COLLECTION = "wp_sync_queue_autobedrijfkooyman"

async def migrate():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Get CDN URL from site_settings using site_url
    site_doc = await db.site_settings.find_one({"site_url": SITE_URL})
    if not site_doc:
        logger.error(f"Site settings not found for {SITE_URL}")
        return
    
    cdn_url = site_doc.get("cdn_url")
    if not cdn_url:
        logger.error(f"cdn_url not set in site_settings for {SITE_URL}")
        return
    
    # Add protocol if missing
    if not cdn_url.startswith("http"):
        cdn_url = f"https://{cdn_url}"
    
    logger.info(f"Using CDN URL: {cdn_url}")
    
    # Find all records with relative paths
    collection = db[PROCESSED_COLLECTION]
    cursor = collection.find({
        "im_featured_image": {"$regex": "^/"}
    })
    
    total = await collection.count_documents({"im_featured_image": {"$regex": "^/"}})
    logger.info(f"Found {total} records to migrate")
    
    updated = 0
    queued = 0
    errors = 0
    
    async for doc in cursor:
        try:
            ad_id = doc.get("im_ad_id")
            
            # Convert featured image
            featured = doc.get("im_featured_image", "")
            if featured.startswith("/"):
                featured = f"{cdn_url}{featured}"
            
            # Convert gallery
            gallery = doc.get("im_gallery", [])
            new_gallery = []
            for img in gallery:
                if isinstance(img, str) and img.startswith("/"):
                    new_gallery.append(f"{cdn_url}{img}")
                else:
                    new_gallery.append(img)
            
            # Update only image fields
            await collection.update_one(
                {"im_ad_id": ad_id},
                {"$set": {
                    "im_featured_image": featured,
                    "im_gallery": new_gallery
                }}
            )
            
            updated += 1
            
            # Queue WP sync job
            wp_post_id = doc.get("wp_post_id")
            if wp_post_id:
                await db[QUEUE_COLLECTION].insert_one({
                    "action": "update",
                    "ad_id": ad_id,
                    "post_id": wp_post_id,
                    "status": "pending",
                    "changed_fields": ["gallery"],
                    "reason": "image_migration",
                    "created_at": datetime.utcnow(),
                    "retry_count": 0
                })
                queued += 1
            
            if updated % 1000 == 0:
                logger.info(f"Progress: {updated}/{total} updated, {queued} queued")
                
        except Exception as ex:
            logger.error(f"Error processing {doc.get('im_ad_id')}: {ex}")
            errors += 1
    
    logger.info(f"Migration complete: {updated} updated, {queued} queued, {errors} errors")
    client.close()

if __name__ == "__main__":
    asyncio.run(migrate())