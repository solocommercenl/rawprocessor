"""
trigger_system.py

MongoDB Change Streams-based trigger system for rawprocessor Stage 1.
Watches for changes in raw collection and triggers processing for all sites.
"""

import asyncio
from typing import Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from datetime import datetime

from site_settings import SiteSettings
from processor import Processor
from jobqueue import WPQueue

class TriggerSystem:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.processors: Dict[str, Processor] = {}
        self.queues: Dict[str, WPQueue] = {}
        self.active_sites: List[str] = []
        self.running = False

    async def initialize(self):
        """
        Initialize the trigger system by loading all active sites.
        """
        try:
            # Get all sites from site_settings
            cursor = self.db.site_settings.find({})
            sites = []
            
            async for site_doc in cursor:
                site_key = site_doc.get("site_url", "").replace(".nl", "").replace(".", "_")
                if site_key:
                    sites.append(site_key)
                    
                    # Initialize processor and queue for each site
                    self.processors[site_key] = Processor(self.db, site_key)
                    self.queues[site_key] = WPQueue(self.db, site_key)
            
            self.active_sites = sites
            logger.info(f"Initialized trigger system for {len(sites)} sites: {sites}")
            
        except Exception as ex:
            logger.error(f"Failed to initialize trigger system: {ex}")
            raise

    async def start_watching(self):
        """
        Start watching MongoDB change streams for both raw and processed collections.
        """
        if self.running:
            logger.warning("Trigger system already running")
            return
            
        self.running = True
        logger.info("Starting MongoDB change stream monitoring...")
        
        try:
            # Start monitoring both raw and processed collections
            tasks = [
                asyncio.create_task(self._watch_raw_collection()),
                asyncio.create_task(self._watch_processed_collections())
            ]
            
            # Wait for all monitoring tasks
            await asyncio.gather(*tasks)
                
        except Exception as ex:
            logger.error(f"Error in change stream monitoring: {ex}")
            self.running = False
            raise

    async def _watch_raw_collection(self):
        """
        Watch raw collection changes - only updates processed_{site} collections.
        """
        logger.info("Starting raw collection monitoring...")
        
        try:
            # Watch for changes in raw collection
            pipeline = [
                {
                    "$match": {
                        "operationType": {"$in": ["insert", "update", "replace"]},
                        "fullDocument.listing_status": True  # Only process active listings
                    }
                }
            ]
            
            change_stream = self.db.raw.watch(pipeline, full_document="updateLookup")
            
            async for change in change_stream:
                if not self.running:
                    break
                    
                await self._handle_raw_change(change)
                
        except Exception as ex:
            logger.error(f"Error in raw collection monitoring: {ex}")
            raise

    async def _watch_processed_collections(self):
        """
        Watch processed_{site} collection changes - only queues WordPress jobs.
        """
        logger.info("Starting processed collections monitoring...")
        
        try:
            # Create change streams for each site's processed collection
            tasks = []
            for site in self.active_sites:
                task = asyncio.create_task(self._watch_single_processed_collection(site))
                tasks.append(task)
            
            await asyncio.gather(*tasks)
                
        except Exception as ex:
            logger.error(f"Error in processed collections monitoring: {ex}")
            raise

    async def _watch_single_processed_collection(self, site: str):
        """
        Watch a single processed_{site} collection for changes.
        """
        collection_name = f"processed_{site}"
        logger.info(f"Starting monitoring for {collection_name}")
        
        try:
            # Watch for changes in processed collection
            pipeline = [
                {
                    "$match": {
                        "operationType": {"$in": ["insert", "update", "replace", "delete"]},
                    }
                }
            ]
            
            change_stream = self.db[collection_name].watch(pipeline, full_document="updateLookup")
            
            async for change in change_stream:
                if not self.running:
                    break
                    
                await self._handle_processed_change(change, site)
                
        except Exception as ex:
            logger.error(f"Error monitoring {collection_name}: {ex}")
            raise

    async def stop_watching(self):
        """
        Stop the change stream monitoring.
        """
        self.running = False
        logger.info("Stopped change stream monitoring")

    async def _handle_raw_change(self, change: Dict[str, Any]):
        """
        Handle raw collection changes - ONLY updates processed collections.
        """
        try:
            operation_type = change.get("operationType")
            document = change.get("fullDocument")
            
            if not document:
                logger.warning("Raw change event missing fullDocument")
                return
                
            car_id = document.get("car_id", "unknown")
            
            logger.info(f"Processing raw {operation_type} for car_id={car_id}")
            
            # Determine what type of raw change occurred
            trigger_type = self._determine_raw_trigger_type(change, document)
            
            # Process for all active sites - UPDATE PROCESSED ONLY
            tasks = []
            for site in self.active_sites:
                task = self._update_processed_for_site(site, trigger_type, document)
                tasks.append(task)
            
            # Process all sites concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Log results
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            error_count = len(results) - success_count
            
            logger.info(f"Updated processed collections for car_id={car_id}: {success_count} sites succeeded, {error_count} failed")
                    
        except Exception as ex:
            logger.error(f"Error handling raw change event: {ex}")

    async def _handle_processed_change(self, change: Dict[str, Any], site: str):
        """
        Handle processed collection changes - ONLY queues WordPress jobs.
        """
        try:
            operation_type = change.get("operationType")
            document = change.get("fullDocument")
            document_key = change.get("documentKey", {})
            
            # Get the ad_id from either the document or document key
            if document:
                ad_id = document.get("im_ad_id")
            else:
                # For delete operations, we need to get ad_id from document key
                ad_id = None  # We'll need to handle this case
            
            if not ad_id:
                logger.warning(f"[{site}] Processed change event missing ad_id")
                return
                
            logger.info(f"[{site}] Processing processed {operation_type} for ad_id={ad_id}")
            
            queue = self.queues[site]
            
            if operation_type == "insert":
                # New processed record - queue create job
                await queue.enqueue_job(
                    action="create",
                    ad_id=ad_id,
                    post_id=None,
                    changed_fields=list(document.get("hashes", {}).keys()),
                    hash_groups=document.get("hashes", {}),
                    meta={"reason": "processed_insert"}
                )
                logger.debug(f"[{site}] Queued create job for ad_id={ad_id}")
                
            elif operation_type in ("update", "replace"):
                # Updated processed record - queue update job
                # We'd need to compare hashes to see what changed
                changed_fields = ["pricing", "leasing", "gallery"]  # Simplified for now
                await queue.enqueue_job(
                    action="update",
                    ad_id=ad_id,
                    post_id=None,
                    changed_fields=changed_fields,
                    hash_groups=document.get("hashes", {}),
                    meta={"reason": "processed_update"}
                )
                logger.debug(f"[{site}] Queued update job for ad_id={ad_id}")
                
            elif operation_type == "delete":
                # Processed record deleted - queue delete job
                await queue.enqueue_job(
                    action="delete",
                    ad_id=ad_id,
                    post_id=None,
                    changed_fields=["status"],
                    hash_groups={},
                    meta={"reason": "processed_delete"}
                )
                logger.debug(f"[{site}] Queued delete job for ad_id={ad_id}")
                
        except Exception as ex:
            logger.error(f"[{site}] Error handling processed change: {ex}")

    def _determine_raw_trigger_type(self, change: Dict[str, Any], document: Dict[str, Any]) -> str:
        """
        Determine the trigger type based on raw collection changes.
        """
        operation_type = change.get("operationType")
        
        if operation_type == "insert":
            return "raw.insert"
        
        if operation_type in ("update", "replace"):
            # Check if price changed
            updated_fields = change.get("updateDescription", {}).get("updatedFields", {})
            if "price" in updated_fields:
                return "raw.update.price"
            
            # Check if status changed to false
            if updated_fields.get("listing_status") is False:
                return "raw.update.status_false"
            
            return "raw.update"
        
        return "raw.change"

    async def _update_processed_for_site(self, site: str, trigger_type: str, document: Dict[str, Any]) -> bool:
        """
        Update processed_{site} collection based on raw changes.
        DOES NOT queue jobs - that's handled by processed collection changes.
        """
        try:
            processor = self.processors[site]
            site_settings = await SiteSettings(self.db).get(site)
            
            if trigger_type == "raw.insert":
                # New record - process if it passes site filters
                processed = await processor.process_single_record(document, site_settings)
                if processed:
                    # Store in processed collection (this will trigger processed change stream)
                    await self.db[f"processed_{site}"].update_one(
                        {"im_ad_id": processed["im_ad_id"]}, 
                        {"$set": processed}, 
                        upsert=True
                    )
                    logger.debug(f"[{site}] Created processed record for car_id={document.get('car_id')}")
                return True
                
            elif trigger_type == "raw.update.price":
                # Price changed - recalculate and update processed
                processed = await processor.process_single_record(document, site_settings)
                if processed:
                    await self.db[f"processed_{site}"].update_one(
                        {"im_ad_id": processed["im_ad_id"]}, 
                        {"$set": processed}, 
                        upsert=True
                    )
                    logger.debug(f"[{site}] Updated processed record for car_id={document.get('car_id')}")
                return True
                
            elif trigger_type == "raw.update.status_false":
                # Status changed to false - mark processed as inactive
                car_id = document.get("car_id")
                if car_id:
                    result = await self.db[f"processed_{site}"].update_one(
                        {"im_ad_id": car_id},
                        {"$set": {"im_status": False, "updated_at": datetime.utcnow()}}
                    )
                    if result.modified_count > 0:
                        logger.debug(f"[{site}] Marked processed record inactive for car_id={car_id}")
                return True
                
            elif trigger_type == "raw.update":
                # General update - reprocess
                processed = await processor.process_single_record(document, site_settings)
                if processed:
                    await self.db[f"processed_{site}"].update_one(
                        {"im_ad_id": processed["im_ad_id"]}, 
                        {"$set": processed}, 
                        upsert=True
                    )
                    logger.debug(f"[{site}] Updated processed record for car_id={document.get('car_id')}")
                return True
                
            return True
            
        except Exception as ex:
            logger.error(f"[{site}] Error updating processed for {trigger_type}: {ex}")
            return False

    async def manual_trigger(self, trigger_type: str, site: str = None, data: Dict[str, Any] = None):
        """
        Manually trigger processing for testing or scheduled jobs.
        """
        logger.info(f"Manual trigger: {trigger_type} for site={site}")
        
        try:
            if trigger_type == "site_settings.filters_changed":
                if not site:
                    raise ValueError("Site required for site_settings triggers")
                    
                await self._handle_site_filters_changed(site)
                
            elif trigger_type == "site_settings.pricing_changed":
                if not site:
                    raise ValueError("Site required for site_settings triggers")
                    
                await self._handle_site_pricing_changed(site)
                
            elif trigger_type == "weekly_scheduled_job":
                await self._handle_weekly_scheduled_job(site)
                
            elif trigger_type == "rebuild_site":
                if not site:
                    raise ValueError("Site required for rebuild")
                    
                await self._handle_rebuild_site(site)
                
            else:
                logger.warning(f"Unknown manual trigger type: {trigger_type}")
                
        except Exception as ex:
            logger.error(f"Error in manual trigger {trigger_type}: {ex}")
            raise

    async def _handle_site_filters_changed(self, site: str):
        """
        Handle site filter changes - re-evaluate all raw records for the site.
        """
        logger.info(f"[{site}] Processing site filters changed trigger")
        
        try:
            processor = self.processors[site]
            queue = self.queues[site]
            site_settings = await SiteSettings(self.db).get(site)
            
            # Get all raw records
            cursor = self.db.raw.find({"listing_status": True})
            processed_count = 0
            excluded_count = 0
            
            async for raw_doc in cursor:
                car_id = raw_doc.get("car_id")
                
                # Check if record should be processed for this site
                processed = await processor.process_single_record(raw_doc, site_settings)
                
                if processed:
                    # Record is now included - create or update
                    processed_count += 1
                    logger.debug(f"[{site}] Included car_id={car_id} after filter change")
                else:
                    # Record is excluded - check if we need to unpublish
                    existing = await self.db[f"processed_{site}"].find_one({"im_ad_id": car_id})
                    if existing and existing.get("im_status", False):
                        await queue.enqueue_job(
                            action="unpublish",
                            ad_id=car_id,
                            post_id=None,
                            changed_fields=["status"],
                            hash_groups={},
                            meta={"reason": "filters_changed_excluded"}
                        )
                        excluded_count += 1
                        logger.debug(f"[{site}] Excluded car_id={car_id} after filter change")
            
            logger.info(f"[{site}] Site filters changed processing complete: {processed_count} included, {excluded_count} excluded")
            
        except Exception as ex:
            logger.error(f"[{site}] Error handling site filters changed: {ex}")
            raise

    async def _handle_site_pricing_changed(self, site: str):
        """
        Handle site pricing changes - recalculate all financial data.
        """
        logger.info(f"[{site}] Processing site pricing changed trigger")
        
        try:
            processor = self.processors[site]
            site_settings = await SiteSettings(self.db).get(site)
            
            # Get all processed records for this site
            cursor = self.db[f"processed_{site}"].find({})
            updated_count = 0
            
            async for processed_doc in cursor:
                car_id = processed_doc.get("im_ad_id")
                
                # Get the original raw record
                raw_doc = await self.db.raw.find_one({"car_id": car_id})
                if not raw_doc:
                    logger.warning(f"[{site}] Raw record not found for car_id={car_id}")
                    continue
                
                # Reprocess with new pricing
                new_processed = await processor.process_single_record(raw_doc, site_settings)
                if new_processed:
                    # Check if pricing actually changed
                    changed, hash_groups, changed_fields = await processor.should_sync(new_processed)
                    if changed and any(field in ["pricing", "leasing"] for field in changed_fields):
                        await processor._store_and_queue_if_changed(new_processed)
                        updated_count += 1
                        logger.debug(f"[{site}] Updated pricing for car_id={car_id}")
            
            logger.info(f"[{site}] Site pricing changed processing complete: {updated_count} records updated")
            
        except Exception as ex:
            logger.error(f"[{site}] Error handling site pricing changed: {ex}")
            raise

    async def _handle_weekly_scheduled_job(self, site: str = None):
        """
        Handle weekly scheduled job - recalculate BPM depreciation for all records.
        """
        sites_to_process = [site] if site else self.active_sites
        logger.info(f"Processing weekly scheduled job for sites: {sites_to_process}")
        
        for site_key in sites_to_process:
            try:
                await self._handle_site_pricing_changed(site_key)  # BPM recalculation is part of pricing
            except Exception as ex:
                logger.error(f"Error in weekly job for site {site_key}: {ex}")

    async def _handle_rebuild_site(self, site: str):
        """
        Handle site rebuild - completely reprocess all raw records for the site.
        """
        logger.info(f"[{site}] Starting site rebuild")
        
        try:
            processor = self.processors[site]
            await processor.run()  # This processes all raw records
            logger.info(f"[{site}] Site rebuild completed")
            
        except Exception as ex:
            logger.error(f"[{site}] Error during site rebuild: {ex}")
            raise

    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get current system status for monitoring.
        """
        status = {
            "running": self.running,
            "active_sites": self.active_sites,
            "site_count": len(self.active_sites),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Get queue stats for each site
        queue_stats = {}
        for site in self.active_sites:
            try:
                pending_count = await self.db[f"wp_sync_queue_{site}"].count_documents({"status": "pending"})
                failed_count = await self.db[f"wp_sync_queue_{site}"].count_documents({"status": "failed"})
                queue_stats[site] = {
                    "pending": pending_count,
                    "failed": failed_count
                }
            except Exception as ex:
                queue_stats[site] = {"error": str(ex)}
        
        status["queue_stats"] = queue_stats
        return status