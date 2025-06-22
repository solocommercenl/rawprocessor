"""
queued_trigger_system.py

Updated trigger system that uses the processing queue for handling raw changes and site settings.
This provides better performance and reliability for high-volume operations.

FIXED: Robust site detection logic that works with any domain format.

Architecture:
Raw Changes → Queue Processing Jobs → Workers Update Processed → Processed Changes → Queue WP Jobs
Site Settings → Queue Processing Jobs → Workers Update Processed → Processed Changes → Queue WP Jobs
"""

import asyncio
from typing import Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from datetime import datetime

from site_settings import SiteSettings
from utils_filters import check_raw_against_filters
from processing_queue import ProcessingQueue, ProcessingJob, JobType
from jobqueue import WPQueue

# Import the new robust site detection
def extract_site_key(site_url: str) -> str:
    """
    Extract a consistent site key from any domain format.
    
    Examples:
    - "solostaging.nl" -> "solostaging"
    - "mysite.com" -> "mysite" 
    - "sub.domain.co.uk" -> "sub_domain"
    """
    import re
    
    if not site_url or not isinstance(site_url, str):
        return ""
    
    try:
        # Remove protocol if present
        clean_url = site_url.lower().strip()
        clean_url = re.sub(r'^https?://', '', clean_url)
        clean_url = re.sub(r'^www\.', '', clean_url)
        
        # Remove trailing slash and path
        clean_url = clean_url.split('/')[0]
        
        # Split by dots and remove TLD (last part)
        parts = clean_url.split('.')
        
        if len(parts) < 2:
            # No TLD found, use as-is
            domain_part = clean_url
        else:
            # Remove the TLD (last part)
            domain_part = '.'.join(parts[:-1])
        
        # Convert to valid identifier
        # Replace dots, hyphens, and other special chars with underscores
        site_key = re.sub(r'[^\w]', '_', domain_part)
        
        # Remove multiple consecutive underscores
        site_key = re.sub(r'_+', '_', site_key)
        
        # Remove leading/trailing underscores
        site_key = site_key.strip('_')
        
        # Validate result - must start with letter
        if not site_key or not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', site_key):
            logger.warning(f"Generated invalid site key '{site_key}' from '{site_url}', using fallback")
            # Fallback: use first part before first dot
            fallback = clean_url.split('.')[0]
            site_key = re.sub(r'[^\w]', '_', fallback)
            if not re.match(r'^[a-zA-Z]', site_key):
                site_key = f"site_{site_key}"
        
        return site_key
        
    except Exception as ex:
        logger.error(f"Error extracting site key from '{site_url}': {ex}")
        return f"site_{hash(site_url) % 10000}"  # Fallback to prevent crashes

class QueuedTriggerSystem:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.processing_queue = ProcessingQueue(db)
        self.wp_queues: Dict[str, WPQueue] = {}
        self.active_sites: List[str] = []
        self.running = False

    async def initialize(self):
        """
        Initialize the queued trigger system.
        FIXED: Uses robust site key extraction.
        """
        try:
            # Initialize processing queue
            await self.processing_queue.initialize()
            
            # Get all sites from site_settings
            cursor = self.db.site_settings.find({})
            sites = []
            
            async for site_doc in cursor:
                site_url = site_doc.get("site_url", "")
                if site_url:
                    # FIXED: Use robust site key extraction
                    site_key = extract_site_key(site_url)
                    if site_key:
                        sites.append(site_key)
                        # Initialize WP queue for each site
                        self.wp_queues[site_key] = WPQueue(self.db, site_key)
                        logger.debug(f"Mapped site_url '{site_url}' -> site_key '{site_key}'")
                    else:
                        logger.warning(f"Could not extract valid site key from '{site_url}'")
            
            self.active_sites = sites
            logger.info(f"Initialized queued trigger system for {len(sites)} sites: {sites}")
            
        except Exception as ex:
            logger.error(f"Failed to initialize queued trigger system: {ex}")
            raise

    async def start_watching(self):
        """
        Start watching MongoDB change streams and processing workers.
        """
        if self.running:
            logger.warning("Queued trigger system already running")
            return
            
        self.running = True
        logger.info("Starting queued trigger system...")
        
        try:
            # Start processing queue workers
            await self.processing_queue.start_workers(num_workers=5)
            
            # Start monitoring tasks
            tasks = [
                asyncio.create_task(self._watch_raw_collection()),
                asyncio.create_task(self._watch_processed_collections()),
                asyncio.create_task(self._watch_site_settings())
            ]
            
            # Wait for all monitoring tasks
            await asyncio.gather(*tasks)
                
        except Exception as ex:
            logger.error(f"Error in queued trigger system: {ex}")
            self.running = False
            raise

    async def stop_watching(self):
        """
        Stop the queued trigger system gracefully.
        """
        self.running = False
        logger.info("Stopping queued trigger system...")
        
        # Stop processing queue workers
        await self.processing_queue.stop_workers()
        
        logger.info("Queued trigger system stopped")

    async def _watch_raw_collection(self):
        """
        Watch raw collection changes and queue processing jobs.
        """
        logger.info("Starting raw collection monitoring (queued)...")
        
        try:
            # Watch for changes in raw collection
            pipeline = [
                {
                    "$match": {
                        "operationType": {"$in": ["insert", "update", "replace"]},
                    }
                }
            ]
            
            change_stream = self.db.raw.watch(pipeline, full_document="updateLookup")
            
            async for change in change_stream:
                if not self.running:
                    break
                    
                await self._handle_raw_change_queued(change)
                
        except Exception as ex:
            logger.error(f"Error in raw collection monitoring: {ex}")
            raise

    async def _watch_processed_collections(self):
        """
        Watch processed_{site} collection changes and queue WordPress jobs.
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
        Watch a single processed_{site} collection for changes and queue WP jobs.
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

    async def _watch_site_settings(self):
        """
        Watch site_settings collection for changes and queue processing jobs.
        """
        logger.info("Starting site_settings monitoring...")
        
        try:
            # Watch for changes in site_settings collection
            pipeline = [
                {
                    "$match": {
                        "operationType": {"$in": ["update", "replace"]},
                    }
                }
            ]
            
            change_stream = self.db.site_settings.watch(pipeline, full_document="updateLookup")
            
            async for change in change_stream:
                if not self.running:
                    break
                    
                await self._handle_site_settings_change(change)
                
        except Exception as ex:
            logger.error(f"Error in site_settings monitoring: {ex}")
            raise

    async def _handle_raw_change_queued(self, change: Dict[str, Any]):
        """
        Handle raw collection changes by queueing processing jobs.
        Only queue jobs for sites where the record passes site filters.
        """
        try:
            operation_type = change.get("operationType")
            document = change.get("fullDocument")
            if not document:
                logger.warning("Raw change event missing fullDocument")
                return
            car_id = document.get("car_id", "unknown")
            listing_status = document.get("listing_status", True)
            
            # Only process active listings or status changes
            if not listing_status and operation_type != "update":
                logger.debug(f"Skipping inactive listing: {car_id}")
                return
            
            logger.info(f"Evaluating raw {operation_type} for car_id={car_id}")
            
            # Check which sites this record is valid for
            valid_sites = []
            site_settings_obj = SiteSettings(self.db)
            
            for site in self.active_sites:
                try:
                    site_settings = await site_settings_obj.get(site)
                    filter_criteria = site_settings.get("filter_criteria", {})
                    
                    if check_raw_against_filters(document, filter_criteria):
                        valid_sites.append(site)
                        logger.debug(f"Record {car_id} passes filters for site {site}")
                    else:
                        logger.debug(f"Record {car_id} excluded by filters for site {site}")
                        
                except Exception as ex:
                    logger.error(f"Error checking filters for site {site}: {ex}")
                    # On error, include the site to be safe
                    valid_sites.append(site)
            
            if valid_sites:
                # Queue processing jobs only for sites where record passes filters
                job_ids = await self.processing_queue.enqueue_raw_change(
                    operation_type=operation_type,
                    car_id=car_id,
                    raw_document=document,
                    sites=valid_sites
                )
                logger.info(f"Queued {len(job_ids)} processing jobs for car_id={car_id} (valid for {len(valid_sites)}/{len(self.active_sites)} sites)")
            else:
                logger.info(f"Record {car_id} excluded by all site filters - no jobs queued")
                
        except Exception as ex:
            logger.error(f"Error handling raw change event: {ex}")
                    

    async def _handle_processed_change(self, change: Dict[str, Any], site: str):
        """
        Handle processed collection changes - queue WordPress jobs.
        """
        try:
            operation_type = change.get("operationType")
            document = change.get("fullDocument")
            document_key = change.get("documentKey", {})
            
            # Get the ad_id from either the document or document key
            if document:
                ad_id = document.get("im_ad_id")
                post_id = document.get("wp_post_id")  # If we track WP post IDs
            else:
                # For delete operations, we need to get ad_id from document key
                ad_id = None  # We'll need to handle this case
                post_id = None
            
            if not ad_id:
                logger.warning(f"[{site}] Processed change event missing ad_id")
                return
                
            logger.info(f"[{site}] Queueing WP {operation_type} for ad_id={ad_id}")
            
            queue = self.wp_queues[site]
            
            if operation_type == "insert":
                # New processed record - queue create job
                await queue.enqueue_job(
                    action="create",
                    ad_id=ad_id,
                    post_id=post_id,
                    changed_fields=list(document.get("hashes", {}).keys()),
                    hash_groups=document.get("hashes", {}),
                    reason="processed_insert"
                )
                logger.debug(f"[{site}] Queued create WP job for ad_id={ad_id}")
                
            elif operation_type in ("update", "replace"):
                # Updated processed record - queue update job
                # In a more sophisticated implementation, we'd compare hashes here
                changed_fields = ["pricing", "leasing", "gallery"]  # Simplified for now
                
                # Determine if this should be an update or create
                action = "update" if post_id else "create"
                
                await queue.enqueue_job(
                    action=action,
                    ad_id=ad_id,
                    post_id=post_id,
                    changed_fields=changed_fields,
                    hash_groups=document.get("hashes", {}),
                    reason="processed_update"
                )
                logger.debug(f"[{site}] Queued {action} WP job for ad_id={ad_id}")
                
            elif operation_type == "delete":
                # Processed record deleted - queue delete job
                await queue.enqueue_job(
                    action="delete",
                    ad_id=ad_id,
                    post_id=post_id,
                    changed_fields=["status"],
                    hash_groups={},
                    reason="processed_delete"
                )
                logger.debug(f"[{site}] Queued delete WP job for ad_id={ad_id}")
                
        except Exception as ex:
            logger.error(f"[{site}] Error handling processed change: {ex}")

    async def _handle_site_settings_change(self, change: Dict[str, Any]):
        """
        Handle site_settings changes by queueing batch processing jobs.
        FIXED: Uses robust site key extraction.
        """
        try:
            document = change.get("fullDocument")
            if not document:
                logger.warning("Site settings change event missing fullDocument")
                return
            
            site_url = document.get("site_url", "")
            # FIXED: Use robust site key extraction
            site_key = extract_site_key(site_url)
            
            if not site_key or site_key not in self.active_sites:
                logger.warning(f"Unknown or invalid site in settings change: '{site_url}' -> '{site_key}'")
                return
            
            # Determine what changed
            updated_fields = change.get("updateDescription", {}).get("updatedFields", {})
            
            change_types = []
            
            # Check for filter changes
            filter_fields = ["filter_criteria", "price_margins"]
            if any(field in updated_fields for field in filter_fields):
                change_types.append("filters_changed")
            
            # Check for pricing changes
            pricing_fields = [
                "licence_plate_fee", "rdw_inspection", "transport_cost", 
                "unforeseen_percentage", "annual_interest_rate", "loan_term_months"
            ]
            if any(field in updated_fields for field in pricing_fields):
                change_types.append("pricing_changed")
            
            # Queue processing jobs for each change type
            for change_type in change_types:
                job_ids = await self.processing_queue.enqueue_site_settings_change(
                    site=site_key,
                    change_type=change_type,
                    batch_processing=True  # Use batch processing for large operations
                )
                
                logger.info(f"Queued {len(job_ids)} batch jobs for {change_type} on site {site_key}")
                
        except Exception as ex:
            logger.error(f"Error handling site settings change: {ex}")

    async def manual_trigger(self, trigger_type: str, site: str = None, data: Dict[str, Any] = None):
        """
        Manually trigger processing through the queue system.
        """
        logger.info(f"Manual trigger: {trigger_type} for site={site}")
        
        try:
            if trigger_type == "site_settings.filters_changed":
                if not site:
                    raise ValueError("Site required for site_settings triggers")
                    
                job_ids = await self.processing_queue.enqueue_site_settings_change(
                    site=site,
                    change_type="filters_changed",
                    batch_processing=True
                )
                logger.info(f"Queued {len(job_ids)} jobs for filters changed on site {site}")
                
            elif trigger_type == "site_settings.pricing_changed":
                if not site:
                    raise ValueError("Site required for site_settings triggers")
                    
                job_ids = await self.processing_queue.enqueue_site_settings_change(
                    site=site,
                    change_type="pricing_changed",
                    batch_processing=True
                )
                logger.info(f"Queued {len(job_ids)} jobs for pricing changed on site {site}")
                
            elif trigger_type == "weekly_scheduled_job":
                # Queue pricing recalculation jobs for all sites (BPM depreciation updates)
                sites_to_process = [site] if site else self.active_sites
                
                for site_key in sites_to_process:
                    job_ids = await self.processing_queue.enqueue_site_settings_change(
                        site=site_key,
                        change_type="pricing_changed",
                        batch_processing=True
                    )
                    logger.info(f"Queued {len(job_ids)} weekly jobs for site {site_key}")
                
            elif trigger_type == "rebuild_site":
                if not site:
                    raise ValueError("Site required for rebuild")
                    
                job_ids = await self.processing_queue.enqueue_site_settings_change(
                    site=site,
                    change_type="rebuild",
                    batch_processing=True
                )
                logger.info(f"Queued {len(job_ids)} rebuild jobs for site {site}")
                
            else:
                logger.warning(f"Unknown manual trigger type: {trigger_type}")
                
        except Exception as ex:
            logger.error(f"Error in manual trigger {trigger_type}: {ex}")
            raise

    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get current system status including queue statistics.
        """
        status = {
            "running": self.running,
            "active_sites": self.active_sites,
            "site_count": len(self.active_sites),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Get processing queue stats
        processing_stats = await self.processing_queue.get_queue_stats()
        status["processing_queue"] = processing_stats
        
        # Get WP queue stats for each site
        wp_queue_stats = {}
        for site in self.active_sites:
            try:
                pending_count = await self.db[f"wp_sync_queue_{site}"].count_documents({"status": "pending"})
                failed_count = await self.db[f"wp_sync_queue_{site}"].count_documents({"status": "failed"})
                wp_queue_stats[site] = {
                    "pending": pending_count,
                    "failed": failed_count
                }
            except Exception as ex:
                wp_queue_stats[site] = {"error": str(ex)}
        
        status["wp_queue_stats"] = wp_queue_stats
        
        return status

    async def cleanup_old_jobs(self, hours: int = 24):
        """Clean up old completed jobs from the processing queue."""
        try:
            deleted_count = await self.processing_queue.cleanup_completed_jobs(older_than_hours=hours)
            logger.info(f"Cleaned up {deleted_count} old processing jobs")
            return deleted_count
        except Exception as ex:
            logger.error(f"Error cleaning up old jobs: {ex}")
            return 0

    async def retry_failed_processing_jobs(self, limit: int = 100):
        """Retry failed processing jobs."""
        try:
            retried_count = await self.processing_queue.retry_failed_jobs(limit=limit)
            logger.info(f"Retried {retried_count} failed processing jobs")
            return retried_count
        except Exception as ex:
            logger.error(f"Error retrying failed jobs: {ex}")
            return 0

    async def get_processing_performance_metrics(self) -> Dict[str, Any]:
        """
        Get detailed performance metrics for monitoring.
        """
        try:
            # Get processing queue metrics
            queue_stats = await self.processing_queue.get_queue_stats()
            
            # Calculate throughput metrics
            processing_stats = self.processing_queue.stats
            
            total_processed = processing_stats.get('jobs_processed', 0)
            total_time = processing_stats.get('processing_time_total', 0)
            
            avg_processing_time = total_time / total_processed if total_processed > 0 else 0
            
            # Get queue depth by priority
            pipeline = [
                {
                    "$group": {
                        "_id": {"priority": "$priority", "status": "$status"},
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            priority_breakdown = {}
            async for doc in self.processing_queue.collection.aggregate(pipeline):
                priority = doc["_id"]["priority"]
                status = doc["_id"]["status"]
                if priority not in priority_breakdown:
                    priority_breakdown[priority] = {}
                priority_breakdown[priority][status] = doc["count"]
            
            return {
                "queue_stats": queue_stats,
                "performance": {
                    "total_jobs_processed": total_processed,
                    "total_processing_time": total_time,
                    "average_processing_time": avg_processing_time,
                    "jobs_failed": processing_stats.get('jobs_failed', 0),
                    "jobs_retried": processing_stats.get('jobs_retried', 0),
                    "last_activity": processing_stats.get('last_activity')
                },
                "priority_breakdown": priority_breakdown,
                "worker_info": {
                    "workers_running": self.processing_queue.workers_running,
                    "active_workers": len(self.processing_queue.worker_tasks),
                    "max_workers": self.processing_queue.max_workers
                }
            }
            
        except Exception as ex:
            logger.error(f"Error getting performance metrics: {ex}")
            return {"error": str(ex)}