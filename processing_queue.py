"""
processing_queue.py

Queued processing system for rawprocessor Stage 1.
Handles high-volume raw changes and site settings changes through a robust queue system.

UPDATED: Now uses centralized configuration system instead of hard-coded values.
FIXED: Proper filter change logic that handles add/remove operations correctly.

Architecture:
Raw/Settings Change → Queue Processing Job → Worker Processes → Update Processed → Trigger WP Jobs

Features:
- MongoDB-based queue with priority, retry logic, and backpressure
- Configurable worker pools with batch processing
- Smart batching for site settings changes (200K records)
- Burst absorption and rate limiting
- Dead letter queue for failed jobs
- Comprehensive monitoring and metrics
- PROPER filter change handling (adds/removes records correctly)
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from enum import Enum
from dataclasses import dataclass, asdict
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from bson import ObjectId

from config import config
from site_settings import SiteSettings
from processor import Processor
from cleaner import Cleaner
from utils_filters import check_raw_against_filters

class JobType(Enum):
    RAW_INSERT = "raw_insert"
    RAW_UPDATE = "raw_update" 
    RAW_PRICE_CHANGE = "raw_price_change"
    RAW_STATUS_CHANGE = "raw_status_change"
    SITE_FILTERS_CHANGED = "site_filters_changed"
    SITE_PRICING_CHANGED = "site_pricing_changed"
    SITE_REBUILD = "site_rebuild"

class JobStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"

@dataclass
class ProcessingJob:
    job_type: JobType
    site: str
    priority: int = 5  # 1=highest, 10=lowest
    payload: Dict[str, Any] = None
    batch_size: int = 1
    max_retries: int = None  # Will use config default
    retry_delay_seconds: int = None  # Will use config default
    timeout_seconds: int = None  # Will use config default
    
    # Runtime fields
    job_id: Optional[str] = None
    status: JobStatus = JobStatus.PENDING
    retries: int = 0
    created_at: Optional[datetime] = None
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        """Set defaults from config if not provided."""
        if self.max_retries is None:
            self.max_retries = config.QUEUE_RETRY_LIMIT
        if self.retry_delay_seconds is None:
            self.retry_delay_seconds = config.QUEUE_RETRY_DELAY
        if self.timeout_seconds is None:
            self.timeout_seconds = config.QUEUE_TIMEOUT_SHORT
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to MongoDB document."""
        data = asdict(self)
        # Convert enums to strings
        data['job_type'] = self.job_type.value
        data['status'] = self.status.value
        # Convert datetime objects
        for field in ['created_at', 'scheduled_at', 'started_at', 'completed_at']:
            if data[field]:
                data[field] = data[field]
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessingJob':
        """Create from MongoDB document."""
        # Remove MongoDB's _id field if present
        clean_data = data.copy()
        if '_id' in clean_data:
            del clean_data['_id']
        
        # Convert enum strings back to enum objects
        clean_data['job_type'] = JobType(clean_data['job_type'])
        clean_data['status'] = JobStatus(clean_data['status'])
        
        return cls(**clean_data)

class ProcessingQueue:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = db.processing_queue
        self.processors: Dict[str, Processor] = {}
        self.wp_queues: Dict[str, Any] = {}
        self.workers_running = False
        self.worker_tasks: List[asyncio.Task] = []
        
        # Configuration from config system
        self.max_workers = config.QUEUE_MAX_WORKERS
        self.batch_size = config.QUEUE_BATCH_SIZE
        self.poll_interval = config.QUEUE_POLL_INTERVAL
        self.max_processing_time = config.QUEUE_MAX_PROCESSING_TIME
        
        # Metrics
        self.stats = {
            'jobs_processed': 0,
            'jobs_failed': 0,
            'jobs_retried': 0,
            'processing_time_total': 0.0,
            'last_activity': None
        }
        
        logger.info(f"ProcessingQueue initialized with config: workers={self.max_workers}, batch={self.batch_size}, poll={self.poll_interval}s")

    async def initialize(self):
        """Initialize the processing queue system."""
        try:
            # Create indexes for efficient querying
            await self.collection.create_index([
                ("status", 1),
                ("priority", 1),
                ("scheduled_at", 1)
            ])
            
            await self.collection.create_index([("site", 1), ("status", 1)])
            await self.collection.create_index([("job_type", 1), ("created_at", -1)])
            
            # Initialize processors for all sites
            await self._initialize_processors()
            
            logger.info("Processing queue system initialized")
            
        except Exception as ex:
            logger.error(f"Failed to initialize processing queue: {ex}")
            raise

    async def _initialize_processors(self):
        """Initialize processors for all active sites."""
        try:
            cursor = self.db.site_settings.find({})
            async for site_doc in cursor:
                site_url = site_doc.get("site_url", "")
                if site_url:
                    # Use the same site key extraction as in queued_trigger_system
                    import re
                    clean_url = site_url.lower().strip()
                    clean_url = re.sub(r'^https?://', '', clean_url)
                    clean_url = re.sub(r'^www\.', '', clean_url)
                    clean_url = clean_url.split('/')[0]
                    
                    parts = clean_url.split('.')
                    if len(parts) >= 2:
                        domain_part = '.'.join(parts[:-1])
                    else:
                        domain_part = clean_url
                    
                    site_key = re.sub(r'[^\w]', '_', domain_part)
                    site_key = re.sub(r'_+', '_', site_key).strip('_')
                    
                    if site_key and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', site_key):
                        self.processors[site_key] = Processor(self.db, site_key)

                        from jobqueue import WPQueue  # ← ADD this import
                        self.wp_queues[site_key] = WPQueue(self.db, site_key)  # ← ADD this line
            
            logger.info(f"Initialized processors for {len(self.processors)} sites")
            
        except Exception as ex:
            logger.error(f"Failed to initialize processors: {ex}")
            raise

    async def enqueue_job(self, job: ProcessingJob) -> str:
        """
        Enqueue a processing job.
        Returns the job ID.
        """
        try:
            # Set runtime fields
            job.job_id = str(ObjectId())
            job.created_at = datetime.utcnow()
            job.scheduled_at = datetime.utcnow()
            job.status = JobStatus.PENDING
            
            # Insert into queue
            await self.collection.insert_one(job.to_dict())
            
            logger.info(f"Enqueued {job.job_type.value} job for site {job.site} with ID {job.job_id}")
            return job.job_id
            
        except Exception as ex:
            logger.error(f"Failed to enqueue job: {ex}")
            raise

    async def enqueue_raw_change(self, operation_type: str, car_id: str, raw_document: Dict[str, Any], sites: List[str] = None):
        """
        Enqueue jobs for raw collection changes.
        Creates separate jobs for each affected site.
        """
        if not sites:
            sites = list(self.processors.keys())
        
        # Determine job type based on change
        if operation_type == "insert":
            job_type = JobType.RAW_INSERT
        elif "price" in str(raw_document.get("updateDescription", {})):
            job_type = JobType.RAW_PRICE_CHANGE
        elif raw_document.get("listing_status") is False:
            job_type = JobType.RAW_STATUS_CHANGE
        else:
            job_type = JobType.RAW_UPDATE
        
        job_ids = []
        for site in sites:
            job = ProcessingJob(
                job_type=job_type,
                site=site,
                priority=3 if job_type == JobType.RAW_PRICE_CHANGE else 5,
                timeout_seconds=config.QUEUE_TIMEOUT_SHORT,
                payload={
                    "car_id": car_id,
                    "raw_document": raw_document,
                    "operation_type": operation_type
                }
            )
            job_id = await self.enqueue_job(job)
            job_ids.append(job_id)
        
        logger.info(f"Enqueued {len(job_ids)} jobs for {job_type.value} on car_id={car_id}")
        return job_ids

    async def enqueue_site_settings_change(self, site: str, change_type: str, batch_processing: bool = True):
        """
        Enqueue site settings change jobs.
        Uses batch processing for large operations.
        """
        if change_type == "filters_changed":
            job_type = JobType.SITE_FILTERS_CHANGED
            priority = 2  # High priority
        elif change_type == "pricing_changed":
            job_type = JobType.SITE_PRICING_CHANGED
            priority = 4  # Medium priority
        else:
            job_type = JobType.SITE_REBUILD
            priority = 6  # Lower priority
        
        if batch_processing and config.ENABLE_BATCH_PROCESSING:
            # For large operations, process in batches
            total_records = await self.db.raw.count_documents({"listing_status": True})
            batch_count = (total_records + self.batch_size - 1) // self.batch_size
            
            job_ids = []
            for batch_num in range(batch_count):
                job = ProcessingJob(
                    job_type=job_type,
                    site=site,
                    priority=priority,
                    batch_size=self.batch_size,
                    timeout_seconds=config.QUEUE_TIMEOUT_MEDIUM,
                    payload={
                        "change_type": change_type,
                        "batch_number": batch_num,
                        "total_batches": batch_count,
                        "skip": batch_num * self.batch_size,
                        "limit": self.batch_size
                    }
                )
                job_id = await self.enqueue_job(job)
                job_ids.append(job_id)
            
            logger.info(f"Enqueued {len(job_ids)} batch jobs for {job_type.value} on site {site}")
            return job_ids
        else:
            # Single job for the entire operation
            job = ProcessingJob(
                job_type=job_type,
                site=site,
                priority=priority,
                timeout_seconds=config.QUEUE_TIMEOUT_LONG,
                payload={"change_type": change_type}
            )
            job_id = await self.enqueue_job(job)
            logger.info(f"Enqueued single job for {job_type.value} on site {site}")
            return [job_id]

    async def start_workers(self, num_workers: int = None):
        """
        Start worker processes to consume the queue.
        """
        if self.workers_running:
            logger.warning("Workers already running")
            return
        
        num_workers = num_workers or self.max_workers
        self.workers_running = True
        
        logger.info(f"Starting {num_workers} queue workers")
        
        # Start worker tasks
        for worker_id in range(num_workers):
            task = asyncio.create_task(self._worker_loop(worker_id))
            self.worker_tasks.append(task)
        
        logger.info("All workers started successfully")

    async def stop_workers(self):
        """
        Stop all worker processes gracefully.
        """
        if not self.workers_running:
            return
        
        logger.info("Stopping queue workers...")
        self.workers_running = False
        
        # Cancel all worker tasks
        for task in self.worker_tasks:
            task.cancel()
        
        # Wait for all tasks to complete
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        
        self.worker_tasks.clear()
        logger.info("All workers stopped")

    async def _worker_loop(self, worker_id: int):
        """
        Main worker loop - processes jobs from the queue.
        """
        logger.info(f"Worker {worker_id} started")
        
        try:
            while self.workers_running:
                try:
                    # Get next job from queue
                    job_doc = await self._get_next_job()
                    
                    if not job_doc:
                        # No jobs available, wait before polling again
                        await asyncio.sleep(self.poll_interval)
                        continue
                    
                    job = ProcessingJob.from_dict(job_doc)
                    logger.info(f"Worker {worker_id} processing job {job.job_id} ({job.job_type.value})")
                    
                    # Process the job
                    start_time = datetime.utcnow()
                    success = await self._process_job(job, worker_id)
                    processing_time = (datetime.utcnow() - start_time).total_seconds()
                    
                    # Update job status
                    if success:
                        await self._mark_job_completed(job.job_id, processing_time)
                        self.stats['jobs_processed'] += 1
                    else:
                        await self._mark_job_failed(job)
                        self.stats['jobs_failed'] += 1
                    
                    self.stats['processing_time_total'] += processing_time
                    self.stats['last_activity'] = datetime.utcnow()
                    
                except asyncio.CancelledError:
                    break
                except Exception as ex:
                    logger.error(f"Worker {worker_id} error: {ex}")
                    await asyncio.sleep(5)  # Wait before retrying
        
        except asyncio.CancelledError:
            pass
        
        logger.info(f"Worker {worker_id} stopped")

    async def _get_next_job(self) -> Optional[Dict[str, Any]]:
        """
        Get the next job from the queue (highest priority, oldest first).
        """
        try:
            # Find and update in one atomic operation
            job_doc = await self.collection.find_one_and_update(
                {
                    "status": JobStatus.PENDING.value,
                    "scheduled_at": {"$lte": datetime.utcnow()}
                },
                {
                    "$set": {
                        "status": JobStatus.PROCESSING.value,
                        "started_at": datetime.utcnow()
                    }
                },
                sort=[("priority", 1), ("created_at", 1)],
                return_document=True
            )
            
            return job_doc
            
        except Exception as ex:
            logger.error(f"Error getting next job: {ex}")
            return None

    async def _process_job(self, job: ProcessingJob, worker_id: int) -> bool:
        """
        Process a single job based on its type.
        Returns True if successful, False if failed.
        """
        try:
            processor = self.processors.get(job.site)
            if not processor:
                logger.error(f"No processor found for site {job.site}")
                return False
            
            site_settings = await SiteSettings(self.db).get(job.site)
            
            if job.job_type in [JobType.RAW_INSERT, JobType.RAW_UPDATE, JobType.RAW_PRICE_CHANGE, JobType.RAW_STATUS_CHANGE]:
                return await self._process_raw_job(job, processor, site_settings, worker_id)
            
            elif job.job_type == JobType.SITE_FILTERS_CHANGED:
                return await self._process_filter_changes(job, processor, site_settings, worker_id)
            
            elif job.job_type == JobType.SITE_PRICING_CHANGED:
                return await self._process_pricing_changes(job, processor, site_settings, worker_id)
            
            elif job.job_type == JobType.SITE_REBUILD:
                return await self._process_site_rebuild_job(job, processor, worker_id)
                
            else:
                logger.error(f"Unknown job type: {job.job_type}")
                return False
                
        except Exception as ex:
            logger.error(f"Error processing job {job.job_id}: {ex}")
            return False

    async def _process_raw_job(self, job: ProcessingJob, processor: Processor, site_settings: Dict[str, Any], worker_id: int) -> bool:
        """
        Process raw collection change jobs.
        """
        try:
            car_id = job.payload.get("car_id")
            raw_document = job.payload.get("raw_document")
            
            if job.job_type == JobType.RAW_STATUS_CHANGE:
                # Handle status change to false - mark processed as inactive
                result = await self.db[f"processed_{job.site}"].update_one(
                    {"im_ad_id": car_id},
                    {"$set": {"im_status": False, "updated_at": datetime.utcnow()}}
                )
                
                if result.modified_count > 0:
                    logger.debug(f"Worker {worker_id} marked car_id={car_id} inactive for site {job.site}")
                
                return True
            
            else:
                # Process the raw record normally
                processed = await processor.process_single_record(raw_document, site_settings)
                
                if processed:
                    # Store in processed collection (triggers WP job queue via change streams)
                    await self.db[f"processed_{job.site}"].update_one(
                        {"im_ad_id": processed["im_ad_id"]}, 
                        {"$set": processed}, 
                        upsert=True
                    )
                    
                    logger.debug(f"Worker {worker_id} processed car_id={car_id} for site {job.site}")
                    return True
                else:
                    logger.debug(f"Worker {worker_id} excluded car_id={car_id} for site {job.site}")
                    return True  # Not an error - just excluded by filters
            
        except Exception as ex:
            logger.error(f"Error in raw job processing: {ex}")
            return False

    async def _process_filter_changes(self, job: ProcessingJob, processor: Processor, site_settings: Dict[str, Any], worker_id: int) -> bool:
    
        try:
            # FORCE clear site settings cache and reload fresh settings
            from site_settings import SiteSettings
            SiteSettings.clear_cache(job.site)
            fresh_site_settings = await SiteSettings(self.db).get(job.site)
            
            payload = job.payload
            batch_number = payload.get("batch_number")
            
            logger.info(f"Worker {worker_id} processing filter changes for site {job.site}")
            
            # Get new filter criteria
            new_filters = fresh_site_settings.get("filter_criteria", {})
            processed_collection = self.db[f"processed_{job.site}"]
            
            if batch_number is not None:
                # Batch processing for large operations
                skip = payload.get("skip", 0)
                limit = payload.get("limit", self.batch_size)
                
                # Get current batch of processed records
                current_processed = {}
                async for doc in processed_collection.find({}).skip(skip).limit(limit):
                    car_id = doc.get("im_ad_id")
                    if car_id:
                        current_processed[car_id] = doc
                
                # Process corresponding raw records in this batch
                raw_cursor = self.db.raw.find({"listing_status": True}).skip(skip).limit(limit)
                should_be_processed = {}  # car_id -> processed_doc
                
                async for raw_doc in raw_cursor:
                    car_id = raw_doc.get("car_id")
                    if not car_id:
                        continue
                    
                    # Check if this raw record should be processed with new filters
                    if check_raw_against_filters(raw_doc, new_filters):
                        # Process the record
                        processed = await processor.process_single_record(raw_doc, fresh_site_settings)
                        if processed:
                            should_be_processed[car_id] = processed
                
                # Find records to remove (in current_processed but not in should_be_processed)
                to_remove = set(current_processed.keys()) - set(should_be_processed.keys())
                
                # Find records to add/update (in should_be_processed)
                to_upsert = should_be_processed
                
                removed_count = 0
                for car_id in to_remove:
                    # First get the record to extract WP info for delete job
                    record_to_delete = await processed_collection.find_one({"im_ad_id": car_id})
                    
                    if record_to_delete:
                        # Create WP delete job BEFORE deleting from processed
                        wp_post_id = record_to_delete.get("wp_post_id")
                        queue = self.wp_queues.get(job.site)
                        
                        if queue:
                            await queue.enqueue_job(
                                action="delete",
                                ad_id=car_id,
                                post_id=wp_post_id,
                                changed_fields=["status"],
                                hash_groups={},
                                reason="filter_exclusion"
                            )
                            logger.debug(f"Worker {worker_id} queued WP delete for car_id={car_id}")
                    
                    # Now delete from processed
                    result = await processed_collection.delete_one({"im_ad_id": car_id})
                    if result.deleted_count > 0:
                        removed_count += 1
                        logger.debug(f"Worker {worker_id} removed car_id={car_id} (no longer passes filters)")
                
                # Add/update records that pass new filters
                upserted_count = 0
                for car_id, processed_doc in to_upsert.items():
                    await processed_collection.update_one(
                        {"im_ad_id": car_id},
                        {"$set": processed_doc},
                        upsert=True
                    )
                    upserted_count += 1
                    logger.debug(f"Worker {worker_id} upserted car_id={car_id} (passes new filters)")
                
                logger.info(f"Worker {worker_id} completed filter batch {batch_number + 1}/{payload.get('total_batches')} for site {job.site}: removed {removed_count}, upserted {upserted_count}")
                
            else:
                # Non-batch processing (full site) - using cursors for memory efficiency
                logger.info(f"Worker {worker_id} starting full filter processing for site {job.site}")
                
                # Step 1: Get all current processed record IDs
                current_processed_ids = set()
                async for doc in processed_collection.find({}, {"im_ad_id": 1}):
                    car_id = doc.get("im_ad_id")
                    if car_id:
                        current_processed_ids.add(car_id)
                
                logger.info(f"Worker {worker_id} found {len(current_processed_ids)} existing processed records")
                
                # Step 2: Process all raw records with new filters
                should_be_processed_ids = set()
                upserted_count = 0
                
                async for raw_doc in self.db.raw.find({"listing_status": True}):
                    car_id = raw_doc.get("car_id")
                    if not car_id:
                        continue
                    
                    # Check if this raw record should be processed with new filters
                    if check_raw_against_filters(raw_doc, new_filters):
                        should_be_processed_ids.add(car_id)
                        
                        # Process and store the record
                        processed = await processor.process_single_record(raw_doc, site_settings)
                        if processed:
                            await processed_collection.update_one(
                                {"im_ad_id": car_id},
                                {"$set": processed},
                                upsert=True
                            )
                            upserted_count += 1
                
                logger.info(f"Worker {worker_id} processed {upserted_count} records that pass new filters")
                
                # Step 3: Remove records that no longer pass filters
                to_remove_ids = current_processed_ids - should_be_processed_ids
                removed_count = 0

                for car_id in to_remove_ids:
                    # First get the record to extract WP info for delete job
                    record_to_delete = await processed_collection.find_one({"im_ad_id": car_id})
                    
                    if record_to_delete:
                        # Create WP delete job BEFORE deleting from processed
                        wp_post_id = record_to_delete.get("wp_post_id")
                        queue = self.wp_queues.get(job.site)
                        
                        if queue:
                            await queue.enqueue_job(
                                action="delete",
                                ad_id=car_id,
                                post_id=wp_post_id,
                                changed_fields=["status"],
                                hash_groups={},
                                reason="filter_exclusion"
                            )
                    
                    # Now delete from processed
                    result = await processed_collection.delete_one({"im_ad_id": car_id})
                    if result.deleted_count > 0:
                        removed_count += 1
                
                logger.info(f"Worker {worker_id} completed full filter processing for site {job.site}: removed {removed_count}, upserted {upserted_count}")
            
            return True
            
        except Exception as ex:
            logger.error(f"Error in filter changes processing: {ex}")
            return False

    async def _process_pricing_changes(self, job: ProcessingJob, processor: Processor, site_settings: Dict[str, Any], worker_id: int) -> bool:
        
        try:
            # FORCE clear site settings cache and reload fresh settings
            from site_settings import SiteSettings
            SiteSettings.clear_cache(job.site)
            fresh_site_settings = await SiteSettings(self.db).get(job.site)
            
            payload = job.payload
            
            batch_number = payload.get("batch_number")
            
            if batch_number is not None:
                # Batch processing
                skip = payload.get("skip", 0)
                limit = payload.get("limit", self.batch_size)
                
                cursor = self.db.raw.find({"listing_status": True}).skip(skip).limit(limit)
                processed_count = 0
                
                async for raw_doc in cursor:
                    try:
                        processed = await processor.process_single_record(raw_doc, fresh_site_settings)
                        
                        if processed:
                            await self.db[f"processed_{job.site}"].update_one(
                                {"im_ad_id": processed["im_ad_id"]}, 
                                {"$set": processed}, 
                                upsert=True
                            )
                            processed_count += 1
                    
                    except Exception as ex:
                        logger.error(f"Error processing record in pricing batch: {ex}")
                        continue
                
                logger.info(f"Worker {worker_id} completed pricing batch {batch_number + 1}/{payload.get('total_batches')} for site {job.site}: {processed_count} records")
                
            else:
                # Full site processing (fallback for non-batch jobs)
                cursor = self.db.raw.find({"listing_status": True})
                processed_count = 0
                
                async for raw_doc in cursor:
                    try:
                        processed = await processor.process_single_record(raw_doc, fresh_site_settings)
                        
                        if processed:
                            await self.db[f"processed_{job.site}"].update_one(
                                {"im_ad_id": processed["im_ad_id"]}, 
                                {"$set": processed}, 
                                upsert=True
                            )
                            processed_count += 1
                    
                    except Exception as ex:
                        logger.error(f"Error processing record in pricing: {ex}")
                        continue
                
                logger.info(f"Worker {worker_id} completed full pricing processing for {job.site}: {processed_count} records")
            
            return True
            
        except Exception as ex:
            logger.error(f"Error in pricing changes processing: {ex}")
            return False

    async def _process_site_rebuild_job(self, job: ProcessingJob, processor: Processor, worker_id: int) -> bool:
        """
        Process site rebuild jobs.
        """
        try:
            await processor.run()  # This does the full site rebuild
            logger.info(f"Worker {worker_id} completed site rebuild for {job.site}")
            return True
            
        except Exception as ex:
            logger.error(f"Error in site rebuild job: {ex}")
            return False

    async def _mark_job_completed(self, job_id: str, processing_time: float):
        """Mark a job as completed."""
        await self.collection.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": JobStatus.COMPLETED.value,
                    "completed_at": datetime.utcnow(),
                    "processing_time": processing_time
                }
            }
        )

    async def _mark_job_failed(self, job: ProcessingJob):
        """Mark a job as failed and handle retry logic."""
        job.retries += 1
        
        if job.retries >= job.max_retries:
            # Move to dead letter queue
            status = JobStatus.DEAD_LETTER.value
            scheduled_at = None
            logger.warning(f"Job {job.job_id} moved to dead letter queue after {job.retries} retries")
        else:
            # Schedule for retry with exponential backoff
            status = JobStatus.PENDING.value
            scheduled_at = datetime.utcnow() + timedelta(seconds=job.retry_delay_seconds * (2 ** (job.retries - 1)))
            self.stats['jobs_retried'] += 1
            logger.info(f"Job {job.job_id} scheduled for retry {job.retries}/{job.max_retries} at {scheduled_at}")
        
        await self.collection.update_one(
            {"job_id": job.job_id},
            {
                "$set": {
                    "status": status,
                    "retries": job.retries,
                    "scheduled_at": scheduled_at,
                    "error_message": f"Failed after {job.retries} attempts"
                }
            }
        )

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics."""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            status_counts = {}
            async for doc in self.collection.aggregate(pipeline):
                status_counts[doc["_id"]] = doc["count"]
            
            # Get per-site statistics
            site_pipeline = [
                {
                    "$group": {
                        "_id": {"site": "$site", "status": "$status"},
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            site_stats = {}
            async for doc in self.collection.aggregate(site_pipeline):
                site = doc["_id"]["site"]
                status = doc["_id"]["status"]
                if site not in site_stats:
                    site_stats[site] = {}
                site_stats[site][status] = doc["count"]
            
            return {
                "total_jobs": sum(status_counts.values()),
                "status_breakdown": status_counts,
                "site_breakdown": site_stats,
                "workers_running": self.workers_running,
                "active_workers": len(self.worker_tasks),
                "processing_stats": self.stats,
                "configuration": {
                    "max_workers": self.max_workers,
                    "batch_size": self.batch_size,
                    "poll_interval": self.poll_interval,
                    "timeout_short": config.QUEUE_TIMEOUT_SHORT,
                    "timeout_medium": config.QUEUE_TIMEOUT_MEDIUM,
                    "timeout_long": config.QUEUE_TIMEOUT_LONG,
                    "retry_limit": config.QUEUE_RETRY_LIMIT
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as ex:
            logger.error(f"Error getting queue stats: {ex}")
            return {"error": str(ex)}

    async def cleanup_completed_jobs(self, older_than_hours: int = None):
        """Clean up completed jobs older than specified hours."""
        try:
            hours = older_than_hours or config.JOB_CLEANUP_RETENTION_HOURS
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            result = await self.collection.delete_many({
                "status": JobStatus.COMPLETED.value,
                "completed_at": {"$lt": cutoff_time}
            })
            
            logger.info(f"Cleaned up {result.deleted_count} completed jobs older than {hours} hours")
            return result.deleted_count
            
        except Exception as ex:
            logger.error(f"Error cleaning up jobs: {ex}")
            return 0

    async def retry_failed_jobs(self, limit: int = 100) -> int:
        """Retry failed jobs (move back to pending)."""
        try:
            result = await self.collection.update_many(
                {
                    "status": JobStatus.FAILED.value,
                    "retries": {"$lt": config.QUEUE_RETRY_LIMIT}
                },
                {
                    "$set": {
                        "status": JobStatus.PENDING.value,
                        "scheduled_at": datetime.utcnow()
                    }
                },
                limit=limit
            )
            
            logger.info(f"Retried {result.modified_count} failed jobs")
            return result.modified_count
            
        except Exception as ex:
            logger.error(f"Error retrying failed jobs: {ex}")
            return 0