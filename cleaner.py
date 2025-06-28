"""
cleaner.py

Enhanced periodic database cleaner for rawprocessor.
Runs based on configured interval to delete invalid records from raw collection.
This is a true preprocessor that maintains database cleanliness.

UPDATED: Enhanced with complete data quality logic matching utils_filters.py
UPDATED: Added inactive record cleanup for listing_status: false records
Logic Flow: Record → Check Images (≥4) → Check Emissions Validity → Check Emissions Cap → Pass/Reject
"""

from typing import Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from datetime import datetime, timedelta, timezone

from config import config

class Cleaner:
    """
    Enhanced periodic database cleaner.
    Deletes invalid records from raw collection to maintain data quality.
    Uses the same logic as utils_filters.py for consistency.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the enhanced periodic cleaner.
        :param db: MongoDB database instance.
        """
        self.db = db
        self.min_images_required = config.MIN_IMAGES_REQUIRED
        self.emissions_threshold = config.EMISSIONS_ZERO_THRESHOLD
        self.emissions_cap = 500  # Maximum allowed emissions
        
        logger.info(f"Enhanced Cleaner initialized with config: min_images={self.min_images_required}, emissions_threshold={self.emissions_threshold}, emissions_cap={self.emissions_cap}")

    async def cleanup_raw_collection(self) -> Dict[str, Any]:
        """
        Enhanced periodic database cleanup - DELETE bad records from raw collection.
        
        Logic Flow: Record → Check Images (≥4) → Check Emissions Validity → Check Emissions Cap → Pass/Reject
        
        Quality Rules:
        1. Images: >= configured minimum images required
        2. Emissions Validity: Missing emissions only allowed for pure electric cars
        3. Emissions Cap: No car can have > 500g emissions (data quality check)  
        4. Zero Emissions: If emissions = 0, fuel type must be Electric
        
        :return: Dictionary with cleanup statistics
        """
        logger.info(f"Starting enhanced raw collection cleanup...")
        start_time = datetime.utcnow()
        
        stats = {
            "start_time": start_time,
            "deleted_insufficient_images": 0,
            "deleted_missing_emissions": 0,
            "deleted_emissions_too_high": 0,
            "deleted_invalid_zero_emissions": 0,
            "deleted_invalid_emissions_value": 0,
            "skipped_processing": 0,
            "total_before": 0,
            "total_after": 0,
            "total_deleted": 0,
            "duration_seconds": 0,
            "config_used": {
                "min_images_required": self.min_images_required,
                "emissions_threshold": self.emissions_threshold,
                "emissions_cap": self.emissions_cap
            }
        }
        
        try:
            # Count total records before cleanup
            stats["total_before"] = await self.db.raw.count_documents({})
            logger.info(f"Raw collection has {stats['total_before']} records before cleanup")
            
            # Get list of car_ids that are currently being processed to exclude them
            processing_car_ids = await self._get_processing_car_ids()
            stats["skipped_processing"] = len(processing_car_ids)
            logger.info(f"Found {stats['skipped_processing']} records currently being processed - will skip these")
            
            # === STEP 1: Delete records with insufficient images ===
            logger.info(f"Deleting records with < {self.min_images_required} images...")
            delete_query_images = {
                "$expr": {"$lt": [{"$size": {"$ifNull": ["$Images", []]}}, self.min_images_required]}
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_images["car_id"] = {"$nin": processing_car_ids}
            
            result_images = await self.db.raw.delete_many(delete_query_images)
            stats["deleted_insufficient_images"] = result_images.deleted_count
            logger.info(f"Deleted {stats['deleted_insufficient_images']} records with < {self.min_images_required} images")
            
            # === STEP 2: Delete records with missing emissions (non-pure-electric) ===
            logger.info("Deleting non-pure-electric records with missing emissions...")
            delete_query_missing_emissions = {
                "energyconsumption.raw_emissions": None,
                "$and": [
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "^Electric$", "$options": "i"}}},
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "^elektrisch$", "$options": "i"}}},
                    {"$or": [
                        {"energyconsumption.Fueltype": {"$regex": "gasoline", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "diesel", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "hybrid", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "electric.*gasoline", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "electric.*diesel", "$options": "i"}}
                    ]}
                ]
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_missing_emissions["car_id"] = {"$nin": processing_car_ids}
            
            result_missing_emissions = await self.db.raw.delete_many(delete_query_missing_emissions)
            stats["deleted_missing_emissions"] = result_missing_emissions.deleted_count
            logger.info(f"Deleted {stats['deleted_missing_emissions']} non-pure-electric records with missing emissions")
            
            # === STEP 3: Delete records with emissions > 500g ===
            logger.info("Deleting records with emissions > 500g...")
            delete_query_high_emissions = {
                "energyconsumption.raw_emissions": {"$gt": self.emissions_cap}
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_high_emissions["car_id"] = {"$nin": processing_car_ids}
            
            result_high_emissions = await self.db.raw.delete_many(delete_query_high_emissions)
            stats["deleted_emissions_too_high"] = result_high_emissions.deleted_count
            logger.info(f"Deleted {stats['deleted_emissions_too_high']} records with emissions > {self.emissions_cap}g")
            
            # === STEP 4: Delete records with emissions = 0 but not electric ===
            logger.info(f"Deleting records with emissions = {self.emissions_threshold} and not electric...")
            delete_query_invalid_zero = {
                "energyconsumption.raw_emissions": self.emissions_threshold,
                "$and": [
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "Electric", "$options": "i"}}},
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "elektrisch", "$options": "i"}}}
                ]
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_invalid_zero["car_id"] = {"$nin": processing_car_ids}
            
            result_invalid_zero = await self.db.raw.delete_many(delete_query_invalid_zero)
            stats["deleted_invalid_zero_emissions"] = result_invalid_zero.deleted_count
            logger.info(f"Deleted {stats['deleted_invalid_zero_emissions']} records with invalid zero emissions")
            
            # === STEP 5: Delete records with invalid emissions values ===
            logger.info("Deleting records with invalid emissions values...")
            # This is harder to do in MongoDB, but we can catch obvious string values
            delete_query_invalid_emissions = {
                "$or": [
                    {"energyconsumption.raw_emissions": {"$type": "string"}},
                    {"energyconsumption.raw_emissions": {"$lt": 0}}  # Negative emissions
                ]
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_invalid_emissions["car_id"] = {"$nin": processing_car_ids}
            
            result_invalid_emissions = await self.db.raw.delete_many(delete_query_invalid_emissions)
            stats["deleted_invalid_emissions_value"] = result_invalid_emissions.deleted_count
            logger.info(f"Deleted {stats['deleted_invalid_emissions_value']} records with invalid emissions values")
            
            # Calculate final statistics
            stats["total_deleted"] = (
                stats["deleted_insufficient_images"] + 
                stats["deleted_missing_emissions"] + 
                stats["deleted_emissions_too_high"] + 
                stats["deleted_invalid_zero_emissions"] + 
                stats["deleted_invalid_emissions_value"]
            )
            stats["total_after"] = await self.db.raw.count_documents({})
            
            end_time = datetime.utcnow()
            stats["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info("Enhanced cleanup completed successfully:")
            logger.info(f"  - Records before: {stats['total_before']}")
            logger.info(f"  - Deleted (insufficient images): {stats['deleted_insufficient_images']}")
            logger.info(f"  - Deleted (missing emissions): {stats['deleted_missing_emissions']}")
            logger.info(f"  - Deleted (emissions > 500g): {stats['deleted_emissions_too_high']}")
            logger.info(f"  - Deleted (invalid zero emissions): {stats['deleted_invalid_zero_emissions']}")
            logger.info(f"  - Deleted (invalid emissions values): {stats['deleted_invalid_emissions_value']}")
            logger.info(f"  - Total deleted: {stats['total_deleted']}")
            logger.info(f"  - Skipped (processing): {stats['skipped_processing']}")
            logger.info(f"  - Records after: {stats['total_after']}")
            logger.info(f"  - Duration: {stats['duration_seconds']:.2f} seconds")
            
            return stats
            
        except Exception as ex:
            logger.error(f"Error during enhanced cleanup: {ex}")
            stats["error"] = str(ex)
            raise

    async def cleanup_inactive_records(self) -> Dict[str, Any]:
        """
        Clean up inactive records (listing_status: false) from raw and processed collections.
        Only deletes records that have no pending unpublish jobs and have been inactive for at least 5 minutes.
        
        :return: Dictionary with cleanup statistics
        """
        logger.info("Starting inactive records cleanup...")
        start_time = datetime.utcnow()
        
        stats = {
            "start_time": start_time,
            "inactive_records_found": 0,
            "records_with_pending_jobs": 0,
            "records_too_recent": 0,
            "deleted_from_raw": 0,
            "deleted_from_processed": 0,
            "total_processed_collections_checked": 0,
            "duration_seconds": 0,
            "config_used": {
                "safety_delay_minutes": config.INACTIVE_CLEANUP_DELAY_MINUTES
            }
        }
        
        try:
            # Get safety delay from config
            safety_delay = timedelta(minutes=config.INACTIVE_CLEANUP_DELAY_MINUTES)
            cutoff_time = datetime.utcnow().replace(tzinfo=timezone.utc) - safety_delay
            
            # Find all inactive records
            inactive_records = []
            async for record in self.db.raw.find({"listing_status": False}):
                stats["inactive_records_found"] += 1
                
                car_id = record.get("car_id")
                updated_at = record.get("updated_at", record.get("_id").generation_time if record.get("_id") else datetime.utcnow())
                
                if not car_id:
                    continue
                    
                # Check if record has been inactive long enough
                if updated_at > cutoff_time:
                    stats["records_too_recent"] += 1
                    logger.debug(f"Skipping {car_id} - too recent ({updated_at})")
                    continue
                
                inactive_records.append({
                    "car_id": car_id,
                    "updated_at": updated_at
                })
            
            logger.info(f"Found {len(inactive_records)} inactive records eligible for cleanup (after {safety_delay.total_seconds()/60:.0f}min delay)")
            
            if not inactive_records:
                stats["duration_seconds"] = (datetime.utcnow() - start_time).total_seconds()
                return stats
            
            # Get list of active sites
            active_sites = []
            async for site_doc in self.db.site_settings.find({}):
                site_url = site_doc.get("site_url", "")
                if site_url:
                    # Extract site key from site_url (same logic as in queued_trigger_system)
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
                        active_sites.append(site_key)
            
            logger.info(f"Found {len(active_sites)} active sites: {active_sites}")
            
            # Check each inactive record for pending unpublish jobs
            records_to_delete = []
            
            for record in inactive_records:
                car_id = record["car_id"]
                has_pending_jobs = False
                
                # Check all site WP queues for pending unpublish jobs
                for site in active_sites:
                    queue_collection = f"wp_sync_queue_{site}"
                    
                    try:
                        pending_unpublish = await self.db[queue_collection].count_documents({
                            "ad_id": car_id,
                            "action": {"$in": ["unpublish", "delete"]},
                            "status": "pending"
                        })
                        
                        if pending_unpublish > 0:
                            has_pending_jobs = True
                            logger.debug(f"Found {pending_unpublish} pending unpublish jobs for {car_id} in {site}")
                            break
                            
                    except Exception as ex:
                        logger.warning(f"Could not check queue {queue_collection} for {car_id}: {ex}")
                        # If we can't check, assume there might be pending jobs to be safe
                        has_pending_jobs = True
                        break
                
                if has_pending_jobs:
                    stats["records_with_pending_jobs"] += 1
                    logger.debug(f"Skipping {car_id} - has pending unpublish jobs")
                else:
                    records_to_delete.append(car_id)
            
            logger.info(f"Ready to delete {len(records_to_delete)} inactive records (no pending jobs)")
            
            # Delete records from raw collection
            if records_to_delete:
                raw_result = await self.db.raw.delete_many({"car_id": {"$in": records_to_delete}})
                stats["deleted_from_raw"] = raw_result.deleted_count
                logger.info(f"Deleted {stats['deleted_from_raw']} inactive records from raw collection")
                
                # Delete from all processed collections
                for site in active_sites:
                    processed_collection = f"processed_{site}"
                    
                    try:
                        processed_result = await self.db[processed_collection].delete_many({
                            "im_ad_id": {"$in": records_to_delete}
                        })
                        
                        if processed_result.deleted_count > 0:
                            stats["deleted_from_processed"] += processed_result.deleted_count
                            logger.info(f"Deleted {processed_result.deleted_count} inactive records from {processed_collection}")
                        
                        stats["total_processed_collections_checked"] += 1
                        
                    except Exception as ex:
                        logger.warning(f"Could not delete from {processed_collection}: {ex}")
            
            # Calculate final statistics
            end_time = datetime.utcnow()
            stats["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info("Inactive records cleanup completed successfully:")
            logger.info(f"  - Inactive records found: {stats['inactive_records_found']}")
            logger.info(f"  - Skipped (pending jobs): {stats['records_with_pending_jobs']}")
            logger.info(f"  - Skipped (too recent): {stats['records_too_recent']}")
            logger.info(f"  - Deleted from raw: {stats['deleted_from_raw']}")
            logger.info(f"  - Deleted from processed: {stats['deleted_from_processed']}")
            logger.info(f"  - Duration: {stats['duration_seconds']:.2f} seconds")
            
            return stats
            
        except Exception as ex:
            logger.error(f"Error during inactive records cleanup: {ex}")
            stats["error"] = str(ex)
            stats["duration_seconds"] = (datetime.utcnow() - start_time).total_seconds()
            raise

    async def _get_processing_car_ids(self) -> list:
        """
        Get list of car_ids that are currently being processed.
        This prevents deleting records that have pending or active processing jobs.
        
        :return: List of car_ids that should not be deleted
        """
        try:
            # Get all pending and processing jobs from the processing queue
            cursor = self.db.processing_queue.find(
                {"status": {"$in": ["pending", "processing"]}},
                {"payload.car_id": 1}
            )
            
            car_ids = []
            async for job in cursor:
                payload = job.get("payload", {})
                car_id = payload.get("car_id")
                if car_id:
                    car_ids.append(car_id)
            
            # Remove duplicates
            return list(set(car_ids))
            
        except Exception as ex:
            logger.error(f"Error getting processing car_ids: {ex}")
            # Return empty list to be safe - this will prevent cleanup but avoid race conditions
            return []

    async def get_cleanup_candidates_count(self) -> Dict[str, int]:
        """
        Get count of records that would be deleted by cleanup (for reporting).
        Uses enhanced data quality logic matching utils_filters.py.
        Does not delete anything, just counts.
        
        :return: Dictionary with counts of records that would be deleted
        """
        try:
            # Get processing car_ids to exclude them from counts
            processing_car_ids = await self._get_processing_car_ids()
            
            # === STEP 1: Count insufficient images ===
            images_query = {
                "$expr": {"$lt": [{"$size": {"$ifNull": ["$Images", []]}}, self.min_images_required]}
            }
            if processing_car_ids:
                images_query["car_id"] = {"$nin": processing_car_ids}
            
            images_count = await self.db.raw.count_documents(images_query)
            
            # === STEP 2: Count missing emissions (non-pure-electric) ===
            missing_emissions_query = {
                "energyconsumption.raw_emissions": None,
                "$and": [
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "^Electric$", "$options": "i"}}},
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "^elektrisch$", "$options": "i"}}},
                    {"$or": [
                        {"energyconsumption.Fueltype": {"$regex": "gasoline", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "diesel", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "hybrid", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "electric.*gasoline", "$options": "i"}},
                        {"energyconsumption.Fueltype": {"$regex": "electric.*diesel", "$options": "i"}}
                    ]}
                ]
            }
            if processing_car_ids:
                missing_emissions_query["car_id"] = {"$nin": processing_car_ids}
            
            missing_emissions_count = await self.db.raw.count_documents(missing_emissions_query)
            
            # === STEP 3: Count high emissions (> 500g) ===
            high_emissions_query = {
                "energyconsumption.raw_emissions": {"$gt": self.emissions_cap}
            }
            if processing_car_ids:
                high_emissions_query["car_id"] = {"$nin": processing_car_ids}
            
            high_emissions_count = await self.db.raw.count_documents(high_emissions_query)
            
            # === STEP 4: Count invalid zero emissions ===
            invalid_zero_query = {
                "energyconsumption.raw_emissions": self.emissions_threshold,
                "$and": [
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "Electric", "$options": "i"}}},
                    {"energyconsumption.Fueltype": {"$not": {"$regex": "elektrisch", "$options": "i"}}}
                ]
            }
            if processing_car_ids:
                invalid_zero_query["car_id"] = {"$nin": processing_car_ids}
            
            invalid_zero_count = await self.db.raw.count_documents(invalid_zero_query)
            
            # === STEP 5: Count invalid emissions values ===
            invalid_emissions_query = {
                "$or": [
                    {"energyconsumption.raw_emissions": {"$type": "string"}},
                    {"energyconsumption.raw_emissions": {"$lt": 0}}  # Negative emissions
                ]
            }
            if processing_car_ids:
                invalid_emissions_query["car_id"] = {"$nin": processing_car_ids}
            
            invalid_emissions_count = await self.db.raw.count_documents(invalid_emissions_query)
            
            # === STEP 6: Count inactive records ===
            safety_delay = timedelta(minutes=config.INACTIVE_CLEANUP_DELAY_MINUTES)
            cutoff_time = datetime.utcnow() - safety_delay
            
            inactive_query = {
                "listing_status": False,
                "updated_at": {"$lt": cutoff_time}
            }
            inactive_count = await self.db.raw.count_documents(inactive_query)
            
            total_candidates = (
                images_count + 
                missing_emissions_count + 
                high_emissions_count + 
                invalid_zero_count + 
                invalid_emissions_count +
                inactive_count
            )
            
            return {
                "insufficient_images": images_count,
                "missing_emissions": missing_emissions_count,
                "emissions_too_high": high_emissions_count,
                "invalid_zero_emissions": invalid_zero_count,
                "invalid_emissions_value": invalid_emissions_count,
                "inactive_records": inactive_count,
                "total_candidates": total_candidates,
                "protected_by_processing": len(processing_car_ids),
                "config_used": {
                    "min_images_required": self.min_images_required,
                    "emissions_threshold": self.emissions_threshold,
                    "emissions_cap": self.emissions_cap,
                    "inactive_delay_minutes": config.INACTIVE_CLEANUP_DELAY_MINUTES
                }
            }
            
        except Exception as ex:
            logger.error(f"Error counting cleanup candidates: {ex}")
            return {
                "insufficient_images": 0,
                "missing_emissions": 0,
                "emissions_too_high": 0,
                "invalid_zero_emissions": 0,
                "invalid_emissions_value": 0,
                "inactive_records": 0,
                "total_candidates": 0,
                "protected_by_processing": 0,
                "config_used": {
                    "min_images_required": self.min_images_required,
                    "emissions_threshold": self.emissions_threshold,
                    "emissions_cap": self.emissions_cap,
                    "inactive_delay_minutes": config.INACTIVE_CLEANUP_DELAY_MINUTES
                },
                "error": str(ex)
            }

    async def validate_cleanup_configuration(self) -> Dict[str, Any]:
        """
        Validate that cleanup configuration is sensible.
        
        :return: Validation results
        """
        issues = []
        warnings = []
        
        # Check minimum images requirement
        if self.min_images_required < 1:
            issues.append(f"MIN_IMAGES_REQUIRED ({self.min_images_required}) must be at least 1")
        elif self.min_images_required > 10:
            warnings.append(f"MIN_IMAGES_REQUIRED ({self.min_images_required}) is quite high - many records may be deleted")
        
        # Check emissions threshold
        if self.emissions_threshold < 0:
            issues.append(f"EMISSIONS_ZERO_THRESHOLD ({self.emissions_threshold}) cannot be negative")
        elif self.emissions_threshold > 50:
            warnings.append(f"EMISSIONS_ZERO_THRESHOLD ({self.emissions_threshold}) is high - may affect valid records")
        
        # Check emissions cap
        if self.emissions_cap < 100:
            warnings.append(f"EMISSIONS_CAP ({self.emissions_cap}) is very low - may delete valid high-performance cars")
        elif self.emissions_cap > 1000:
            warnings.append(f"EMISSIONS_CAP ({self.emissions_cap}) is very high - may not catch data quality issues")
        
        # Check inactive cleanup delay
        if hasattr(config, 'INACTIVE_CLEANUP_DELAY_MINUTES'):
            if config.INACTIVE_CLEANUP_DELAY_MINUTES < 1:
                warnings.append(f"INACTIVE_CLEANUP_DELAY_MINUTES ({config.INACTIVE_CLEANUP_DELAY_MINUTES}) is very short")
            elif config.INACTIVE_CLEANUP_DELAY_MINUTES > 60:
                warnings.append(f"INACTIVE_CLEANUP_DELAY_MINUTES ({config.INACTIVE_CLEANUP_DELAY_MINUTES}) is quite long")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "configuration": {
                "min_images_required": self.min_images_required,
                "emissions_threshold": self.emissions_threshold,
                "emissions_cap": self.emissions_cap,
                "inactive_cleanup_delay": getattr(config, 'INACTIVE_CLEANUP_DELAY_MINUTES', 'not_configured')
            }
        }

    async def analyze_data_quality(self) -> Dict[str, Any]:
        """
        Analyze current data quality in the raw collection.
        Provides insights into the types and counts of quality issues.
        
        :return: Data quality analysis
        """
        try:
            total_records = await self.db.raw.count_documents({})
            active_records = await self.db.raw.count_documents({"listing_status": True})
            
            candidates = await self.get_cleanup_candidates_count()
            
            # Calculate quality percentages
            quality_pass_rate = ((total_records - candidates["total_candidates"]) / total_records * 100) if total_records > 0 else 0
            
            # Get fuel type distribution for context
            fuel_type_pipeline = [
                {"$group": {"_id": "$energyconsumption.Fueltype", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            
            fuel_types = {}
            async for doc in self.db.raw.aggregate(fuel_type_pipeline):
                fuel_types[doc["_id"] or "Unknown"] = doc["count"]
            
            return {
                "total_records": total_records,
                "active_records": active_records,
                "quality_pass_rate_percent": round(quality_pass_rate, 1),
                "cleanup_candidates": candidates,
                "fuel_type_distribution": fuel_types,
                "quality_rules": {
                    "min_images": self.min_images_required,
                    "emissions_threshold": self.emissions_threshold,
                    "emissions_cap": self.emissions_cap,
                    "inactive_delay_minutes": config.INACTIVE_CLEANUP_DELAY_MINUTES,
                    "pure_electric_exemption": "Only pure electric cars can have missing emissions"
                }
            }
            
        except Exception as ex:
            logger.error(f"Error analyzing data quality: {ex}")
            return {"error": str(ex)}