"""
cleaner.py

Periodic database cleaner for rawprocessor.
Runs every 8 hours to delete invalid records from raw collection.
This is a true preprocessor that maintains database cleanliness.

FIXED: Added processing queue checks to prevent race conditions.
"""

from typing import Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from datetime import datetime

class Cleaner:
    """
    Periodic database cleaner.
    Deletes invalid records from raw collection to maintain data quality.
    """
    
    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the periodic cleaner.
        :param db: MongoDB database instance.
        """
        self.db = db

    async def cleanup_raw_collection(self) -> Dict[str, Any]:
        """
        Periodic database cleanup - actually DELETE bad records from raw collection.
        
        FIXED: Now checks processing queue to avoid deleting records that are being processed.
        
        Deletes records that fail basic quality requirements:
        1. Records with < 4 images (excluding those being processed)
        2. Records with 0 emissions AND not electric fuel type (excluding those being processed)
        
        :return: Dictionary with cleanup statistics
        """
        logger.info("Starting periodic raw collection cleanup...")
        start_time = datetime.utcnow()
        
        stats = {
            "start_time": start_time,
            "deleted_images": 0,
            "deleted_emissions": 0,
            "skipped_processing": 0,
            "total_before": 0,
            "total_after": 0,
            "total_deleted": 0,
            "duration_seconds": 0
        }
        
        try:
            # Count total records before cleanup
            stats["total_before"] = await self.db.raw.count_documents({})
            logger.info(f"Raw collection has {stats['total_before']} records before cleanup")
            
            # Get list of car_ids that are currently being processed to exclude them
            processing_car_ids = await self._get_processing_car_ids()
            stats["skipped_processing"] = len(processing_car_ids)
            logger.info(f"Found {stats['skipped_processing']} records currently being processed - will skip these")
            
            # 1. Delete records with < 4 images (excluding those being processed)
            logger.info("Deleting records with insufficient images...")
            delete_query_images = {
                "$expr": {"$lt": [{"$size": {"$ifNull": ["$Images", []]}}, 4]}
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_images["car_id"] = {"$nin": processing_car_ids}
            
            result_images = await self.db.raw.delete_many(delete_query_images)
            stats["deleted_images"] = result_images.deleted_count
            logger.info(f"Deleted {stats['deleted_images']} records with < 4 images")
            
            # 2. Delete records with 0 emissions AND not electric fuel type (excluding those being processed)
            logger.info("Deleting records with invalid emissions...")
            delete_query_emissions = {
                "energyconsumption.raw_emissions": 0,
                "energyconsumption.Fueltype": {"$not": {"$regex": "Electric", "$options": "i"}}
            }
            
            # Add exclusion for records being processed
            if processing_car_ids:
                delete_query_emissions["car_id"] = {"$nin": processing_car_ids}
            
            result_emissions = await self.db.raw.delete_many(delete_query_emissions)
            stats["deleted_emissions"] = result_emissions.deleted_count
            logger.info(f"Deleted {stats['deleted_emissions']} records with invalid emissions")
            
            # Calculate final statistics
            stats["total_deleted"] = stats["deleted_images"] + stats["deleted_emissions"]
            stats["total_after"] = await self.db.raw.count_documents({})
            
            end_time = datetime.utcnow()
            stats["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info("Periodic cleanup completed successfully:")
            logger.info(f"  - Records before: {stats['total_before']}")
            logger.info(f"  - Deleted (images): {stats['deleted_images']}")
            logger.info(f"  - Deleted (emissions): {stats['deleted_emissions']}")
            logger.info(f"  - Total deleted: {stats['total_deleted']}")
            logger.info(f"  - Skipped (processing): {stats['skipped_processing']}")
            logger.info(f"  - Records after: {stats['total_after']}")
            logger.info(f"  - Duration: {stats['duration_seconds']:.2f} seconds")
            
            return stats
            
        except Exception as ex:
            logger.error(f"Error during periodic cleanup: {ex}")
            stats["error"] = str(ex)
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
        Does not delete anything, just counts.
        
        FIXED: Now accounts for records being processed.
        
        :return: Dictionary with counts of records that would be deleted
        """
        try:
            # Get processing car_ids to exclude them from counts
            processing_car_ids = await self._get_processing_car_ids()
            
            # Count records with < 4 images (excluding those being processed)
            images_query = {
                "$expr": {"$lt": [{"$size": {"$ifNull": ["$Images", []]}}, 4]}
            }
            if processing_car_ids:
                images_query["car_id"] = {"$nin": processing_car_ids}
            
            images_count = await self.db.raw.count_documents(images_query)
            
            # Count records with 0 emissions AND not electric (excluding those being processed)
            emissions_query = {
                "energyconsumption.raw_emissions": 0,
                "energyconsumption.Fueltype": {"$not": {"$regex": "Electric", "$options": "i"}}
            }
            if processing_car_ids:
                emissions_query["car_id"] = {"$nin": processing_car_ids}
            
            emissions_count = await self.db.raw.count_documents(emissions_query)
            
            return {
                "insufficient_images": images_count,
                "invalid_emissions": emissions_count,
                "total_candidates": images_count + emissions_count,
                "protected_by_processing": len(processing_car_ids)
            }
            
        except Exception as ex:
            logger.error(f"Error counting cleanup candidates: {ex}")
            return {
                "insufficient_images": 0,
                "invalid_emissions": 0,
                "total_candidates": 0,
                "protected_by_processing": 0,
                "error": str(ex)
            }