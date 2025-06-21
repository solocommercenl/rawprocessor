"""
cleaner.py

Periodic database cleaner for rawprocessor.
Runs every 8 hours to delete invalid records from raw collection.
This is a true preprocessor that maintains database cleanliness.
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
        
        Deletes records that fail basic quality requirements:
        1. Records with < 4 images
        2. Records with 0 emissions AND not electric fuel type
        
        :return: Dictionary with cleanup statistics
        """
        logger.info("Starting periodic raw collection cleanup...")
        start_time = datetime.utcnow()
        
        stats = {
            "start_time": start_time,
            "deleted_images": 0,
            "deleted_emissions": 0,
            "total_before": 0,
            "total_after": 0,
            "total_deleted": 0,
            "duration_seconds": 0
        }
        
        try:
            # Count total records before cleanup
            stats["total_before"] = await self.db.raw.count_documents({})
            logger.info(f"Raw collection has {stats['total_before']} records before cleanup")
            
            # 1. Delete records with < 4 images
            logger.info("Deleting records with insufficient images...")
            result_images = await self.db.raw.delete_many({
                "$expr": {"$lt": [{"$size": {"$ifNull": ["$Images", []]}}, 4]}
            })
            stats["deleted_images"] = result_images.deleted_count
            logger.info(f"Deleted {stats['deleted_images']} records with < 4 images")
            
            # 2. Delete records with 0 emissions AND not electric fuel type
            logger.info("Deleting records with invalid emissions...")
            result_emissions = await self.db.raw.delete_many({
                "energyconsumption.raw_emissions": 0,
                "energyconsumption.Fueltype": {"$not": {"$regex": "Electric", "$options": "i"}}
            })
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
            logger.info(f"  - Records after: {stats['total_after']}")
            logger.info(f"  - Duration: {stats['duration_seconds']:.2f} seconds")
            
            return stats
            
        except Exception as ex:
            logger.error(f"Error during periodic cleanup: {ex}")
            stats["error"] = str(ex)
            raise

    async def get_cleanup_candidates_count(self) -> Dict[str, int]:
        """
        Get count of records that would be deleted by cleanup (for reporting).
        Does not delete anything, just counts.
        
        :return: Dictionary with counts of records that would be deleted
        """
        try:
            # Count records with < 4 images
            images_count = await self.db.raw.count_documents({
                "$expr": {"$lt": [{"$size": {"$ifNull": ["$Images", []]}}, 4]}
            })
            
            # Count records with 0 emissions AND not electric
            emissions_count = await self.db.raw.count_documents({
                "energyconsumption.raw_emissions": 0,
                "energyconsumption.Fueltype": {"$not": {"$regex": "Electric", "$options": "i"}}
            })
            
            return {
                "insufficient_images": images_count,
                "invalid_emissions": emissions_count,
                "total_candidates": images_count + emissions_count
            }
            
        except Exception as ex:
            logger.error(f"Error counting cleanup candidates: {ex}")
            return {
                "insufficient_images": 0,
                "invalid_emissions": 0,
                "total_candidates": 0,
                "error": str(ex)
            }