"""
jobqueue.py

WP sync job queue module for rawprocessor.
- Async Motor, per-site queueing.
- Enqueues jobs (create, update, unpublish, delete) only if needed.
- Full job schema, retry tracking, logging, no silent skips.
"""

from typing import Any, Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger
from bson import ObjectId

QUEUE_ACTIONS = {"create", "update", "unpublish", "delete"}
RETRY_LIMIT_DEFAULT = 3

class WPQueue:
    def __init__(self, db: AsyncIOMotorDatabase, site: str, retry_limit: int = RETRY_LIMIT_DEFAULT):
        self.db = db
        self.site = site
        self.collection = db[f"wp_sync_queue_{site}"]
        self.retry_limit = retry_limit

    async def enqueue_job(
        self,
        action: str,
        ad_id: str,
        post_id: Optional[int],
        changed_fields: List[str],
        hash_groups: Dict[str, str],
        retries: int = 0,
        meta: Optional[Dict[str, Any]] = None,
        reason: Optional[str] = None,
    ) -> None:
        if action not in QUEUE_ACTIONS:
            logger.error(f"Invalid queue action: {action}")
            raise ValueError(f"Invalid action: {action}")

        job = {
            "action": action,
            "ad_id": ad_id,
            "post_id": post_id,
            "changed_fields": changed_fields,
            "hash_groups": hash_groups,
            "retries": retries,
            "meta": meta or {},
            "reason": reason,
            "status": "pending"
        }
        try:
            await self.collection.insert_one(job)
            logger.info(f"Enqueued job: {action} for ad_id={ad_id}, fields={changed_fields}")
        except Exception as e:
            logger.opt(exception=True).error(f"Failed to enqueue job for ad_id={ad_id}: {e}")
            raise

    async def mark_failed(self, job_id: ObjectId, error_msg: str) -> None:
        job = await self.collection.find_one({"_id": job_id})
        if not job:
            logger.error(f"Job {job_id} not found for failure marking")
            return

        retries = job.get("retries", 0) + 1
        if retries >= self.retry_limit:
            status = "exhausted"
            logger.warning(f"Job {job_id} hit retry limit ({self.retry_limit})")
        else:
            status = "pending"

        await self.collection.update_one(
            {"_id": job_id},
            {"$set": {"status": status, "retries": retries, "reason": error_msg}}
        )

    async def get_pending_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        cursor = self.collection.find({"status": "pending"}).limit(limit)
        return [doc async for doc in cursor]

    async def retry_failed_jobs(self, limit: int = 100) -> int:
        count = 0
        async for job in self.collection.find({"status": "failed", "retries": {"$lt": self.retry_limit}}).limit(limit):
            job_id = job["_id"]
            await self.collection.update_one({"_id": job_id}, {"$set": {"status": "pending"}})
            logger.info(f"Retrying job {job_id} (retry {job.get('retries', 0) + 1})")
            count += 1
        return count
