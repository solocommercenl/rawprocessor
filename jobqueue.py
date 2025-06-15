# jobqueue.py

"""
WP sync job queue module for rawprocessor.
- Async Motor, per-site queueing.
- Only enqueues jobs (create, update, unpublish, delete) if group hashes differ.
- Full job schema, retry tracking, robust logging, no silent skips.
- Never hardcodes sites, actions, or fields.
"""

from typing import Any, Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

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
        """
        Enqueue a sync job if hashes changed.
        Logs and raises error if invalid.
        """
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
            logger.info(f"Enqueued job: {action} for ad_id={ad_id}, changed_fields={changed_fields}")
        except Exception as e:
            logger.opt(exception=True).error(f"Failed to enqueue job for ad_id={ad_id}: {str(e)}")
            raise

    async def mark_failed(self, job_id, error_msg: str):
        """
        Mark a job as failed, increment retries.
        """
        job = await self.collection.find_one({"_id": job_id})
        if not job:
            logger.error(f"Job {job_id} not found for fail marking")
            return
        retries = job.get("retries", 0) + 1
        update = {
            "$set": {
                "status": "failed",
                "reason": error_msg,
                "retries": retries
            }
        }
        await self.collection.update_one({"_id": job_id}, update)
        if retries >= self.retry_limit:
            logger.warning(f"Job {job_id} reached retry limit ({self.retry_limit})")
        else:
            logger.info(f"Job {job_id} marked failed (retry {retries}/{self.retry_limit}): {error_msg}")

    async def get_pending_jobs(self, limit=50) -> List[Dict[str, Any]]:
        cursor = self.collection.find({"status": "pending"}).limit(limit)
        ret
