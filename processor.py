# processor.py

"""
Main orchestrator module for rawprocessor Stage 1.
- Loads per-site settings (filters, translation profile, etc).
- Enforces all filter_criteria and data validation before translation/calculation.
- Calls translator and calculator, checks group hashes for partial update logic.
- Writes eligible records to processed_{site}.
- Logs all processing steps, skips, and reasons.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple, Set, Callable, Union
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

from translator import Translator
# from calculator import calculate_financials  # Uncomment and implement in Session 3

class Processor:
    def __init__(self, db: AsyncIOMotorDatabase, site: str):
        """
        Args:
            db: AsyncIOMotorDatabase (Motor) instance.
            site: Site key (e.g. 'texbijl', 'mothisautomotive')
        """
        self.db = db
        self.site = site
        self.translator = Translator(db)
        # self.calculator = Calculator(db)  # Uncomment after implementation

    async def load_site_settings(self) -> Dict[str, Any]:
        """Load site settings for this site."""
        settings = await self.db.site_settings.find_one({"site_url": {"$regex": self.site, "$options": "i"}})
        if not settings:
            logger.error(f"[{self.site}] Site settings not found.")
            raise RuntimeError(f"Site settings not found for {self.site}")
        return settings

    def _matches_criteria(self, record: dict, filter_criteria: dict) -> bool:
        """
        Checks if a record matches all per-site filter_criteria.
        (Strict; skips if any filter is not matched.)
        """
        for key, cond in filter_criteria.items():
            val = record.get(key)
            if isinstance(cond, dict):
                # e.g. {"$gte": 2018, "$lte": 2023}
                if "$gte" in cond and (val is None or str(val) < str(cond["$gte"])):
                    return False
                if "$lte" in cond and (val is None or str(val) > str(cond["$lte"])):
                    return False
                if "$nin" in cond and val in cond["$nin"]:
                    return False
                if "$in" in cond and val not in cond["$in"]:
                    return False
            elif isinstance(cond, list):
                if val not in cond:
                    return False
            else:
                if val != cond:
                    return False
        return True

    def _get_hash_groups(self, record: dict, group_fields: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Returns dict of group_name: hash (simple stable hash for field set).
        """
        import hashlib, json
        hashes = {}
        for group, fields in group_fields.items():
            payload = {k: record.get(k) for k in fields}
            # Only hash if any field is present
            if any(v is not None for v in payload.values()):
                raw = json.dumps(payload, sort_keys=True, default=str).encode()
                hashes[group] = hashlib.sha256(raw).hexdigest()
        return hashes

    async def process_record(self, record: dict, record_id: Optional[Union[str, int]] = None) -> Optional[dict]:
        """
        Full processing for a single record:
        - Loads settings, enforces filter, translates, calculates, hashes, and writes to processed_{site}.

        Args:
            record: Cleaned input record (from raw).
            record_id: Optional, for logging.

        Returns:
            The final processed record (written to processed_{site}), or None if skipped/excluded.
        """
        site_settings = await self.load_site_settings()
        filter_criteria = site_settings.get("filter_criteria", {})
        site_url = site_settings.get("site_url", self.site)
        processed_collection = self.db[f"processed_{self.site}"]

        # 1. Filtering
        if not self._matches_criteria(record, filter_criteria):
            logger.info(f"[{self.site}] Record {record_id} excluded by filter_criteria.")
            return None

        # 2. Translation (returns dict of translated fields only)
        translated = await self.translator.translate_fields(
            record, site_settings, record_id=record_id, site=self.site
        )
        if not translated:
            logger.info(f"[{self.site}] Record {record_id} skipped: No fields could be translated.")
            return None

        # 3. Financial calculations (Session 3: implement calculator logic)
        # calculated = await self.calculator.calculate_financials(translated, site_settings)
        # For now, skip; just pass translated to output

        # 4. Hash groups for partial update
        group_fields = site_settings.get("hash_groups", {
            "pricing": ["im_price", "im_nett_price", "im_bpm_rate", "im_vat_amount"],
            "leasing": ["im_monthly_payment", "im_down_payment", "im_desired_remaining_debt"],
            "gallery": ["im_gallery"],
        })
        group_hashes = self._get_hash_groups(translated, group_fields)

        # 5. Build processed record (JetEngine schema/field expectations)
        processed_record = {
            **translated,
            "site": self.site,
            "updated_at": datetime.utcnow().isoformat(),
            "hashes": group_hashes,
            "status": "created",  # or 'updated', logic can be extended
        }

        # 6. Write/update to processed_{site}
        unique_id = record.get("im_unique_id") or record.get("_id") or record_id
        if not unique_id:
            logger.warning(f"[{self.site}] Record missing unique identifier, skipping.")
            return None

        result = await processed_collection.update_one(
            {"im_unique_id": unique_id},
            {"$set": processed_record},
            upsert=True
        )
        logger.info(f"[{self.site}] Record {unique_id} processed and written to processed_{self.site}.")

        return processed_record

    async def process_batch(self, records: List[dict]) -> List[dict]:
        """
        Processes a batch of records. Returns list of processed records (successful only).
        """
        results = []
        for record in records:
            unique_id = record.get("im_unique_id") or record.get("_id")
            processed = await self.process_record(record, record_id=unique_id)
            if processed:
                results.append(processed)
        return results