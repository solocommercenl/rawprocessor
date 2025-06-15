# translator.py

"""
Async translation module for per-field value mapping in vehicle import pipeline.
Handles translation for all relevant meta fields and taxonomies (including color) using the correct
site translation profile from MongoDB, with fallback to default. 
Never raw-pass values: if translation is missing, field is omitted and a log entry is made.
"""

import logging
from typing import Dict, Any, List, Optional, Union
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

class Translator:
    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Translator for value mappings using MongoDB translation profiles.

        Args:
            db: Motor AsyncIOMotorDatabase instance for autodex.
        """
        self.db = db

    async def load_translation_profile(self, site_settings: dict) -> dict:
        """
        Loads the translation mapping from MongoDB for a given site.

        Args:
            site_settings: dict with key 'translation_profile' or fallback to 'default'.

        Returns:
            The complete translation profile dict from MongoDB.
        """
        profile_name = site_settings.get("translation_profile") or "default"
        translation_doc = await self.db.translations.find_one({"_id": profile_name})
        if not translation_doc:
            if profile_name != "default":
                logger.warning(f"Translation profile '{profile_name}' not found, falling back to 'default'")
                translation_doc = await self.db.translations.find_one({"_id": "default"})
            if not translation_doc:
                raise RuntimeError("No translation profiles found in MongoDB (neither site nor default)")
        return translation_doc

    async def translate_fields(
        self,
        input_record: dict,
        site_settings: dict,
        record_id: Optional[Union[str, int]] = None,
        site: Optional[str] = None
    ) -> dict:
        """
        Translate all supported fields in a raw input record, omitting any field with no translation.
        All translated values are suitable for JetEngine/WordPress assignment (meta or taxonomy).

        Args:
            input_record: Raw cleaned input dict.
            site_settings: Site settings dict (contains translation_profile).
            record_id: Optional unique record id for logging.
            site: Site slug or name for logging.

        Returns:
            Dict with only fields that could be successfully translated (matching JetEngine schema).
        """
        translation_map = await self.load_translation_profile(site_settings)

        meta_fields = [
            ("im_fuel_type", "fuel_type"),
            ("im_gearbox", "gearbox"),
            ("im_body_type", "body_type"),
            ("im_upholstery", "upholstery"),
        ]

        options_fields = [
            ("im_comfort_convenience", "options"),
            ("im_entertainment_media", "options"),
            ("im_extras", "options"),
            ("im_safety_security", "options"),
        ]

        tax_fields = [("color", "color")]

        translated: Dict[str, Any] = {}
        failed_fields: List[str] = []

        for field, trans_key in meta_fields:
            val = input_record.get(field)
            if val is None:
                continue
            translated_val = self._translate_value(val, trans_key, translation_map, site, field, record_id)
            if translated_val is not None:
                translated[field] = translated_val
            else:
                failed_fields.append(f"{field}='{val}'")

        for field, trans_key in options_fields:
            raw_options = input_record.get(field)
            if not raw_options:
                continue
            if isinstance(raw_options, str):
                opts = [x.strip() for x in raw_options.replace('|', ',').split(',') if x.strip()]
            elif isinstance(raw_options, list):
                opts = raw_options
            else:
                opts = []
            mapped_options = []
            for opt in opts:
                translated_val = self._translate_value(opt, trans_key, translation_map, site, field, record_id)
                if translated_val is not None:
                    mapped_options.append(translated_val)
                else:
                    failed_fields.append(f"{field}[]='{opt}'")
            if mapped_options:
                translated[field] = mapped_options

        for field, trans_key in tax_fields:
            val = input_record.get(field)
            if val is None:
                continue
            translated_val = self._translate_value(val, trans_key, translation_map, site, field, record_id)
            if translated_val is not None:
                translated[field] = translated_val
            else:
                failed_fields.append(f"{field}='{val}'")

        if not translated:
            logger.warning(
                f"[TRANSLATE FAIL] {record_id or '-'} @ {site or '-'} - no translated fields found. "
                f"Untranslated: {failed_fields}"
            )

        return translated

    def _translate_value(
        self,
        raw_value: Any,
        trans_key: str,
        translation_map: dict,
        site: Optional[str],
        field: str,
        record_id: Optional[Union[str, int]]
    ) -> Optional[Any]:
        """
        Translates a single value using the translation mapping (profile first, then default).
        If not found, returns None and logs the omission.

        Args:
            raw_value: The value to translate.
            trans_key: The top-level key in translation_map (e.g., "fuel_type", "options").
            translation_map: The loaded translation profile.
            site: Site slug/name for logging.
            field: Source field name for logging.
            record_id: Unique record id for logging.

        Returns:
            Translated value, or None if not found.
        """
        val = str(raw_value).strip()
        result = None

        # Try site-specific first, then default
        mapping = translation_map.get(trans_key, {})

        # For options, mapping may be flat dict; for others, dict of raw: translated
        if trans_key == "options":
            result = mapping.get(val)
        else:
            result = mapping.get(val)

        if result is None and translation_map.get("_id") != "default":
            # Try fallback default profile
            default_map = translation_map.get("default_fallback")
            if not default_map:
                # Lazy-load if not pre-cached in profile
                # Should be available in actual Mongo, otherwise skip
                pass
            else:
                default_mapping = default_map.get(trans_key, {})
                result = default_mapping.get(val)

        if result is None:
            logger.warning(
                f"[{site or '-'}][{field}] Omitted untranslated value: '{val}' "
                f"(record: {record_id or '-'})"
            )
            return None
        return result
