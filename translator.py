# translator.py

"""
Async translation module for per-field value mapping in vehicle import pipeline.
Handles translation for all relevant meta fields and options using the correct
site translation profile from MongoDB, with fallback to default.
Never raw-pass values: if translation is missing, field is omitted and a log entry is made.
"""

import logging
from typing import Dict, Any, List, Optional, Union
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

class Translator:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def load_translation_profile(self, site_settings: dict) -> dict:
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
        raw: dict,
        site_settings: dict,
        record_id: Optional[Union[str, int]] = None,
        site: Optional[str] = None
    ) -> dict:
        """
        Translate all supported fields from a raw input record, using only real raw field names.
        Output will be a dict with JetEngine (im_*) keys, suitable for processed_{site} write.
        """
        translation_map = await self.load_translation_profile(site_settings)

        # Map from raw field names to JetEngine/processed names for output
        output: Dict[str, Any] = {}
        failed_fields: List[str] = []

        # Meta fields
        raw_fields = [
            ("Fueltype", "im_fuel_type", "fuel_type"),
            ("Gearbox", "im_gearbox", "gearbox"),
            ("Body", "im_body_type", "body_type"),
            ("Upholstery", "im_upholstery", "upholstery"),
        ]

        for raw_field, im_field, trans_key in raw_fields:
            val = raw.get(raw_field)
            if val is None:
                continue
            translated_val = self._translate_value(val, trans_key, translation_map, site, im_field, record_id)
            if translated_val is not None:
                output[im_field] = translated_val
            else:
                failed_fields.append(f"{im_field}='{val}'")

        # Options fields, from nested "Equipment" dict
        options_map = {
            "ComfortConvenience": "im_comfort_convenience",
            "entertainmentAndMedia": "im_entertainment_media",
            "extras": "im_extras",
            "safetyAndSecurity": "im_safety_security"
        }
        equipment = raw.get("Equipment", {})
        for eq_key, im_field in options_map.items():
            raw_opts = equipment.get(eq_key)
            if not raw_opts:
                continue
            mapped_options = []
            for opt in raw_opts if isinstance(raw_opts, list) else [raw_opts]:
                translated_val = self._translate_value(opt, "options", translation_map, site, im_field, record_id)
                if translated_val is not None:
                    mapped_options.append(translated_val)
                else:
                    failed_fields.append(f"{im_field}[]='{opt}'")
            if mapped_options:
                output[im_field] = mapped_options

        # Color (tax field, if present in raw)
        if "Colour" in raw:
            color_val = raw["Colour"]
            translated_val = self._translate_value(color_val, "color", translation_map, site, "color", record_id)
            if translated_val is not None:
                output["color"] = translated_val
            else:
                failed_fields.append(f"color='{color_val}'")

        if not output:
            logger.warning(
                f"[TRANSLATE FAIL] {record_id or '-'} @ {site or '-'} - no translated fields found. "
                f"Untranslated: {failed_fields}"
            )

        return output

    def _translate_value(
        self,
        raw_value: Any,
        trans_key: str,
        translation_map: dict,
        site: Optional[str],
        field: str,
        record_id: Optional[Union[str, int]]
    ) -> Optional[Any]:
        val = str(raw_value).strip()
        result = None
        mapping = translation_map.get(trans_key, {})
        result = mapping.get(val)
        if result is None and translation_map.get("_id") != "default":
            default_map = translation_map.get("default_fallback")
            if default_map:
                fallback_mapping = default_map.get(trans_key, {})
                result = fallback_mapping.get(val)
        if result is None:
            logger.warning(
                f"[{site or '-'}][{field}] Omitted untranslated value: '{val}' (record: {record_id or '-'})"
            )
        return result
