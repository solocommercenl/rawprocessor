import logging
from typing import Dict, Any, List, Optional, Union
from motor.motor_asyncio import AsyncIOMotorDatabase
from loguru import logger

class Translator:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def load_translation_profile(self, site_settings: dict) -> dict:
        """
        Load the translation profile from MongoDB based on the site's settings.
        """
        profile_name = site_settings.get("translation_profile") or "default"
        translation_doc = await self.db.translations.find_one({"_id": profile_name})
        if not translation_doc:
            raise RuntimeError(f"Translation profile '{profile_name}' not found in MongoDB.")
        return translation_doc

    async def translate_fields(
        self,
        raw: dict,
        site_settings: dict,
        record_id: Optional[Union[str, int]] = None,
        site: Optional[str] = None
    ) -> dict:
        """
        Translate all supported fields from a raw input record using the translation profile.
        Output will be a dict with JetEngine (im_*) keys, suitable for processed_{site} write.
        """
        translation_map = await self.load_translation_profile(site_settings)

        # Map from raw field names to JetEngine/processed names for output
        output: Dict[str, Any] = {}

        # Meta fields to translate
        raw_fields = [
            ("Fueltype", "im_fuel_type", "fuel_type"),
            ("Gearbox", "im_gearbox", "gearbox"),
            ("Body", "im_body_type", "body_type"),
            ("Upholstery", "im_upholstery", "upholstery"),
        ]

        # Process each raw field and translate it
        for raw_field, im_field, trans_key in raw_fields:
            val = raw.get(raw_field)
            if val is None:
                continue  # Skip if the field is missing in raw data
            translated_val = await self._translate_value(val, trans_key, translation_map, site, im_field, record_id)
            # Always include the translated value or raw value if translation is missing
            output[im_field] = translated_val

        # Options fields from nested "Equipment" dict
        options_map = {
            "ComfortConvenience": "im_comfort_convenience",
            "entertainmentAndMedia": "im_entertainment_media",
            "extras": "im_extras",
            "safetyAndSecurity": "im_safety_security"
        }

        # Process equipment options fields
        equipment = raw.get("Equipment", {})
        for eq_key, im_field in options_map.items():
            raw_opts = equipment.get(eq_key)
            if not raw_opts:
                continue  # Skip if no options are present
            mapped_options = []
            for opt in raw_opts if isinstance(raw_opts, list) else [raw_opts]:
                translated_val = await self._translate_value(opt, "options", translation_map, site, im_field, record_id)
                mapped_options.append(translated_val)  # Always add the translated or raw value
            if mapped_options:
                output[im_field] = mapped_options

        # Color handling (raw-pass, no translation)
        if "Colour" in raw:
            color_val = raw["Colour"]
            output["color"] = color_val  # Pass color raw without translation.

        # If no fields are translated, log and report failed fields
        if not output:
            logger.warning(
                f"[TRANSLATE FAIL] {record_id or '-'} @ {site or '-'} - no translated fields found."
            )

        return output

    async def _translate_value(
        self,
        raw_value: Any,
        trans_key: str,
        translation_map: dict,
        site: Optional[str],
        field: str,
        record_id: Optional[Union[str, int]]
    ) -> Any:
        """
        Translate a single field value using the provided translation map.
        """
        val = str(raw_value).strip()
        result = None
        mapping = translation_map.get(trans_key, {})

        # Get the translated value if available
        result = mapping.get(val)

        # If no result is found, log the missing translation and use the raw value
        if result is None:
            logger.warning(
                f"[{site or '-'}][{field}] Missing translation for value: '{val}' (record: {record_id or '-'})"
            )
            result = raw_value  # Use raw value when no translation is available
        
        return result
