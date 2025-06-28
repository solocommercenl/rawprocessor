import logging
import re
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

    async def consolidate_model(self, brand: str, model: str) -> str:
        """
        Consolidate model based on brand-specific rules from MongoDB.
        
        :param brand: Vehicle brand/make (e.g., "BMW", "Volkswagen")
        :param model: Original model from raw data (e.g., "Golf GTI", "118i")
        :return: Consolidated model (e.g., "Golf", "1-serie") or original if no rule matches
        """
        if not brand or not model:
            return model
        
        try:
            # Normalize brand for lookup
            brand_key = brand.lower().replace(" ", "-").replace("_", "-")
            
            # Find rules for this brand
            rules_doc = await self.db.model_consolidation_rules.find_one({"_id": brand_key})
            
            if not rules_doc:
                logger.debug(f"No model consolidation rules found for brand: {brand}")
                return model
            
            # Apply rules in order (most specific first)
            for rule in rules_doc.get("rules", []):
                pattern = rule.get("pattern", "")
                consolidated_model = rule.get("consolidated_model", "")
                
                if not pattern or not consolidated_model:
                    continue
                
                # Check if pattern matches
                if re.match(pattern, model, re.IGNORECASE):
                    # Apply substitution (supports capture groups like $1)
                    result = re.sub(pattern, consolidated_model, model, flags=re.IGNORECASE)
                    logger.debug(f"Model consolidation: {brand} {model} -> {result} (rule: {rule.get('description', 'no description')})")
                    return result
            
            # No matching rule found
            logger.debug(f"No matching rule for {brand} {model}, keeping original")
            return model
            
        except Exception as ex:
            logger.error(f"Error in model consolidation for {brand} {model}: {ex}")
            return model  # Fallback to original on error

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
        
        Based on the actual raw data structure from your JSON exports.
        """
        translation_map = await self.load_translation_profile(site_settings)
        output: Dict[str, Any] = {}
        record_id = record_id or raw.get("_id") or raw.get("car_id") or "unknown"

        # Helper function to safely get nested values
        def safe_get(data, *keys):
            """Safely navigate nested dictionaries."""
            try:
                result = data
                for key in keys:
                    if result is None:
                        return None
                    result = result.get(key)
                return result
            except (AttributeError, TypeError):
                return None

        # FUEL TYPE - from energyconsumption.Fueltype
        fuel_type = safe_get(raw, "energyconsumption", "Fueltype")
        if fuel_type:
            translated_fuel = await self._translate_value(
                fuel_type, "fuel_type", translation_map, site, "im_fuel_type", record_id
            )
            output["im_fuel_type"] = translated_fuel

        # GEARBOX - from TechnicalData.Gearbox or direct gearbox field
        gearbox = safe_get(raw, "TechnicalData", "Gearbox") or raw.get("gearbox")
        if gearbox:
            translated_gearbox = await self._translate_value(
                gearbox, "gearbox", translation_map, site, "im_gearbox", record_id
            )
            output["im_gearbox"] = translated_gearbox

        # BODY TYPE - from Basicdata.Body
        body_type = safe_get(raw, "Basicdata", "Body")
        if body_type:
            translated_body = await self._translate_value(
                body_type, "body_type", translation_map, site, "im_body_type", record_id
            )
            output["im_body_type"] = translated_body

        # UPHOLSTERY - from colourandupholstery.Upholstery
        upholstery = safe_get(raw, "colourandupholstery", "Upholstery")
        if upholstery:
            translated_upholstery = await self._translate_value(
                upholstery, "upholstery", translation_map, site, "im_upholstery", record_id
            )
            output["im_upholstery"] = translated_upholstery

        # COLOR - from colourandupholstery.Colour (for taxonomy)
        color = safe_get(raw, "colourandupholstery", "Colour")
        if color:
            translated_color = await self._translate_value(
                color, "color", translation_map, site, "color", record_id
            )
            output["color"] = translated_color

        # MODEL CONSOLIDATION - consolidate model based on brand-specific rules
        brand = raw.get("brand", "")
        model = raw.get("model", "")
        if brand and model:
            consolidated_model = await self.consolidate_model(brand, model)
            output["model"] = consolidated_model  # This goes to the model taxonomy field
            logger.debug(f"Model consolidation: {brand} {model} -> {consolidated_model} | record_id={record_id}")

        # EQUIPMENT/OPTIONS - from Equipment nested structure
        equipment = raw.get("Equipment", {})
        if isinstance(equipment, dict):
            
            # Comfort & Convenience
            comfort_options = equipment.get("ComfortConvenience", [])
            if comfort_options:
                translated_comfort = await self._translate_options_list(
                    comfort_options, "options", translation_map, site, "im_comfort_convenience", record_id
                )
                if translated_comfort:
                    output["im_comfort_convenience"] = translated_comfort

            # Entertainment & Media  
            media_options = equipment.get("entertainmentAndMedia", [])
            if media_options:
                translated_media = await self._translate_options_list(
                    media_options, "options", translation_map, site, "im_entertainment_media", record_id
                )
                if translated_media:
                    output["im_entertainment_media"] = translated_media

            # Extras
            extras_options = equipment.get("extras", [])
            if extras_options:
                translated_extras = await self._translate_options_list(
                    extras_options, "options", translation_map, site, "im_extras", record_id
                )
                if translated_extras:
                    output["im_extras"] = translated_extras

            # Safety & Security
            safety_options = equipment.get("safetyAndSecurity", [])
            if safety_options:
                translated_safety = await self._translate_options_list(
                    safety_options, "options", translation_map, site, "im_safety_security", record_id
                )
                if translated_safety:
                    output["im_safety_security"] = translated_safety

        # Log translation results
        if output:
            logger.debug(f"Successfully translated {len(output)} fields for record_id={record_id}")
        else:
            logger.warning(f"No fields were translated for record_id={record_id}")

        return output

    async def _translate_options_list(
        self,
        options_list: List[str],
        trans_key: str,
        translation_map: dict,
        site: Optional[str],
        field: str,
        record_id: Optional[Union[str, int]]
    ) -> List[str]:
        """
        Translate a list of equipment options.
        """
        if not isinstance(options_list, list):
            return []
            
        translated_options = []
        for option in options_list:
            if option and isinstance(option, str):
                translated_option = await self._translate_value(
                    option, trans_key, translation_map, site, field, record_id
                )
                if translated_option:
                    translated_options.append(translated_option)
        
        return translated_options

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
        Falls back to raw value if no translation is found.
        """
        if not raw_value:
            return raw_value

        val = str(raw_value).strip()
        mapping = translation_map.get(trans_key, {})
        
        # Try exact match first
        result = mapping.get(val)
        
        # Try case-insensitive match if exact fails
        if result is None:
            for map_key, map_value in mapping.items():
                if map_key.lower() == val.lower():
                    result = map_value
                    break
        
        # Fall back to raw value if no translation found
        if result is None:
            result = raw_value
            logger.debug(f"No translation found for {trans_key}.{val}, using raw value | record_id={record_id}")
        else:
            logger.debug(f"Translated {trans_key}.{val} → {result} | record_id={record_id}")
        
        return result