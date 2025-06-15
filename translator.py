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
        ("Gearbox", "im_gearbox", "gearbox"),      # Correct mapping
        ("Body", "im_body_type", "body_type"),     # Correct mapping
        ("Upholstery", "im_upholstery", "upholstery"), # Correct mapping
        ("Colour", "color", "color")               # Correct mapping for color
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

    # Handle color translation properly (not raw-pass anymore)
    if "Colour" in raw:
        color_val = raw["Colour"]
        translated_color = await self._translate_value(color_val, "color", translation_map, site, "color", record_id)
        output["color"] = translated_color  # Use translated value instead of raw

    # If no fields are translated, log and report failed fields
    if not output:
        logger.warning(
            f"[TRANSLATE FAIL] {record_id or '-'} @ {site or '-'} - no translated fields found."
        )

    return output
