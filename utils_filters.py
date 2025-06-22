"""
utils_filters.py

Enhanced filtering utilities for rawprocessor.
Contains both basic data quality checks and comprehensive site-specific filtering logic.
Used during processing for immediate validation without database operations.

UPDATED: Now uses centralized configuration system for data quality thresholds.
"""

from loguru import logger
from typing import Dict, Any

from config import config

def is_record_clean(record: dict) -> bool:
    """
    Fast validation - check if record meets basic quality standards.
    Used during processing to skip bad records immediately.
    
    Uses configuration values for quality thresholds:
    1. Record has >= configured minimum images 
    2. If emissions = configured threshold, fuel type must be Electric
    
    :param record: Raw record dictionary
    :return: True if record passes basic quality checks, False otherwise
    """
    # Check 1: Images count (configurable requirement)
    images = record.get("Images", [])
    if not images or len(images) < config.MIN_IMAGES_REQUIRED:
        return False
    
    # Check 2: Emissions validation (configurable threshold)
    energy_data = record.get("energyconsumption", {})
    fuel_type = energy_data.get("Fueltype", "").strip()
    raw_emissions = energy_data.get("raw_emissions")
    
    # If emissions is at threshold and not electric, it's invalid
    if raw_emissions == config.EMISSIONS_ZERO_THRESHOLD and "Electric" not in fuel_type:
        return False
    
    return True


def check_raw_against_filters(raw_record: Dict[str, Any], filter_criteria: Dict[str, Any], log_prefix: str = "") -> bool:
    """
    Enhanced site filtering - check if record meets all site filter criteria.
    Includes specific handling for vehicle data types and edge cases.
    
    :param raw_record: Raw record dictionary
    :param filter_criteria: Site-specific filter criteria
    :param log_prefix: Optional prefix for debug logging
    :return: True if record passes all site filters, False otherwise
    """
    record_id = raw_record.get("_id") or raw_record.get("car_id") or "unknown"
    
    try:
        # Helper function to get nested values (e.g., "Basicdata.Type")
        def get_nested_value(obj, key):
            if '.' in key:
                keys = key.split('.')
                value = obj
                for k in keys:
                    if isinstance(value, dict) and k in value:
                        value = value[k]
                    else:
                        return None
                return value
            return obj.get(key)
        
        # Check each filter criterion
        for field, criteria in filter_criteria.items():
            # Handle special field mappings
            if field == "im_mileage":
                # Handle both 'milage' (typo in source) and 'mileage'
                raw_value = raw_record.get("milage") or raw_record.get("mileage") or 0
                try:
                    raw_value = int(raw_value)
                except (ValueError, TypeError):
                    raw_value = 0
            elif field == "im_price_org":
                # Map to 'price' field in raw data
                raw_value = raw_record.get("price")
                try:
                    raw_value = float(raw_value) if raw_value is not None else 0
                except (ValueError, TypeError):
                    raw_value = 0
            elif field == "im_registration_year":
                # Map to 'registration_year' field
                raw_value = raw_record.get("registration_year")
                try:
                    raw_value = int(raw_value) if raw_value is not None else 0
                except (ValueError, TypeError):
                    raw_value = 0
            elif field == "im_fullservicehistory":
                # Map to nested vehiclehistory.Fullservicehistory
                raw_value = raw_record.get("vehiclehistory", {}).get("Fullservicehistory", False)
            elif field == "make":
                # Map to 'brand' field in raw data
                raw_value = raw_record.get("brand", "")
            elif field == "cartype":
                # Map to Basicdata.Type
                raw_value = get_nested_value(raw_record, "Basicdata.Type")
            else:
                # Use generic nested value getter
                raw_value = get_nested_value(raw_record, field)
            
            # Apply filter criteria based on type
            if isinstance(criteria, dict):
                # MongoDB operators like $gte, $lte, $nin
                for operator, expected_value in criteria.items():
                    if operator == '$gte':
                        if raw_value is None:
                            if log_prefix:
                                logger.debug(f"{log_prefix} Filter failed: {field} is None (needs >= {expected_value}) | record_id={record_id}")
                            return False
                        
                        # Handle string/numeric comparisons
                        try:
                            if isinstance(expected_value, str):
                                if str(raw_value) < str(expected_value):
                                    if log_prefix:
                                        logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} < {expected_value} | record_id={record_id}")
                                    return False
                            else:
                                if float(raw_value) < float(expected_value):
                                    if log_prefix:
                                        logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} < {expected_value} | record_id={record_id}")
                                    return False
                        except (ValueError, TypeError):
                            if log_prefix:
                                logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} invalid for >= comparison | record_id={record_id}")
                            return False
                            
                    elif operator == '$lte':
                        if raw_value is None:
                            continue  # None values pass <= checks
                        
                        try:
                            if float(raw_value) > float(expected_value):
                                if log_prefix:
                                    logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} > {expected_value} | record_id={record_id}")
                                return False
                        except (ValueError, TypeError):
                            if log_prefix:
                                logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} invalid for <= comparison | record_id={record_id}")
                            return False
                            
                    elif operator == '$nin':
                        if raw_value in expected_value:
                            if log_prefix:
                                logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} in exclusion list {expected_value} | record_id={record_id}")
                            return False
                            
            elif isinstance(criteria, list):
                # Direct list membership (e.g., Basicdata.Type: ['Car'])
                if raw_value not in criteria:
                    if log_prefix:
                        logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} not in allowed list {criteria} | record_id={record_id}")
                    return False
                    
            elif isinstance(criteria, bool):
                # Boolean comparison (e.g., service_history: True)
                if bool(raw_value) != criteria:
                    if log_prefix:
                        logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} != {criteria} | record_id={record_id}")
                    return False
                    
            elif isinstance(criteria, str):
                # String comparison with special handling
                if criteria == "true":
                    if not bool(raw_value):
                        if log_prefix:
                            logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} not truthy | record_id={record_id}")
                        return False
                elif criteria == "false":
                    if bool(raw_value):
                        if log_prefix:
                            logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} not falsy | record_id={record_id}")
                        return False
                else:
                    # Direct string equality
                    if str(raw_value) != str(criteria):
                        if log_prefix:
                            logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} != {criteria} | record_id={record_id}")
                        return False
            else:
                # Direct equality for other types
                if raw_value != criteria:
                    if log_prefix:
                        logger.debug(f"{log_prefix} Filter failed: {field}={raw_value} != {criteria} | record_id={record_id}")
                    return False
        
        # If we get here, all filters passed
        if log_prefix:
            logger.debug(f"{log_prefix} Record passed all site filters | record_id={record_id}")
        return True
        
    except Exception as ex:
        logger.error(f"{log_prefix} Error checking site filters: {ex} | record_id={record_id}")
        return False


def get_filter_summary(filter_criteria: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get a human-readable summary of filter criteria for debugging.
    
    :param filter_criteria: Site filter criteria
    :return: Summary dictionary
    """
    summary = {
        "total_filters": len(filter_criteria),
        "price_filters": [],
        "year_filters": [],
        "exclusions": [],
        "requirements": []
    }
    
    for field, criteria in filter_criteria.items():
        if "price" in field.lower():
            summary["price_filters"].append(f"{field}: {criteria}")
        elif "year" in field.lower():
            summary["year_filters"].append(f"{field}: {criteria}")
        elif isinstance(criteria, dict) and "$nin" in criteria:
            summary["exclusions"].append(f"{field}: exclude {criteria['$nin']}")
        elif criteria is True or criteria == "true":
            summary["requirements"].append(f"{field}: required")
    
    return summary


def get_data_quality_config_summary() -> Dict[str, Any]:
    """
    Get summary of data quality configuration being used.
    
    :return: Configuration summary
    """
    return {
        "min_images_required": config.MIN_IMAGES_REQUIRED,
        "emissions_threshold": config.EMISSIONS_ZERO_THRESHOLD,
        "source": "configuration system"
    }


def validate_data_quality_config() -> Dict[str, Any]:
    """
    Validate that data quality configuration is sensible.
    
    :return: Validation results
    """
    issues = []
    warnings = []
    
    # Check minimum images requirement
    if config.MIN_IMAGES_REQUIRED < 1:
        issues.append(f"MIN_IMAGES_REQUIRED ({config.MIN_IMAGES_REQUIRED}) must be at least 1")
    elif config.MIN_IMAGES_REQUIRED > 20:
        warnings.append(f"MIN_IMAGES_REQUIRED ({config.MIN_IMAGES_REQUIRED}) is very high - most records may be rejected")
    
    # Check emissions threshold
    if config.EMISSIONS_ZERO_THRESHOLD < 0:
        issues.append(f"EMISSIONS_ZERO_THRESHOLD ({config.EMISSIONS_ZERO_THRESHOLD}) cannot be negative")
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "configuration": get_data_quality_config_summary()
    }