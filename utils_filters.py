def check_raw_against_filters(raw_record, filter_criteria):
    """
    Check if a raw record meets the site filter criteria.
    Returns True if the record passes all filters, False otherwise.
    """
    
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
        raw_value = get_nested_value(raw_record, field)
        
        # Handle different types of criteria
        if isinstance(criteria, dict):
            # MongoDB operators like $gte, $lte, $nin
            for operator, expected_value in criteria.items():
                if operator == '$gte':
                    if raw_value is None or str(raw_value) < str(expected_value):
                        return False
                elif operator == '$lte':
                    if raw_value is None or int(raw_value or 0) > expected_value:
                        return False
                elif operator == '$nin':
                    if raw_value in expected_value:
                        return False
        elif isinstance(criteria, list):
            # Direct list membership (e.g., Basicdata.Type: ['Car'])
            if raw_value not in criteria:
                return False
        elif isinstance(criteria, bool):
            # Boolean comparison (e.g., service_history: True)
            if bool(raw_value) != criteria:
                return False
        else:
            # Direct equality
            if raw_value != criteria:
                return False
    
    return True

# Test function
if __name__ == "__main__":
    # Test with sample data
    sample_raw = {
        'registration_year': '2020',
        'price': 25000,
        'mileage': 50000,
        'service_history': True,
        'Basicdata': {'Type': 'Car'},
        'brand': 'BMW',
        'model': '3 Series',
        'fuel_type': 'Gasoline'
    }
    
    sample_filters = {
        'registration_year': {'$gte': '2016'},
        'price': {'$gte': 10000, '$lte': 125000},
        'mileage': {'$lte': 160000},
        'service_history': True,
        'Basicdata.Type': ['Car'],
        'brand': {'$nin': ['Aiways', 'Aixam']},
        'fuel_type': {'$nin': []}
    }
    
    result = check_raw_against_filters(sample_raw, sample_filters)
    print(f"Sample record passes filters: {result}")
