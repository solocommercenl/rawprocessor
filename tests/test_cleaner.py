from cleaner import clean_raw_record

def test_valid_non_electric_record():
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": 122,
        "im_gallery": "url1|url2|url3|url4"
    }
    result = clean_raw_record(rec, "TEST")
    assert result is not None

def test_missing_price():
    rec = {
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": 122,
        "im_gallery": "url1|url2|url3|url4"
    }
    result = clean_raw_record(rec, "TEST")
    assert result is None

def test_non_electric_missing_emissions_too_few_images():
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": None,
        "im_gallery": "url1|url2|url3"
    }
    result = clean_raw_record(rec, "TEST")
    assert result is None

def test_electric_missing_emissions_few_images():
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "electric",
        "im_raw_emissions": None,
        "im_gallery": "url1|url2"
    }
    result = clean_raw_record(rec, "TEST")
    assert result is not None

def test_missing_gallery():
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": 122
        # No gallery field
    }
    result = clean_raw_record(rec, "TEST")
    assert result is None

def test_gallery_comma_delimited():
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": 122,
        "im_gallery": "url1,url2,url3,url4"
    }
    result = clean_raw_record(rec, "TEST")
    assert result is not None
    assert isinstance(result["im_gallery"], list)
    assert len(result["im_gallery"]) == 4

def test_gallery_list():
    rec = {
        "im_price_org": 12345,
        "im_registration_year": "2024",
        "im_first_registration": "06/2024",
        "im_fuel_type": "benzine",
        "im_raw_emissions": 122,
        "im_gallery": ["url1", "url2", "url3", "url4"]
    }
    result = clean_raw_record(rec, "TEST")
    assert result is not None
    assert isinstance(result["im_gallery"], list)
    assert len(result["im_gallery"]) == 4
