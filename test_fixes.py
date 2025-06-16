#!/usr/bin/env python3
"""
test_fixes.py

Test script to verify all the fixes work correctly with real data.
"""

import asyncio
import os
import json
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# Import our fixed modules
from site_settings import SiteSettings
from cleaner import Cleaner
from translator import Translator
from calculator import Calculator
from processor import Processor

load_dotenv()
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "autodex")
SITE = "solostaging"

async def test_data_structure():
    """Test that we can read the actual data structure correctly."""
    print("=== Testing Data Structure ===")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Get a sample raw record
    raw = await db.raw.find_one({"listing_status": True})
    if not raw:
        print("❌ No raw records found")
        return False
    
    print(f"✅ Found raw record: {raw.get('car_id', 'unknown')}")
    
    # Test field access
    fuel_type = raw.get("energyconsumption", {}).get("Fueltype")
    raw_emissions = raw.get("energyconsumption", {}).get("raw_emissions")
    images = raw.get("Images", [])
    
    print(f"  Fuel type: {fuel_type}")
    print(f"  Raw emissions: {raw_emissions}")
    print(f"  Images count: {len(images) if isinstance(images, list) else 'not a list'}")
    
    return True

async def test_cleaner():
    """Test the fixed cleaner logic."""
    print("\n=== Testing Cleaner ===")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    cleaner = Cleaner(db, SITE)
    
    # Get a test record
    raw = await db.raw.find_one({"listing_status": True})
    if not raw:
        print("❌ No raw records found")
        return False
    
    # Test cleaning
    cleaned = await cleaner.clean_raw_record(raw, f"[{SITE}]")
    
    if cleaned:
        print("✅ Record passed cleaning")
        print(f"  Derived fuel type: {cleaned.get('_fuel_type')}")
        print(f"  Derived emissions: {cleaned.get('_raw_emissions')}")
        print(f"  Derived price: {cleaned.get('_price')}")
    else:
        print("❌ Record failed cleaning")
        return False
    
    # Test site filter check
    try:
        site_settings = await SiteSettings(db).get(SITE)
        site_filters = site_settings.get("filter_criteria", {})
        
        processable = await cleaner.is_record_processable_for_site(cleaned, site_filters, f"[{SITE}]")
        print(f"  Site filter check: {'✅ PASS' if processable else '❌ FAIL'}")
        
    except Exception as ex:
        print(f"❌ Site filter check failed: {ex}")
        return False
    
    return True

async def test_translator():
    """Test the fixed translator logic."""
    print("\n=== Testing Translator ===")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    translator = Translator(db)
    
    # Get test data
    raw = await db.raw.find_one({"listing_status": True})
    site_settings = await SiteSettings(db).get(SITE)
    
    if not raw:
        print("❌ No raw records found")
        return False
    
    # Test translation
    try:
        translated = await translator.translate_fields(raw, site_settings, raw.get("_id"), SITE)
        
        if translated:
            print("✅ Translation successful")
            print(f"  Translated fields: {list(translated.keys())}")
            
            # Show some key translations
            if "im_fuel_type" in translated:
                original = raw.get("energyconsumption", {}).get("Fueltype")
                print(f"  Fuel: {original} → {translated['im_fuel_type']}")
            
            if "im_gearbox" in translated:
                original = raw.get("gearbox") or raw.get("TechnicalData", {}).get("Gearbox")
                print(f"  Gearbox: {original} → {translated['im_gearbox']}")
            
            if "color" in translated:
                original = raw.get("colourandupholstery", {}).get("Colour")
                print(f"  Color: {original} → {translated['color']}")
        else:
            print("❌ No translations produced")
            return False
            
    except Exception as ex:
        print(f"❌ Translation failed: {ex}")
        return False
    
    return True

async def test_calculator():
    """Test the fixed calculator logic."""
    print("\n=== Testing Calculator ===")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Get test data  
    raw = await db.raw.find_one({"listing_status": True})
    site_settings = await SiteSettings(db).get(SITE)
    
    if not raw:
        print("❌ No raw records found")
        return False
    
    calculator = Calculator(db, site_settings)
    
    # Test calculation
    try:
        vated = bool(raw.get("vatded", False))
        financials = await calculator.calculate_financials(raw, vated)
        
        if financials:
            print("✅ Financial calculation successful")
            print(f"  VAT deductible: {vated}")
            print(f"  Original price: €{raw.get('price', 0):,.2f}")
            print(f"  Final price: €{financials.get('im_price', 0):,.2f}")
            print(f"  BMP rate: €{financials.get('im_bmp_rate', 0):,.2f}")
            print(f"  Monthly payment: €{financials.get('im_monthly_payment', 0):,.2f}")
        else:
            print("❌ No financial calculations produced")
            return False
            
    except Exception as ex:
        print(f"❌ Financial calculation failed: {ex}")
        return False
    
    return True

async def test_full_processing():
    """Test the complete processing pipeline."""
    print("\n=== Testing Full Processing Pipeline ===")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Get test data
    raw = await db.raw.find_one({"listing_status": True})
    site_settings = await SiteSettings(db).get(SITE)
    
    if not raw:
        print("❌ No raw records found")
        return False
    
    processor = Processor(db, SITE)
    
    # Test full processing
    try:
        processed = await processor.process_single_record(raw, site_settings)
        
        if processed:
            print("✅ Full processing successful")
            print(f"  Record ID: {processed.get('im_ad_id')}")
            print(f"  Title: {processed.get('im_title')}")
            print(f"  Price: €{processed.get('im_price', 0):,}")
            print(f"  Gallery images: {len(processed.get('im_gallery', '').split('|')) if processed.get('im_gallery') else 0}")
            print(f"  Hash groups: {list(processed.get('hashes', {}).keys())}")
            print(f"  Is new: {processed.get('_is_new', False)}")
            
            # Verify key fields match expected structure
            expected_fields = [
                "im_ad_id", "im_title", "im_gallery", "im_price", "im_fuel_type",
                "im_gearbox", "im_body_type", "make", "model", "color"
            ]
            
            missing_fields = [field for field in expected_fields if field not in processed]
            if missing_fields:
                print(f"⚠️  Missing expected fields: {missing_fields}")
            else:
                print("✅ All expected fields present")
        else:
            print("❌ Processing returned None")
            return False
            
    except Exception as ex:
        print(f"❌ Full processing failed: {ex}")
        return False
    
    return True

async def test_database_connections():
    """Test database table access."""
    print("\n=== Testing Database Connections ===")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Test collection access
    collections_to_test = [
        "raw", "site_settings", "translations", 
        "bmp_tables", "depreciation_table", "diesel_surcharges"
    ]
    
    for collection_name in collections_to_test:
        try:
            count = await db[collection_name].count_documents({})
            print(f"  {collection_name}: {count} documents ✅")
        except Exception as ex:
            print(f"  {collection_name}: ERROR - {ex} ❌")
            return False
    
    return True

async def main():
    """Run all tests."""
    print("🧪 Running rawprocessor Stage 1 fix verification tests...\n")
    
    tests = [
        ("Database Connections", test_database_connections),
        ("Data Structure", test_data_structure),
        ("Cleaner", test_cleaner),
        ("Translator", test_translator),
        ("Calculator", test_calculator),
        ("Full Processing", test_full_processing),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as ex:
            print(f"❌ Test '{test_name}' crashed: {ex}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nPassed: {passed}/{len(results)} tests")
    
    if passed == len(results):
        print("🎉 All tests passed! The fixes are working correctly.")
    else:
        print("⚠️  Some tests failed. Please review the output above.")

if __name__ == "__main__":
    asyncio.run(main())