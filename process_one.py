# process_one.py

import asyncio
import os
import json
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from site_settings import SiteSettings
from cleaner import clean_raw_record
from translator import Translator
from calculator import Calculator
from processor import Processor

load_dotenv()
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "autodex")

RAW_ID = ""   # Optionally set a MongoDB ObjectId string here
SITE = "solostaging"  # <- site key from your site_settings example

async def main():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    # Load one raw record (by id or just the first)
    if RAW_ID:
        from bson import ObjectId
        raw = await db.raw.find_one({"_id": ObjectId(RAW_ID)})
    else:
        raw = await db.raw.find_one({})

    if not raw:
        print("No raw record found.")
        return

    settings = await SiteSettings(db).get(SITE)
    cleaned = clean_raw_record(raw, f"[{SITE}]")
    if not cleaned:
        print("Raw record failed cleaner and would be excluded.")
        return

    processor = Processor(db, SITE)
    processed = await processor.process(cleaned, settings)
    if not processed:
        print("Processing returned None. Record would not be imported.")
        return

    print("Processed output (as would be written to processed_{site}):")
    print(json.dumps(processed, indent=2, default=str))

if __name__ == "__main__":
    asyncio.run(main())
