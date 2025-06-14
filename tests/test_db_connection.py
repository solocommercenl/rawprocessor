# tests/test_db_connection.py

import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import asyncio

def load_config():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
    mongo_uri = os.getenv('MONGO_URI')
    mongo_db = os.getenv('MONGO_DB')
    return mongo_uri, mongo_db

async def main():
    mongo_uri, mongo_db = load_config()
    client = AsyncIOMotorClient(mongo_uri)
    db = client[mongo_db]
    # Try counting documents in the 'raw' collection
    count = await db.raw.count_documents({})
    print(f"MongoDB connection OK. 'raw' collection document count: {count}")

if __name__ == "__main__":
    asyncio.run(main())
