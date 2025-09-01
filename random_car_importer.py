import asyncio
import random
from datetime import datetime
from typing import Dict, List, Any
from motor.motor_asyncio import AsyncIOMotorClient
import argparse
import json
from collections import defaultdict

# MongoDB connection
MONGO_URI = "mongodb://10.0.0.21:27017"  # ADMDB server
MONGO_DB = "autodex"

class RandomCarImporter:
    """Import random cars for testing purposes."""
    
    def __init__(self, site: str = "dev.autodex.nl"):
        self.site = site
        self.client = AsyncIOMotorClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.stats = defaultdict(int)
        
    async def get_random_cars(self, total: int = 5000) -> List[Dict[str, Any]]:
        """
        Fetch random cars from processed collections with diversity.
        Ensures good distribution across makes, models, colors, etc.
        """
        cars = []
        
        # Strategy: Get cars from different processed collections
        # Priority on getting diverse data for filter testing
        
        print(f"🔍 Gathering {total} random cars for testing...")
        
        # 1. Try to get from main processed collection first
        processed_collections = [
            f"processed_{self.site}",
            "processed_solostaging",  # Fallback collections
            "processed_texbijl",
            "processed_mothisautomotive", 
            "processed_autobedrijfkooyman"
        ]
        
        # Find which collections exist and have data
        available_collections = []
        for coll_name in processed_collections:
            count = await self.db[coll_name].count_documents({})
            if count > 0:
                available_collections.append((coll_name, count))
                print(f"  ✓ Found {count} cars in {coll_name}")
        
        if not available_collections:
            # Fallback: try voorraad collections
            voorraad_collections = [
                f"processed_voorraad_{self.site}",
                "processed_voorraad_solostaging"
            ]
            for coll_name in voorraad_collections:
                count = await self.db[coll_name].count_documents({})
                if count > 0:
                    available_collections.append((coll_name, count))
                    print(f"  ✓ Found {count} voorraad cars in {coll_name}")
        
        if not available_collections:
            raise ValueError("No processed collections found with data!")
        
        # 2. Distribute selection across collections
        cars_per_collection = total // len(available_collections)
        remainder = total % len(available_collections)
        
        for i, (coll_name, coll_count) in enumerate(available_collections):
            # Add remainder to first collection
            target_count = cars_per_collection + (1 if i < remainder else 0)
            
            # Don't try to get more than available
            actual_count = min(target_count, coll_count)
            
            print(f"\n📦 Fetching {actual_count} cars from {coll_name}...")
            
            # Use aggregation pipeline for true random selection with diversity
            pipeline = [
                # Only active listings
                {"$match": {"im_status": {"$ne": "False"}}},
                
                # Add random score for sampling
                {"$sample": {"size": actual_count * 2}},  # Get 2x for filtering
                
                # Ensure we have required fields
                {"$match": {
                    "im_ad_id": {"$exists": True, "$ne": ""},
                    "make": {"$exists": True, "$ne": ""},
                    "model": {"$exists": True, "$ne": ""}
                }},
                
                # Limit to target count
                {"$limit": actual_count}
            ]
            
            cursor = self.db[coll_name].aggregate(pipeline)
            collection_cars = await cursor.to_list(length=actual_count)
            
            # Track statistics
            for car in collection_cars:
                make = car.get("make", "Unknown")
                model = car.get("model", "Unknown") 
                color = car.get("color", "Unknown")
                fuel = car.get("im_fuel_type", "Unknown")
                
                self.stats[f"make_{make}"] += 1
                self.stats[f"model_{model}"] += 1
                self.stats[f"color_{color}"] += 1
                self.stats[f"fuel_{fuel}"] += 1
                
                # Update site field for target
                car["site"] = self.site
                cars.append(car)
            
            print(f"  ✓ Retrieved {len(collection_cars)} cars")
        
        # 3. Ensure diversity by checking distribution
        print(f"\n📊 Total cars collected: {len(cars)}")
        
        # Shuffle for random distribution
        random.shuffle(cars)
        
        return cars[:total]  # Ensure exactly the requested amount
    
    async def queue_for_wordpress(self, cars: List[Dict[str, Any]]) -> None:
        """
        Queue cars for WordPress import using the WP queue system.
        """
        print(f"\n🚀 Queueing {len(cars)} cars for WordPress import to {self.site}...")
        
        # Get or create WP queue for the site
        wp_queue_collection = self.db[f"wp_queue_{self.site}"]
        
        # Prepare jobs for WP queue
        jobs = []
        batch_size = 100
        
        for i, car in enumerate(cars):
            ad_id = car.get("im_ad_id") or car.get("st_ad_id")
            
            if not ad_id:
                print(f"  ⚠️  Skipping car without ad_id")
                continue
            
            # Check if already exists in WP queue
            existing = await wp_queue_collection.find_one({"ad_id": ad_id})
            
            job = {
                "ad_id": ad_id,
                "action": "update" if existing else "create",
                "site": self.site,
                "priority": 5,  # Normal priority
                "status": "pending",
                "created_at": datetime.utcnow(),
                "retry_count": 0,
                "meta": {
                    "source_type": "voorraad" if "st_" in str(car.get("st_ad_id", "")) else "import",
                    "test_import": True,
                    "batch_import": True
                }
            }
            
            jobs.append(job)
            
            # Insert in batches
            if len(jobs) >= batch_size:
                await wp_queue_collection.insert_many(jobs)
                print(f"  ✓ Queued batch of {len(jobs)} cars ({i+1}/{len(cars)})")
                jobs = []
        
        # Insert remaining jobs
        if jobs:
            await wp_queue_collection.insert_many(jobs)
            print(f"  ✓ Queued final batch of {len(jobs)} cars")
        
        print(f"\n✅ Successfully queued all cars for import!")
    
    def print_statistics(self) -> None:
        """Print diversity statistics of selected cars."""
        print("\n📈 Diversity Statistics:")
        print("-" * 40)
        
        # Count unique makes
        makes = {k.replace("make_", ""): v for k, v in self.stats.items() if k.startswith("make_")}
        print(f"Unique makes: {len(makes)}")
        
        # Top 10 makes
        top_makes = sorted(makes.items(), key=lambda x: x[1], reverse=True)[:10]
        print("\nTop 10 Makes:")
        for make, count in top_makes:
            print(f"  • {make}: {count} cars")
        
        # Count unique colors  
        colors = {k.replace("color_", ""): v for k, v in self.stats.items() if k.startswith("color_")}
        print(f"\nUnique colors: {len(colors)}")
        
        # Fuel types
        fuels = {k.replace("fuel_", ""): v for k, v in self.stats.items() if k.startswith("fuel_")}
        print(f"\nFuel types distribution:")
        for fuel, count in fuels.items():
            print(f"  • {fuel}: {count} cars")
    
    async def store_in_processed_collection(self, cars: List[Dict[str, Any]]) -> None:
        """
        Alternative: Store cars directly in processed collection for the site.
        """
        print(f"\n💾 Storing {len(cars)} cars in processed_{self.site} collection...")
        
        processed_collection = self.db[f"processed_{self.site}"]
        
        # Clear existing test data if needed
        if await processed_collection.count_documents({}) > 0:
            response = input("  ⚠️  Collection already has data. Clear it first? (y/n): ")
            if response.lower() == 'y':
                await processed_collection.delete_many({})
                print("  ✓ Cleared existing data")
        
        # Insert cars in batches
        batch_size = 500
        for i in range(0, len(cars), batch_size):
            batch = cars[i:i+batch_size]
            await processed_collection.insert_many(batch)
            print(f"  ✓ Inserted batch {i//batch_size + 1}/{(len(cars)//batch_size) + 1}")
        
        print(f"✅ Successfully stored all cars in processed_{self.site}")
        
        # Create index for better performance
        await processed_collection.create_index("im_ad_id")
        await processed_collection.create_index("site")
        print("  ✓ Created indexes")

async def main():
    parser = argparse.ArgumentParser(description="Import random test cars to WordPress")
    parser.add_argument("--site", default="dev.autodex.nl", help="Target site (default: dev.autodex.nl)")
    parser.add_argument("--count", type=int, default=5000, help="Number of cars to import (default: 5000)")
    parser.add_argument("--method", choices=["queue", "direct"], default="queue", 
                       help="Import method: 'queue' for WP queue or 'direct' for processed collection")
    parser.add_argument("--dry-run", action="store_true", help="Test run without actual import")
    
    args = parser.parse_args()
    
    importer = RandomCarImporter(args.site)
    
    try:
        # Get random cars
        cars = await importer.get_random_cars(args.count)
        
        # Print statistics
        importer.print_statistics()
        
        if args.dry_run:
            print("\n🔍 DRY RUN - No actual import performed")
            print(f"Would import {len(cars)} cars to {args.site}")
        else:
            if args.method == "queue":
                # Queue for WP processor to handle
                await importer.queue_for_wordpress(cars)
                print("\n💡 Next steps:")
                print(f"1. The WP processor will pick up these jobs from wp_queue_{args.site}")
                print("2. Monitor progress in WP processor logs")
                print("3. Use --status flag on wp-processor to check queue status")
            else:
                # Store directly in processed collection
                await importer.store_in_processed_collection(cars)
                print("\n💡 Next steps:")
                print(f"1. Trigger site rebuild: python main.py --rebuild-site {args.site}")
                print("2. Or manually sync: python main.py --trigger site_settings.filters_changed --site {args.site}")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        importer.client.close()

if __name__ == "__main__":
    asyncio.run(main())