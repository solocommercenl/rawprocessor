#!/usr/bin/env python3
"""
Comprehensive fix script for site_settings change detection issues.
This script will diagnose and fix the most common problems.
"""

import asyncio
import os
import sys
import subprocess
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

async def diagnose_and_fix():
    """Comprehensive diagnosis and fix."""
    
    print("🔍 COMPREHENSIVE SITE SETTINGS CHANGE DIAGNOSIS")
    print("=" * 60)
    
    client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB", "autodex")]
    
    issues_found = []
    fixes_applied = []
    
    try:
        # 1. Check MongoDB connection
        print("1. Checking MongoDB connection...")
        await client.admin.command('ping')
        print("   ✅ MongoDB connection OK")
        
        # 2. Check site_settings collection
        print("\n2. Checking site_settings collection...")
        site_settings_count = await db.site_settings.count_documents({})
        if site_settings_count == 0:
            issues_found.append("No site_settings documents found")
            print("   ❌ No site_settings documents found!")
            return
        print(f"   ✅ Found {site_settings_count} site_settings documents")
        
        # 3. Check processing queue
        print("\n3. Checking processing queue...")
        total_jobs = await db.processing_queue.count_documents({})
        pending_jobs = await db.processing_queue.count_documents({"status": "pending"})
        processing_jobs = await db.processing_queue.count_documents({"status": "processing"})
        
        print(f"   Total jobs: {total_jobs}")
        print(f"   Pending: {pending_jobs}")
        print(f"   Processing: {processing_jobs}")
        
        if total_jobs == 0:
            issues_found.append("No jobs in processing queue")
            print("   ⚠️  No jobs found - this suggests change streams aren't working")
        
        # 4. Check for stuck jobs
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        stuck_jobs = await db.processing_queue.count_documents({
            "status": "processing",
            "started_at": {"$lt": one_hour_ago}
        })
        
        if stuck_jobs > 0:
            issues_found.append(f"{stuck_jobs} stuck jobs found")
            print(f"   ⚠️  Found {stuck_jobs} stuck jobs")
            
            # Fix: Reset stuck jobs to pending
            result = await db.processing_queue.update_many(
                {
                    "status": "processing",
                    "started_at": {"$lt": one_hour_ago}
                },
                {
                    "$set": {
                        "status": "pending",
                        "started_at": None
                    }
                }
            )
            fixes_applied.append(f"Reset {result.modified_count} stuck jobs to pending")
            print(f"   🔧 Reset {result.modified_count} stuck jobs to pending")
        
        # 5. Test change streams capability
        print("\n4. Testing change streams capability...")
        try:
            # Test if we can create a change stream
            pipeline = [{"$match": {"operationType": "update"}}]
            change_stream = db.site_settings.watch(pipeline)
            print("   ✅ Change streams supported")
            
            # Make a test update and see if it's detected
            print("   Testing change detection...")
            first_doc = await db.site_settings.find_one({})
            if first_doc:
                # Update with test field
                await db.site_settings.update_one(
                    {"_id": first_doc["_id"]},
                    {"$set": {"_test_change_detection": datetime.utcnow()}}
                )
                
                # Try to get the change event
                try:
                    change = await asyncio.wait_for(change_stream.__anext__(), timeout=5.0)
                    print("   ✅ Change stream event received!")
                    
                    # Clean up test field
                    await db.site_settings.update_one(
                        {"_id": first_doc["_id"]},
                        {"$unset": {"_test_change_detection": ""}}
                    )
                    
                except asyncio.TimeoutError:
                    issues_found.append("Change streams not receiving events")
                    print("   ❌ Change stream not receiving events!")
                    print("   This is likely the root cause of your issue.")
                    
            await change_stream.close()
            
        except Exception as ex:
            issues_found.append(f"Change streams not supported: {ex}")
            print(f"   ❌ Change streams error: {ex}")
        
        # 6. Check daemon process
        print("\n5. Checking daemon process...")
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            daemon_processes = [line for line in result.stdout.split('\n') 
                             if 'main.py --daemon' in line and 'grep' not in line]
            
            if daemon_processes:
                print("   ✅ Daemon process found")
                for proc in daemon_processes:
                    print(f"   Process: {proc.split()[1]} {proc.split()[10:]}")
            else:
                issues_found.append("Daemon process not running")
                print("   ❌ Daemon process not found")
        except Exception as ex:
            print(f"   ⚠️  Could not check daemon process: {ex}")
        
        # 7. Manual fix - trigger processing for all sites
        print("\n6. Applying manual fix...")
        
        sites_processed = []
        async for doc in db.site_settings.find({}):
            site_url = doc.get("site_url", "")
            if not site_url:
                continue
            
            # Extract site key using same logic as trigger system
            import re
            clean_url = site_url.lower().strip()
            clean_url = re.sub(r'^https?://', '', clean_url)
            clean_url = re.sub(r'^www\.', '', clean_url)
            clean_url = clean_url.split('/')[0]
            
            parts = clean_url.split('.')
            if len(parts) >= 2:
                domain_part = '.'.join(parts[:-1])
            else:
                domain_part = clean_url
            
            site_key = re.sub(r'[^\w]', '_', domain_part)
            site_key = re.sub(r'_+', '_', site_key).strip('_')
            
            if site_key and re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', site_key):
                # Create manual pricing change jobs
                raw_count = await db.raw.count_documents({"listing_status": True})
                batch_size = 50
                batch_count = (raw_count + batch_size - 1) // batch_size
                
                for batch_num in range(batch_count):
                    job_doc = {
                        "job_type": "site_pricing_changed",
                        "site": site_key,
                        "priority": 4,
                        "status": "pending",
                        "payload": {
                            "change_type": "pricing_changed",
                            "batch_number": batch_num,
                            "total_batches": batch_count,
                            "skip": batch_num * batch_size,
                            "limit": batch_size
                        },
                        "created_at": datetime.utcnow(),
                        "scheduled_at": datetime.utcnow(),
                        "retries": 0,
                        "max_retries": 3,
                        "retry_delay_seconds": 60,
                        "timeout_seconds": 600
                    }
                    
                    await db.processing_queue.insert_one(job_doc)
                
                sites_processed.append(site_key)
                fixes_applied.append(f"Created {batch_count} pricing jobs for {site_key}")
                print(f"   🔧 Created {batch_count} pricing jobs for {site_key}")
        
        print(f"\n✅ DIAGNOSIS COMPLETE")
        print(f"Sites processed: {len(sites_processed)}")
        print(f"Issues found: {len(issues_found)}")
        print(f"Fixes applied: {len(fixes_applied)}")
        
        if issues_found:
            print(f"\n❌ ISSUES FOUND:")
            for issue in issues_found:
                print(f"   - {issue}")
        
        if fixes_applied:
            print(f"\n🔧 FIXES APPLIED:")
            for fix in fixes_applied:
                print(f"   - {fix}")
        
        # Next steps
        print(f"\n📋 NEXT STEPS:")
        print("1. Start/restart the daemon:")
        print("   python main.py --daemon")
        print("   # or")
        print("   sudo systemctl restart rawprocessor")
        print()
        print("2. Monitor processing:")
        print("   python main.py --queue-status")
        print()
        print("3. Check results:")
        print("   python main.py --status")
        print()
        
        if "Change streams not receiving events" in [str(issue) for issue in issues_found]:
            print("⚠️  CHANGE STREAMS ISSUE DETECTED:")
            print("   Your MongoDB setup may not support change streams.")
            print("   This could be due to:")
            print("   - MongoDB version < 4.0")
            print("   - Not running as replica set")
            print("   - Insufficient permissions")
            print("   ")
            print("   You may need to manually trigger processing after changes:")
            print("   python main.py --trigger site_settings.pricing_changed --site SITENAME")
        
    except Exception as ex:
        print(f"❌ Error during diagnosis: {ex}")
    
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(diagnose_and_fix())