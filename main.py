"""
main.py

Entrypoint and orchestrator for rawprocessor Stage 1.
Handles MongoDB change streams, manual triggers, and CLI operations.

FIXED: Complete integration with corrected data mapping and trigger system.
"""

import asyncio
import os
import sys
import argparse
import signal
from typing import Any, Dict
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger

from logger import configure_logger, log_exceptions
from trigger_system import TriggerSystem
from jobqueue import WPQueue
from processor import Processor

# Load environment
load_dotenv()
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "autodex")

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable required")

# Global state
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
trigger_system = None
shutdown_event = asyncio.Event()

def setup_signal_handlers():
    """Setup graceful shutdown signal handlers."""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

@log_exceptions
async def start_daemon_mode():
    """
    Start the daemon mode with MongoDB change stream monitoring.
    """
    global trigger_system
    
    logger.info("Starting rawprocessor Stage 1 daemon...")
    
    try:
        # Initialize trigger system
        trigger_system = TriggerSystem(db)
        await trigger_system.initialize()
        
        # Start change stream monitoring
        monitor_task = asyncio.create_task(trigger_system.start_watching())
        
        # Wait for shutdown signal
        await shutdown_event.wait()
        
        logger.info("Shutdown signal received, stopping daemon...")
        
        # Stop monitoring
        await trigger_system.stop_watching()
        monitor_task.cancel()
        
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
            
        logger.info("Daemon stopped gracefully")
        
    except Exception as ex:
        logger.error(f"Error in daemon mode: {ex}")
        raise

@log_exceptions  
async def handle_manual_trigger(args):
    """
    Handle manual trigger execution.
    """
    global trigger_system
    
    if not trigger_system:
        trigger_system = TriggerSystem(db)
        await trigger_system.initialize()
    
    trigger_data = {}
    if args.data:
        import json
        trigger_data = json.loads(args.data)
    
    await trigger_system.manual_trigger(args.trigger, args.site, trigger_data)
    logger.info(f"Manual trigger '{args.trigger}' completed")

@log_exceptions
async def handle_retry_failed(args):
    """
    Retry failed queue jobs for a site.
    """
    if not args.site:
        raise ValueError("--site required for --retry-failed")
    
    queue = WPQueue(db, args.site)
    count = await queue.retry_failed_jobs(limit=args.limit or 100)
    logger.info(f"Retried {count} failed jobs for site '{args.site}'")

@log_exceptions
async def handle_rebuild_site(args):
    """
    Rebuild all records for a site.
    """
    if not args.site:
        raise ValueError("--site required for --rebuild-site")
    
    global trigger_system
    
    if not trigger_system:
        trigger_system = TriggerSystem(db)
        await trigger_system.initialize()
    
    await trigger_system.manual_trigger("rebuild_site", args.site)
    logger.info(f"Site rebuild completed for '{args.site}'")

@log_exceptions
async def handle_status():
    """
    Show system status.
    """
    global trigger_system
    
    if not trigger_system:
        trigger_system = TriggerSystem(db)
        await trigger_system.initialize()
    
    status = await trigger_system.get_system_status()
    
    print("=== Rawprocessor Stage 1 Status ===")
    print(f"Running: {status['running']}")
    print(f"Active Sites: {status['site_count']}")
    print(f"Sites: {', '.join(status['active_sites'])}")
    print(f"Timestamp: {status['timestamp']}")
    
    print("\n=== Queue Status ===")
    for site, stats in status['queue_stats'].items():
        if 'error' in stats:
            print(f"{site}: ERROR - {stats['error']}")
        else:
            print(f"{site}: {stats['pending']} pending, {stats['failed']} failed")

@log_exceptions
async def handle_test_one(args):
    """
    Test processing of a single raw record.
    """
    if not args.site:
        raise ValueError("--site required for --test-one")
    
    from site_settings import SiteSettings
    from cleaner import Cleaner
    import json
    
    # Get one raw record
    if args.car_id:
        raw = await db.raw.find_one({"car_id": args.car_id})
    else:
        raw = await db.raw.find_one({"listing_status": True})
    
    if not raw:
        print("No raw record found")
        return
    
    print(f"Testing record: {raw.get('car_id', 'unknown')}")
    
    # Load site settings
    site_settings = await SiteSettings(db).get(args.site)
    
    # Test cleaning
    cleaner = Cleaner(db, args.site)
    cleaned = await cleaner.clean_raw_record(raw, f"[{args.site}]")
    
    if not cleaned:
        print("❌ Record failed cleaning validation")
        return
    
    print("✅ Record passed cleaning validation")
    
    # Test site filter check
    site_filters = site_settings.get("filter_criteria", {})
    processable = await cleaner.is_record_processable_for_site(cleaned, site_filters, f"[{args.site}]")
    
    if not processable:
        print("❌ Record excluded by site filters")
        return
    
    print("✅ Record passed site filters")
    
    # Test full processing
    processor = Processor(db, args.site)
    processed = await processor.process_single_record(cleaned, site_settings)
    
    if not processed:
        print("❌ Processing returned None")
        return
    
    print("✅ Processing successful")
    print("\nProcessed output:")
    print(json.dumps(processed, indent=2, default=str))

async def main():
    """
    Main entry point - parse arguments and route to appropriate handler.
    """
    parser = argparse.ArgumentParser(
        description="Rawprocessor Stage 1 - MongoDB to WordPress Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start daemon mode (production)
  python main.py --daemon
  
  # Manual triggers
  python main.py --trigger site_settings.filters_changed --site solostaging
  python main.py --trigger weekly_scheduled_job
  
  # Maintenance operations  
  python main.py --rebuild-site solostaging
  python main.py --retry-failed --site solostaging --limit 50
  
  # Testing and monitoring
  python main.py --test-one --site solostaging --car-id "7ade-23a6-1e5e"
  python main.py --status
        """
    )
    
    # Mode selection
    parser.add_argument("--daemon", action="store_true", help="Start daemon mode (MongoDB change streams)")
    parser.add_argument("--trigger", help="Manual trigger type")
    parser.add_argument("--rebuild-site", action="store_true", help="Rebuild all records for a site")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed queue jobs")
    parser.add_argument("--status", action="store_true", help="Show system status")
    parser.add_argument("--test-one", action="store_true", help="Test processing of one record")
    
    # Parameters
    parser.add_argument("--site", help="Site to operate on")
    parser.add_argument("--car-id", help="Specific car ID for testing")
    parser.add_argument("--data", help="JSON data for manual triggers")
    parser.add_argument("--limit", type=int, help="Limit for batch operations")
    
    # Logging
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Configure logging
    log_site = args.site or "system"
    configure_logger(log_site)
    
    # Set log level
    logger.remove()
    logger.add(sys.stdout, level=args.log_level)
    if args.site:
        logger.add(f"./logs/{args.site}.log", rotation="10 MB", retention="7 days", level=args.log_level)
    
    # Setup signal handlers for daemon mode
    if args.daemon:
        setup_signal_handlers()
    
    try:
        # Route to appropriate handler
        if args.daemon:
            await start_daemon_mode()
        elif args.trigger:
            await handle_manual_trigger(args)
        elif args.rebuild_site:
            await handle_rebuild_site(args)
        elif args.retry_failed:
            await handle_retry_failed(args)
        elif args.status:
            await handle_status()
        elif args.test_one:
            await handle_test_one(args)
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as ex:
        logger.error(f"Fatal error: {ex}")
        sys.exit(1)
    finally:
        # Cleanup
        if client:
            client.close()

if __name__ == "__main__":
    asyncio.run(main())