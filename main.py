"""
main.py (Updated for Queued Processing with Periodic Cleanup)

Entrypoint and orchestrator for rawprocessor Stage 1 with queued processing architecture.
Handles MongoDB change streams with processing queue for high-volume operations.
Includes periodic database cleanup every 8 hours.

UPDATED: Now uses centralized configuration system.
"""

import asyncio
import os
import sys
import argparse
import signal
from typing import Any, Dict
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger

from config import config
from logger import configure_logger, log_exceptions
from queued_trigger_system import QueuedTriggerSystem
from processing_queue import ProcessingQueue
from jobqueue import WPQueue
from processor import Processor
from cleaner import Cleaner

if not config.MONGO_URI:
    raise ValueError("MONGO_URI environment variable required")

# Global state with improved connection settings from config
client = AsyncIOMotorClient(
    config.MONGO_URI,
    maxPoolSize=config.MONGO_MAX_POOL_SIZE,
    serverSelectionTimeoutMS=config.MONGO_SERVER_SELECTION_TIMEOUT,
    connectTimeoutMS=config.MONGO_CONNECT_TIMEOUT,
    socketTimeoutMS=config.MONGO_SOCKET_TIMEOUT
)
db = client[config.MONGO_DB]
trigger_system = None
processing_queue = None
shutdown_event = asyncio.Event()

async def get_trigger_system():
    """Get or create trigger system singleton with proper initialization."""
    global trigger_system
    if not trigger_system:
        trigger_system = QueuedTriggerSystem(db)
        await trigger_system.initialize()
    return trigger_system

async def get_processing_queue():
    """Get or create processing queue singleton with proper initialization."""
    global processing_queue
    if not processing_queue:
        processing_queue = ProcessingQueue(db)
        await processing_queue.initialize()
    return processing_queue

def setup_signal_handlers():
    """Setup graceful shutdown signal handlers."""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

async def run_periodic_cleanup():
    """
    Run database cleanup based on configured interval.
    Deletes invalid records from raw collection to maintain data quality.
    """
    if not config.ENABLE_PERIODIC_CLEANUP:
        logger.info("Periodic cleanup disabled by configuration")
        return
    
    cleaner = Cleaner(db)
    cleanup_interval = config.CLEANUP_INTERVAL_HOURS * 3600  # Convert to seconds
    
    logger.info(f"Starting periodic cleanup task (every {config.CLEANUP_INTERVAL_HOURS} hours)")
    
    while not shutdown_event.is_set():
        try:
            # Wait for configured interval or until shutdown
            await asyncio.wait_for(shutdown_event.wait(), timeout=cleanup_interval)
            break  # Shutdown event was set
        except asyncio.TimeoutError:
            # Timeout occurred, run cleanup
            pass
        
        try:
            logger.info("Running periodic database cleanup...")
            stats = await cleaner.cleanup_raw_collection()
            
            total_deleted = stats.get("total_deleted", 0)
            duration = stats.get("duration_seconds", 0)
            
            logger.info(f"Periodic cleanup completed: {total_deleted} records deleted in {duration:.2f}s")
            
            inactive_stats = await cleaner.cleanup_inactive_records()
        
            inactive_deleted = inactive_stats.get("deleted_from_raw", 0) + inactive_stats.get("deleted_from_processed", 0)
            inactive_duration = inactive_stats.get("duration_seconds", 0)
            
            logger.info(f"Inactive cleanup completed: {inactive_deleted} records deleted in {inactive_duration:.2f}s")
        
        except Exception as ex:
            logger.error(f"Error during periodic cleanup: {ex}")

@log_exceptions
async def start_daemon_mode():
    """
    Start the daemon mode with queued processing system and periodic cleanup.
    """
    logger.info("Starting rawprocessor Stage 1 daemon with queued processing...")
    
    # Log configuration summary
    config_summary = config.summary()
    logger.info("Configuration loaded:")
    logger.info(f"  - Database: {config_summary['database']['database']} (pool: {config_summary['database']['pool_size']})")
    logger.info(f"  - Workers: {config_summary['processing']['max_workers']}, Batch: {config_summary['processing']['batch_size']}")
    logger.info(f"  - Cleanup: every {config_summary['maintenance']['cleanup_hours']}h")
    logger.info(f"  - Features: cleanup={config_summary['features']['periodic_cleanup']}, health={config_summary['features']['health_server']}")
    
    try:
        # Initialize queued trigger system
        trigger_sys = await get_trigger_system()
        
        # Start change stream monitoring and workers
        monitor_task = asyncio.create_task(trigger_sys.start_watching())
        
        # Start periodic maintenance tasks
        maintenance_task = asyncio.create_task(run_maintenance_tasks())
        
        # Start periodic cleanup task (every configured hours)
        cleanup_task = asyncio.create_task(run_periodic_cleanup())
        
        # Wait for shutdown signal
        await shutdown_event.wait()
        
        logger.info("Shutdown signal received, stopping daemon...")
        
        # Stop monitoring, maintenance, and cleanup
        await trigger_sys.stop_watching()
        monitor_task.cancel()
        maintenance_task.cancel()
        cleanup_task.cancel()
        
        try:
            await asyncio.gather(monitor_task, maintenance_task, cleanup_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
            
        logger.info("Daemon stopped gracefully")
        
    except Exception as ex:
        logger.error(f"Error in daemon mode: {ex}")
        raise

async def run_maintenance_tasks():
    """
    Run periodic maintenance tasks based on configured interval.
    """
    maintenance_interval = config.MAINTENANCE_INTERVAL_HOURS * 3600  # Convert to seconds
    
    try:
        while not shutdown_event.is_set():
            try:
                # Wait for configured interval or until shutdown
                await asyncio.wait_for(shutdown_event.wait(), timeout=maintenance_interval)
                break  # Shutdown event was set
            except asyncio.TimeoutError:
                # Timeout occurred, run maintenance
                pass
            
            trigger_sys = await get_trigger_system()
            logger.info("Running periodic maintenance tasks...")
            
            # Clean up old completed jobs (configured retention)
            try:
                cleaned_count = await trigger_sys.cleanup_old_jobs(hours=config.JOB_CLEANUP_RETENTION_HOURS)
                logger.info(f"Maintenance: Cleaned up {cleaned_count} old jobs")
            except Exception as ex:
                logger.error(f"Maintenance error during cleanup: {ex}")
            
            # Log system performance metrics
            if config.ENABLE_PERFORMANCE_MONITORING:
                try:
                    metrics = await trigger_sys.get_processing_performance_metrics()
                    processed = metrics.get("performance", {}).get("total_jobs_processed", 0)
                    avg_time = metrics.get("performance", {}).get("average_processing_time", 0)
                    logger.info(f"Performance: {processed} jobs processed, avg time: {avg_time:.2f}s")
                except Exception as ex:
                    logger.error(f"Maintenance error getting metrics: {ex}")
                
    except asyncio.CancelledError:
        logger.info("Maintenance tasks cancelled")

@log_exceptions  
async def handle_manual_trigger(args):
    """
    Handle manual trigger execution with queued processing.
    """
    trigger_sys = await get_trigger_system()
    
    trigger_data = {}
    if args.data:
        import json
        trigger_data = json.loads(args.data)
    
    await trigger_sys.manual_trigger(args.trigger, args.site, trigger_data)
    logger.info(f"Manual trigger '{args.trigger}' queued successfully")

@log_exceptions
async def handle_retry_failed(args):
    """
    Retry failed processing jobs or WP queue jobs.
    """
    if not args.site and not args.processing:
        raise ValueError("Either --site (for WP jobs) or --processing (for processing jobs) required")
    
    if args.processing:
        # Retry failed processing jobs
        pq = await get_processing_queue()
        count = await pq.retry_failed_jobs(limit=args.limit or 100)
        logger.info(f"Retried {count} failed processing jobs")
    
    else:
        # Retry failed WP queue jobs
        queue = WPQueue(db, args.site)
        count = await queue.retry_failed_jobs(limit=args.limit or 100)
        logger.info(f"Retried {count} failed WP jobs for site '{args.site}'")

@log_exceptions
async def handle_rebuild_site(args):
    """
    Rebuild all records for a site using queued processing.
    """
    if not args.site:
        raise ValueError("--site required for --rebuild-site")
    
    trigger_sys = await get_trigger_system()
    await trigger_sys.manual_trigger("rebuild_site", args.site)
    logger.info(f"Site rebuild queued for '{args.site}'")

@log_exceptions
async def handle_preprocess(args):
    """
    Run manual preprocessing to clean raw collection.
    """
    logger.info("Running manual preprocessing...")
    
    cleaner = Cleaner(db)
    
    # Show what would be deleted (dry run)
    if args.dry_run:
        candidates = await cleaner.get_cleanup_candidates_count()
        logger.info(f"Cleanup candidates found:")
        logger.info(f"  - Records with < {config.MIN_IMAGES_REQUIRED} images: {candidates['insufficient_images']}")
        logger.info(f"  - Records with invalid emissions: {candidates['invalid_emissions']}")
        logger.info(f"  - Total candidates: {candidates['total_candidates']}")
        logger.info(f"  - Protected by processing: {candidates.get('protected_by_processing', 0)}")
        logger.info("Use --preprocess without --dry-run to actually delete these records")
    else:
        # Actually run cleanup
        stats = await cleaner.cleanup_raw_collection()
        total_deleted = stats.get("total_deleted", 0)
        duration = stats.get("duration_seconds", 0)
        
        logger.info(f"Manual preprocessing completed:")
        logger.info(f"  - Total deleted: {total_deleted} records")
        logger.info(f"  - Duration: {duration:.2f} seconds")
        
        if total_deleted > 0:
            logger.info("Recommendation: Restart processing to ensure clean pipeline")
@log_exceptions
async def handle_manual_maintenance():
    """
    Run the same maintenance tasks that run every 8 hours.
    """
    logger.info("Running manual maintenance cycle...")
    
    cleaner = Cleaner(db)
    
    try:
        # Run data quality cleanup (same as 8-hour cycle)
        logger.info("Running data quality cleanup...")
        stats = await cleaner.cleanup_raw_collection()
        
        total_deleted = stats.get("total_deleted", 0)
        duration = stats.get("duration_seconds", 0)
        
        logger.info(f"Data quality cleanup completed: {total_deleted} records deleted in {duration:.2f}s")
        
        # Run inactive record cleanup (same as 8-hour cycle)
        logger.info("Running inactive records cleanup...")
        inactive_stats = await cleaner.cleanup_inactive_records()
        
        inactive_deleted = inactive_stats.get("deleted_from_raw", 0) + inactive_stats.get("deleted_from_processed", 0)
        inactive_duration = inactive_stats.get("duration_seconds", 0)
        
        logger.info(f"Inactive cleanup completed: {inactive_deleted} records deleted in {inactive_duration:.2f}s")
        
        logger.info("Manual maintenance cycle completed successfully")
        
    except Exception as ex:
        logger.error(f"Error during manual maintenance: {ex}")
        raise

@log_exceptions
async def handle_status():
    """
    Show comprehensive system status including queue metrics.
    """
    trigger_sys = await get_trigger_system()
    
    status = await trigger_sys.get_system_status()
    performance = await trigger_sys.get_processing_performance_metrics()
    
    print("=== Rawprocessor Stage 1 Status (Queued) ===")
    print(f"Running: {status['running']}")
    print(f"Active Sites: {status['site_count']}")
    print(f"Sites: {', '.join(status['active_sites'])}")
    print(f"Timestamp: {status['timestamp']}")
    
    # Show configuration summary
    config_summary = config.summary()
    print(f"\n=== Configuration ===")
    print(f"Workers: {config_summary['processing']['max_workers']}")
    print(f"Batch Size: {config_summary['processing']['batch_size']}")
    print(f"Cleanup Interval: {config_summary['maintenance']['cleanup_hours']}h")
    print(f"Min Images: {config_summary['data_quality']['min_images']}")
    
    print("\n=== Processing Queue Status ===")
    pq_stats = status.get('processing_queue', {})
    print(f"Total Jobs: {pq_stats.get('total_jobs', 0)}")
    print(f"Workers Running: {pq_stats.get('workers_running', False)}")
    print(f"Active Workers: {pq_stats.get('active_workers', 0)}")
    
    status_breakdown = pq_stats.get('status_breakdown', {})
    for status_name, count in status_breakdown.items():
        print(f"  {status_name}: {count}")
    
    print("\n=== Processing Performance ===")
    perf = performance.get('performance', {})
    print(f"Total Processed: {perf.get('total_jobs_processed', 0)}")
    print(f"Total Failed: {perf.get('jobs_failed', 0)}")
    print(f"Average Processing Time: {perf.get('average_processing_time', 0):.2f}s")
    print(f"Last Activity: {perf.get('last_activity', 'Never')}")
    
    print("\n=== WordPress Queue Status ===")
    for site, stats in status.get('wp_queue_stats', {}).items():
        if 'error' in stats:
            print(f"{site}: ERROR - {stats['error']}")
        else:
            print(f"{site}: {stats['pending']} pending, {stats['failed']} failed")
    
    print("\n=== Site Breakdown (Processing Queue) ===")
    site_breakdown = pq_stats.get('site_breakdown', {})
    for site, site_stats in site_breakdown.items():
        print(f"{site}:")
        for status_name, count in site_stats.items():
            print(f"  {status_name}: {count}")

@log_exceptions
async def handle_test_one(args):
    """
    Test processing of a single raw record using the standard processor.
    """
    if not args.site:
        raise ValueError("--site required for --test-one")
    
    from site_settings import SiteSettings
    from utils_filters import is_record_clean, check_raw_against_filters
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
    
    # Test basic cleaning (using utils_filters now)
    is_clean = is_record_clean(raw)
    if not is_clean:
        print(f"❌ Record failed basic quality validation (min {config.MIN_IMAGES_REQUIRED} images required)")
        return
    
    print("✅ Record passed basic quality validation")
    
    # Test site filter check
    site_filters = site_settings.get("filter_criteria", {})
    processable = check_raw_against_filters(raw, site_filters, f"[{args.site}]")
    
    if not processable:
        print("❌ Record excluded by site filters")
        return
    
    print("✅ Record passed site filters")
    
    # Test full processing
    processor = Processor(db, args.site)
    processed = await processor.process_single_record(raw, site_settings)
    
    if not processed:
        print("❌ Processing returned None")
        return
    
    print("✅ Processing successful")
    print("\nProcessed output:")
    print(json.dumps(processed, indent=2, default=str))

@log_exceptions
async def handle_queue_status(args):
    """
    Show detailed queue status and performance metrics.
    """
    pq = await get_processing_queue()
    stats = await pq.get_queue_stats()
    
    print("=== Processing Queue Detailed Status ===")
    print(f"Total Jobs: {stats.get('total_jobs', 0)}")
    print(f"Workers Running: {stats.get('workers_running', False)}")
    print(f"Active Workers: {stats.get('active_workers', 0)}")
    print(f"Timestamp: {stats.get('timestamp', 'Unknown')}")
    
    print("\n=== Status Breakdown ===")
    for status, count in stats.get('status_breakdown', {}).items():
        print(f"  {status}: {count}")
    
    print("\n=== Site Breakdown ===")
    for site, site_stats in stats.get('site_breakdown', {}).items():
        print(f"{site}:")
        for status, count in site_stats.items():
            print(f"  {status}: {count}")
    
    print("\n=== Processing Stats ===")
    proc_stats = stats.get('processing_stats', {})
    for key, value in proc_stats.items():
        print(f"  {key}: {value}")

@log_exceptions
async def handle_cleanup_jobs(args):
    """
    Clean up old completed jobs.
    """
    pq = await get_processing_queue()
    hours = args.hours or config.JOB_CLEANUP_RETENTION_HOURS
    count = await pq.cleanup_completed_jobs(older_than_hours=hours)
    print(f"Cleaned up {count} completed jobs older than {hours} hours")

async def main():
    """
    Main entry point - parse arguments and route to appropriate handler.
    """
    parser = argparse.ArgumentParser(
        description="Rawprocessor Stage 1 - MongoDB to WordPress Data Pipeline (Queued Processing)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start daemon mode (production) - includes periodic cleanup
  python main.py --daemon
  
  # Manual preprocessing (database cleanup)
  python main.py --preprocess --dry-run  # See what would be deleted
  python main.py --preprocess             # Actually delete invalid records
  
  # Manual triggers (now queued)
  python main.py --trigger site_settings.filters_changed --site solostaging
  python main.py --trigger weekly_scheduled_job
  
  # Maintenance operations  
  python main.py --rebuild-site solostaging
  python main.py --retry-failed --site solostaging --limit 50
  python main.py --retry-failed --processing --limit 100
  
  # Queue management
  python main.py --queue-status
  python main.py --cleanup-jobs --hours 48
  
  # Testing and monitoring
  python main.py --test-one --site solostaging --car-id "7ade-23a6-1e5e"
  python main.py --status
        """
    )
    
    # Mode selection
    parser.add_argument("--daemon", action="store_true", help="Start daemon mode (MongoDB change streams + queue workers + periodic cleanup)")
    parser.add_argument("--trigger", help="Manual trigger type")
    parser.add_argument("--rebuild-site", action="store_true", help="Rebuild all records for a site (queued)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed jobs")
    parser.add_argument("--status", action="store_true", help="Show system status")
    parser.add_argument("--test-one", action="store_true", help="Test processing of one record")
    parser.add_argument("--queue-status", action="store_true", help="Show detailed queue status")
    parser.add_argument("--cleanup-jobs", action="store_true", help="Clean up old completed jobs")
    parser.add_argument("--preprocess", action="store_true", help="Run manual preprocessing to clean raw collection")
    
    # Parameters
    parser.add_argument("--site", help="Site to operate on")
    parser.add_argument("--car-id", help="Specific car ID for testing")
    parser.add_argument("--data", help="JSON data for manual triggers")
    parser.add_argument("--limit", type=int, help="Limit for batch operations")
    parser.add_argument("--hours", type=int, help="Hours for cleanup operations")
    parser.add_argument("--processing", action="store_true", help="Target processing queue instead of WP queue")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without actually doing it")
    parser.add_argument("--maintenance", action="store_true", help="Run full 8-hour maintenance cycle manually")

    # Logging
    parser.add_argument("--log-level", default=config.LOG_LEVEL, choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Configure logging
    log_site = args.site or "system"
    configure_logger(log_site)
    
    # Set log level
    logger.remove()
    logger.add(sys.stdout, level=args.log_level)
    if args.site:
        logger.add(f"./logs/{args.site}.log", rotation=config.LOG_ROTATION_SIZE, retention=config.LOG_RETENTION_DAYS, level=args.log_level)
    
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
        elif args.queue_status:
            await handle_queue_status(args)
        elif args.cleanup_jobs:
            await handle_cleanup_jobs(args)
        elif args.preprocess:
            await handle_preprocess(args)
        elif args.maintenance:
            await handle_manual_maintenance()
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