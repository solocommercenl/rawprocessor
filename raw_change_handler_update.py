# Import the filter function
import sys
import os
sys.path.append(os.path.dirname(__file__))
from utils_filters import check_raw_against_filters
from site_settings import SiteSettings

# Updated method for queued_trigger_system.py
async def _handle_raw_change_queued(self, change: Dict[str, Any]):
    """
    Handle raw collection changes by queueing processing jobs.
    Only queue jobs for sites where the record passes site filters.
    """
    try:
        operation_type = change.get("operationType")
        document = change.get("fullDocument")
        if not document:
            logger.warning("Raw change event missing fullDocument")
            return
        car_id = document.get("car_id", "unknown")
        listing_status = document.get("listing_status", True)
        
        # Only process active listings or status changes
        if not listing_status and operation_type != "update":
            logger.debug(f"Skipping inactive listing: {car_id}")
            return
        
        logger.info(f"Evaluating raw {operation_type} for car_id={car_id}")
        
        # Check which sites this record is valid for
        valid_sites = []
        site_settings_obj = SiteSettings(self.db)
        
        for site in self.active_sites:
            try:
                site_settings = await site_settings_obj.get(site)
                filter_criteria = site_settings.get("filter_criteria", {})
                
                if check_raw_against_filters(document, filter_criteria):
                    valid_sites.append(site)
                    logger.debug(f"Record {car_id} passes filters for site {site}")
                else:
                    logger.debug(f"Record {car_id} excluded by filters for site {site}")
                    
            except Exception as ex:
                logger.error(f"Error checking filters for site {site}: {ex}")
                # On error, include the site to be safe
                valid_sites.append(site)
        
        if valid_sites:
            # Queue processing jobs only for sites where record passes filters
            job_ids = await self.processing_queue.enqueue_raw_change(
                operation_type=operation_type,
                car_id=car_id,
                raw_document=document,
                sites=valid_sites
            )
            logger.info(f"Queued {len(job_ids)} processing jobs for car_id={car_id} (valid for {len(valid_sites)}/{len(self.active_sites)} sites)")
        else:
            logger.info(f"Record {car_id} excluded by all site filters - no jobs queued")
            
    except Exception as ex:
        logger.error(f"Error handling raw change event: {ex}")
