from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_latest_date(existing_dates):
    """Get the most recent date across all data types"""
    all_dates = []
    for dates in existing_dates.values():
        if dates:  # Only include non-empty sets
            all_dates.extend(dates)
    
    return max(all_dates) if all_dates else None

def get_date_range(existing_dates):
    """
    Calculate start and end dates based on existing data
    Returns tuple of (start_date, end_date)
    """
    end_date = datetime.now().date()
    latest_date = get_latest_date(existing_dates)
    
    if latest_date:
        # If we have data, start from the day after the latest date
        start_date = latest_date + timedelta(days=1)
        logger.info(f"Found existing data up to {latest_date}, fetching from {start_date}")
    else:
        # If no data exists, fetch last 365 days
        start_date = end_date - timedelta(days=365)
        logger.info("No existing data found, fetching last 365 days")
        
    return start_date, end_date

def get_existing_dates(pipeline):
    """Get existing dates for all Oura tables"""
    return {
        'activity': pipeline.loader.get_existing_dates('oura_activity'),
        'sleep': pipeline.loader.get_existing_dates('oura_sleep'),
        'readiness': pipeline.loader.get_existing_dates('oura_readiness')
    }
