from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional, List, Any
from pathlib import Path
import json
import logging
from google.cloud import storage
from data_sources.oura.config.constants import HISTORICAL_DAYS
from data_sources.oura.pipelines.dataflow_pipeline import OuraPipeline

logger = logging.getLogger(__name__)

def get_latest_date(existing_dates: Dict[str, Set[datetime.date]]) -> Optional[datetime.date]:
    """Get the most recent date across all data types"""
    all_dates = set().union(*existing_dates.values())
    return max(all_dates) if all_dates else None

def get_date_range(existing_dates: Dict[str, Set[datetime.date]]) -> Tuple[datetime.date, datetime.date]:
    """Calculate start and end dates based on existing data"""
    if not isinstance(existing_dates, dict):
        raise TypeError("existing_dates must be a dictionary")
        
    end_date = datetime.now().date()
    latest_date = get_latest_date(existing_dates)
    
    if latest_date:
        # Ensure we don't try to fetch future data
        start_date = min(latest_date + timedelta(days=1), end_date)
        logger.info(f"Found existing data up to {latest_date}, fetching from {start_date}")
    else:
        # If no data exists, fetch historical days but not future dates
        start_date = end_date - timedelta(days=HISTORICAL_DAYS)
        logger.info(f"No existing data found, fetching last {HISTORICAL_DAYS} days")
    
    return start_date, end_date

def get_existing_dates(pipeline: OuraPipeline) -> Dict[str, Set[datetime.date]]:
    """
    Get existing dates from BigQuery tables
    
    Args:
        pipeline: OuraPipeline instance with configured loader
        
    Returns:
        Dict mapping data types to sets of existing dates
    """
    return {
        'activity': pipeline.loader.get_existing_dates('oura_activity'),
        'sleep': pipeline.loader.get_existing_dates('oura_sleep'),
        'readiness': pipeline.loader.get_existing_dates('oura_readiness')
    }

def get_raw_data_dates(raw_data_path: Path) -> Dict[str, Set[datetime.date]]:
    """
    Get dates available in raw data files from GCS.
    
    Args:
        raw_data_path (Path): Path to raw data directory
        
    Returns:
        Dict[str, Set[datetime.date]]: Dictionary with data types as keys and sets of dates as values
        
    Raises:
        ValueError: If raw_data_path is invalid
        google.cloud.exceptions.GoogleCloudError: If GCS operations fail
    """
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        
        # Parse bucket name and prefix from raw_data_path
        path_parts = str(raw_data_path).split('/')
        bucket_name = path_parts[0]
        prefix = '/'.join(path_parts[1:])
        
        bucket = storage_client.bucket(bucket_name)
        
        # Initialize result dictionary
        dates: Dict[str, Set[datetime.date]] = {
            'activity': set(),
            'sleep': set(),
            'readiness': set()
        }
        
        # List all blobs in the bucket with the given prefix
        blobs = bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Extract data type and date from blob path
            # Expected format: raw/oura/{data_type}/{date}/data.json
            path_parts = blob.name.split('/')
            if len(path_parts) >= 4 and path_parts[-1] == 'data.json':
                data_type = path_parts[-3]
                date_str = path_parts[-2]
                
                if data_type in dates:
                    try:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                        dates[data_type].add(date_obj)
                    except ValueError as e:
                        logger.warning(f"Invalid date format in path {blob.name}: {e}")
        
        return dates
        
    except Exception as e:
        logger.error(f"Error reading raw data dates: {e}")
        return {
            'activity': set(),
            'sleep': set(),
            'readiness': set()
        }

def get_dates_to_extract(
    raw_dates: Dict[str, Set[datetime.date]], 
    end_date: datetime.date
) -> Tuple[datetime.date, datetime.date]:
    """
    Calculate date range for extraction, considering only raw data dates.
    
    Args:
        raw_dates: Dictionary mapping data types to sets of dates in raw storage
        end_date: Upper bound for date range (usually current date)
        
    Returns:
        Tuple of start_date and end_date for extraction
    """
    # Combine all existing dates from raw storage
    all_raw_dates = set().union(*raw_dates.values())
    
    if all_raw_dates:
        start_date = max(all_raw_dates) + timedelta(days=1)
    else:
        start_date = end_date - timedelta(days=365)
        
    # Don't try to fetch future dates
    if start_date > end_date:
        logger.info("No new dates to extract")
        return end_date, end_date
        
    return start_date, end_date

def get_dates_for_transform(
    raw_dates: Dict[str, Set[datetime.date]], 
    bq_dates: Dict[str, Set[datetime.date]]
) -> Dict[str, Set[datetime.date]]:
    """
    Find dates that exist in raw data but are missing in BigQuery.
    
    Args:
        raw_dates: Dictionary of data types and their available dates in raw storage
        bq_dates: Dictionary of data types and their existing dates in BigQuery
        
    Returns:
        Dictionary mapping data types to sets of dates that need transformation
        
    Raises:
        TypeError: If inputs are not of correct type
    """
    if not isinstance(raw_dates, dict) or not isinstance(bq_dates, dict):
        raise TypeError("Both raw_dates and bq_dates must be dictionaries")
        
    missing_dates: Dict[str, Set[datetime.date]] = {}
    
    try:
        for data_type in raw_dates:
            if not isinstance(raw_dates[data_type], set):
                raise TypeError(f"Values in raw_dates must be sets, got {type(raw_dates[data_type])} for {data_type}")
            bq_dates_for_type = bq_dates.get(data_type, set())
            if not isinstance(bq_dates_for_type, set):
                raise TypeError(f"Values in bq_dates must be sets, got {type(bq_dates_for_type)} for {data_type}")
            missing_dates[data_type] = raw_dates[data_type] - bq_dates_for_type
            
        return missing_dates
    except Exception as e:
        logger.error(f"Error calculating missing dates: {e}")
        raise
