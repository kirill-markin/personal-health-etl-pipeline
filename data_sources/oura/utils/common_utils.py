from datetime import datetime, date, timedelta
from typing import Dict, Set, Tuple, Optional, List, Any
from pathlib import Path
import json
import logging
from google.cloud import storage
from data_sources.oura.config.constants import HISTORICAL_DAYS, DATA_TYPES
from data_sources.oura.etl.load import OuraLoader

logger = logging.getLogger(__name__)

def get_latest_date(existing_dates: Dict[str, Set[date]]) -> Optional[date]:
    """Get the most recent date across all data types"""
    all_dates = set().union(*existing_dates.values())
    return max(all_dates) if all_dates else None

def get_date_range(existing_dates: Dict[str, Set[date]]) -> Tuple[date, date]:
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

def get_raw_data_dates(raw_data_path: Path) -> Dict[str, Set[date]]:
    """
    Get dates available in raw data files from GCS.
    
    Args:
        raw_data_path (Path): Path to raw data directory (e.g., 'oura-raw-data/raw/oura')
        
    Returns:
        Dict[str, Set[date]]: Dictionary with data types as keys and sets of dates as values
    """
    try:
        storage_client = storage.Client()
        
        path_parts = str(raw_data_path).split('/')
        bucket_name = path_parts[0]
        prefix = '/'.join(path_parts[1:])
        
        bucket = storage_client.bucket(bucket_name)
        
        # Initialize dates dictionary with all data types from constants
        dates: Dict[str, Set[date]] = {
            data_type: set() 
            for data_type in DATA_TYPES.keys()
        }
        
        # List all blobs in the bucket with the given prefix
        blobs = bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Extract data type and date range from folder structure
            # Expected format: raw/oura/{data_type}/{date_range}/data.json
            path_parts = blob.name.split('/')
            if len(path_parts) >= 4 and path_parts[-1] == 'data.json':
                data_type = path_parts[-3]
                date_range_str = path_parts[-2]
                
                if data_type in dates and '_' in date_range_str:
                    start_str, end_str = date_range_str.split('_')
                    try:
                        start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
                        end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
                        
                        # Add all dates in the range to the set
                        current_date = start_date
                        while current_date <= end_date:
                            dates[data_type].add(current_date)
                            current_date += timedelta(days=1)
                            
                    except ValueError as e:
                        logger.warning(f"Invalid date format in folder {date_range_str}: {e}")
        
        return dates
        
    except Exception as e:
        logger.error(f"Error reading raw data dates: {e}")
        return {data_type: set() for data_type in DATA_TYPES.keys()}

def get_dates_to_extract(raw_dates: Dict[str, set[date]]) -> Tuple[date, date]:
    """
    Calculate start and end dates for data extraction based on existing raw data
    
    Args:
        raw_dates: Dictionary with data types as keys and sets of existing dates as values
    
    Returns:
        Tuple of start_date and end_date for extraction
    """
    today = date.today()
    yesterday = today - timedelta(days=1)  # We only need data up to yesterday
    
    # Find the earliest date we need to fetch (latest date + 1 for each type)
    start_dates = []
    for dates in raw_dates.values():
        if dates:
            latest_date = max(dates)
            start_dates.append(latest_date + timedelta(days=1))
        else:
            start_dates.append(yesterday - timedelta(days=90))
    
    # Start from the earliest date needed
    start_date = min(start_dates) if start_dates else yesterday - timedelta(days=90)
    
    # Only extract up to yesterday
    end_date = yesterday
    
    return start_date, end_date

def get_dates_for_transform(
    raw_dates: Dict[str, Set[date]], 
    bq_dates: Dict[str, Set[date]]
) -> Dict[str, Set[date]]:
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
        
    missing_dates: Dict[str, Set[date]] = {}
    
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
