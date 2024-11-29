from datetime import datetime, date, timedelta
from typing import Dict, Set, Tuple, Optional, List, Any
from pathlib import Path
import json
import logging
from google.cloud import storage
from data_sources.oura.config.constants import HISTORICAL_DAYS
from data_sources.oura.pipelines.dataflow_pipeline import OuraPipeline

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

def get_existing_dates(pipeline: OuraPipeline) -> Dict[str, Set[date]]:
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

def get_raw_data_dates(raw_data_path: Path) -> Dict[str, Set[date]]:
    """
    Get dates available in raw data files from GCS.
    
    Args:
        raw_data_path (Path): Path to raw data directory
        
    Returns:
        Dict[str, Set[date]]: Dictionary with data types as keys and sets of dates as values
    """
    try:
        storage_client = storage.Client()
        
        path_parts = str(raw_data_path).split('/')
        bucket_name = path_parts[0]
        prefix = '/'.join(path_parts[1:])
        
        bucket = storage_client.bucket(bucket_name)
        
        dates: Dict[str, Set[date]] = {
            'activity': set(),
            'sleep': set(),
            'readiness': set()
        }
        
        # List all blobs in the bucket with the given prefix
        blobs = bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Extract data type and date range from blob path
            # Expected format: raw/oura/{data_type}/{start_date}_{end_date}/data.json
            path_parts = blob.name.split('/')
            if len(path_parts) >= 4 and path_parts[-1] == 'data.json':
                data_type = path_parts[-3]
                date_range = path_parts[-2].split('_')
                
                if len(date_range) == 2 and data_type in dates:
                    try:
                        # Load and parse the JSON data to find the actual dates
                        content = blob.download_as_string()
                        data = json.loads(content)
                        
                        # Extract all dates from the data
                        for record in data.get('data', []):
                            if 'day' in record:
                                date_obj = datetime.strptime(record['day'], '%Y-%m-%d').date()
                                dates[data_type].add(date_obj)
                                
                    except (ValueError, json.JSONDecodeError) as e:
                        logger.warning(f"Error processing data in {blob.name}: {e}")
        
        return dates
        
    except Exception as e:
        logger.error(f"Error reading raw data dates: {e}")
        return {
            'activity': set(),
            'sleep': set(),
            'readiness': set()
        }

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
    
    # Find the latest date across all data types
    all_dates = set().union(*raw_dates.values()) if raw_dates.values() else set()
    latest_date = max(all_dates) if all_dates else yesterday - timedelta(days=90)
    
    # Start from the day after the latest existing date
    start_date = latest_date + timedelta(days=1)
    
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
