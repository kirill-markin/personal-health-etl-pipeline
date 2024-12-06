from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Set, Any, List
import re
import logging
import os
from airflow.models.variable import Variable
from google.cloud import storage

from ..config.constants import DATA_TYPES

logger = logging.getLogger(__name__)

def get_raw_data_dates(raw_data_path: Path) -> Dict[str, Set[date]]:
    """
    Get existing dates from raw data files
    
    Args:
        raw_data_path: Path to raw data directory
        
    Returns:
        Dict mapping data types to sets of dates
    """
    dates: Dict[str, Set[date]] = {}
    date_ranges: Dict[str, List[tuple[date, date]]] = {}
    
    try:
        # Initialize storage client
        storage_client = storage.Client()
        bucket_name = raw_data_path.parts[0]  # Get bucket name from path
        bucket = storage_client.bucket(bucket_name)
        
        # Pattern to match date range directories
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})')
        
        # Initialize sets for all data types from constants
        for data_type in DATA_TYPES.keys():
            dates[data_type] = set()
            date_ranges[data_type] = []
            
            # List all blobs with the prefix for this data type
            prefix = f"raw/oura/{data_type}/"
            blobs = bucket.list_blobs(prefix=prefix)
            
            for blob in blobs:
                # Skip if not a data file
                if not blob.name.endswith('data.json'):
                    continue
                    
                # Extract date range from path
                path_parts = blob.name.split('/')
                if len(path_parts) < 4:
                    continue
                    
                date_dir = path_parts[-2]  # Get the date directory name
                if match := date_pattern.search(date_dir):
                    start_date = date.fromisoformat(match.group(1))
                    end_date = date.fromisoformat(match.group(2))
                    
                    # Check for overlaps
                    for existing_start, existing_end in date_ranges[data_type]:
                        if (start_date <= existing_end and end_date >= existing_start):
                            raise ValueError(
                                f"Overlapping date ranges detected for {data_type}: "
                                f"{start_date}-{end_date} overlaps with "
                                f"{existing_start}-{existing_end}"
                            )
                    
                    date_ranges[data_type].append((start_date, end_date))
                    
                    # Add all dates in range
                    current_date = start_date
                    while current_date <= end_date:
                        dates[data_type].add(current_date)
                        current_date += timedelta(days=1)
            
            logger.info(f"Found {len(dates[data_type])} dates for {data_type}")
            
        return dates
        
    except Exception as e:
        logger.error(f"Error in get_raw_data_dates: {str(e)}", exc_info=True)
        raise ValueError(f"Error getting raw data dates: {e}")

def get_config() -> Dict[str, Dict[str, Any]]:
    """Get configuration for Oura ETL pipeline"""
    # Get GCP config from Airflow variables
    gcp_config = Variable.get("oura_gcp_config", deserialize_json=True)
    
    # Get API token from environment
    api_token = os.environ.get('OURA_API_TOKEN')
    if not api_token:
        raise ValueError("OURA_API_TOKEN environment variable not set")
    
    # Build API config using DATA_TYPES from constants
    api_config = {
        "base_url": "https://api.ouraring.com/v2",
        "token": api_token,
        "endpoints": {
            data_type: config.endpoint 
            for data_type, config in DATA_TYPES.items()
        }
    }
    
    return {"api": api_config, "gcp": gcp_config}
