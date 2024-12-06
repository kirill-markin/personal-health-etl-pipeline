from typing import Dict, Any, Union, Optional, List
from datetime import datetime, timedelta, date
import logging
from pathlib import Path
import requests

from ..config.config import OuraConfig
from ..config.constants import DATA_TYPES, DataCategory, HISTORICAL_DAYS
from ..utils.common_utils import get_raw_data_dates, get_config

from .load import OuraLoader

logger = logging.getLogger(__name__)

class OuraExtractor:
    def __init__(self, config: Union[str, Dict[str, Any], OuraConfig]):
        if isinstance(config, OuraConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = OuraConfig.from_dict(config)
        else:
            self.config = OuraConfig.from_yaml(Path(config))
        
    def _make_request(self, endpoint: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """Make request to date-based endpoint"""
        if start_date > end_date:
            logger.warning(f"Start date {start_date} is after end date {end_date}, skipping request")
            return {"data": []}
        
        headers = {"Authorization": f"Bearer {self.config.api_token}"}
        url = f"{self.config.api_base_url}{endpoint}"
        
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Oura API: {e}")
            raise
    
    def _make_datetime_request(self, endpoint: str, start_datetime: datetime, end_datetime: datetime) -> Dict[str, Any]:
        """
        Make request to endpoints that use datetime range parameters
        
        Args:
            endpoint: API endpoint path
            start_datetime: Start datetime for the request
            end_datetime: End datetime for the request
            
        Returns:
            Dict[str, Any]: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        headers = {"Authorization": f"Bearer {self.config.api_token}"}
        url = f"{self.config.api_base_url}{endpoint}"
        
        # Format datetime strings according to Oura API requirements (RFC 3339 format)
        params = {
            "start_datetime": start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_datetime": end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching datetime-based data from Oura API: {e}")
            raise
    
    def extract_data_type(self, data_type: str, start_date: date, end_date: date, 
                         chunk_size: Optional[timedelta] = None) -> Dict[str, Any]:
        """Extract data for a specific data type with optional chunking"""
        if start_date > end_date:
            logger.info(f"No new {data_type} data to extract: already up to date")
            return {"data": []}

        config = DATA_TYPES.get(data_type)
        if not config:
            raise ValueError(f"Unknown data type: {data_type}")
        
        if data_type not in self.config.endpoints:
            logger.warning(f"Endpoint not configured for {data_type}, skipping")
            return {"data": []}

        # Handle chunked extraction (e.g., for heartrate)
        if chunk_size:
            return self._extract_chunked_data(data_type, start_date, end_date, chunk_size)

        try:
            if (config.category == DataCategory.SPECIAL and 
                config.special_params and 
                config.special_params.get('uses_datetime')):
                # Handle datetime-based endpoints
                start_datetime = datetime.combine(start_date, datetime.min.time())
                end_datetime = datetime.combine(end_date, datetime.max.time())
                return self._make_datetime_request(
                    self.config.endpoints[data_type],
                    start_datetime,
                    end_datetime
                )
            elif config.category in [DataCategory.DAILY, DataCategory.DETAILED]:
                return self._make_request(
                    self.config.endpoints[data_type],
                    start_date,
                    end_date
                )
            else:
                raise ValueError(f"Unhandled data category for {data_type}")
        except Exception as e:
            logger.error(f"Error extracting {data_type} data: {e}")
            raise

    def _extract_chunked_data(self, data_type: str, start_date: date, 
                            end_date: date, chunk_size: timedelta) -> Dict[str, Any]:
        """Extract data in chunks and combine results"""
        all_data: List[Dict[str, Any]] = []
        current_start = start_date

        while current_start <= end_date:
            chunk_end = min(current_start + chunk_size, end_date)
            logger.info(f"Extracting {data_type} chunk: {current_start} to {chunk_end}")

            chunk_data = self.extract_data_type(data_type, current_start, chunk_end)
            if chunk_data.get('data'):
                all_data.extend(chunk_data['data'])

            current_start = chunk_end + timedelta(days=1)

        return {"data": all_data}

def run_extract_pipeline(**context) -> None:
    """Extract data from Oura API starting from the latest existing date"""
    logger.info("Starting extract pipeline")
    config = get_config()
    oura_config = OuraConfig.from_dict(config)
    extractor = OuraExtractor(oura_config)
    loader = OuraLoader(oura_config)
    
    raw_data_path: str = f"{config['gcp']['bucket_name']}/raw/oura"
    raw_dates: Dict[str, set[date]] = get_raw_data_dates(Path(raw_data_path))
    logger.info(f"Detected raw dates: {raw_dates}")
    
    end_date: date = datetime.now().date() - timedelta(days=1)  # Yesterday
    
    try:
        for data_type in DATA_TYPES.keys():
            type_dates: set[date] = raw_dates.get(data_type, set())
            logger.info(f"Detected dates for {data_type}: {sorted(type_dates) if type_dates else 'None'}")
            
            # Determine start date based on existing data
            start_date: date
            if type_dates:
                start_date = max(type_dates) + timedelta(days=1)
                logger.info(f"Found existing {data_type} data up to {max(type_dates)}, starting from {start_date}")
            else:
                start_date = end_date - timedelta(days=HISTORICAL_DAYS)
                logger.info(f"No existing {data_type} data found, starting from {start_date} — {HISTORICAL_DAYS} days ago")

            if start_date > end_date:
                logger.info(f"No new {data_type} data to extract: already up to date")
                continue
                
            # Extract data with appropriate chunking for heartrate
            chunk_size: Optional[timedelta] = timedelta(days=7) if data_type == 'heartrate' else None
            raw_data: Dict[str, List[Any]] = extractor.extract_data_type(data_type, start_date, end_date, chunk_size)
            
            # Always save data, even if empty, to mark the date as processed
            loader.save_to_gcs(
                raw_data if raw_data.get('data') else {"data": []}, 
                data_type, 
                start_date, 
                end_date
            )
            logger.info(
                f"Saved {len(raw_data.get('data', []))} records for {data_type} "
                f"from {start_date} to {end_date}"
            )
        
        logger.info("Extract pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Extract pipeline failed: {e}")
        raise

__all__ = ['run_extract_pipeline', 'OuraExtractor']
