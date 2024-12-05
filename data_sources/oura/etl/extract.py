import requests
from datetime import date, datetime
from typing import Dict, Any, Union
import logging
from data_sources.oura.config.config import OuraConfig
from pathlib import Path
from data_sources.oura.config.constants import DATA_TYPES, DataCategory

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
    
    def extract_data(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Extract all data types from Oura API"""
        data = {}
        
        for data_type, config in DATA_TYPES.items():
            logger.info(f"Extracting {data_type} data")
            
            if data_type not in self.config.endpoints:
                logger.warning(f"Endpoint not configured for {data_type}, skipping")
                continue

            try:
                if config.category == DataCategory.SPECIAL and config.special_params.get('uses_datetime'):
                    # Handle datetime-based endpoints (like heartrate)
                    start_datetime = datetime.combine(start_date, datetime.min.time())
                    end_datetime = datetime.combine(end_date, datetime.max.time())
                    data[data_type] = self._make_datetime_request(
                        self.config.endpoints[data_type],
                        start_datetime,
                        end_datetime
                    )
                elif config.category in [DataCategory.DAILY, DataCategory.DETAILED]:
                    data[data_type] = self._make_request(
                        self.config.endpoints[data_type],
                        start_date,
                        end_date
                    )
                else:
                    raise ValueError(f"Unhandled data category for {data_type}")
                    
            except Exception as e:
                logger.error(f"Error extracting {data_type} data: {e}")
                raise
            
        return data
