import os
import requests
from datetime import date
from typing import Dict, Any, Union
import yaml
import logging
from data_sources.oura.config.config import OuraConfig
from pathlib import Path

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
        headers = {"Authorization": f"Bearer {self.config.api_token}"}
        url = f"{self.config.api_base_url}{endpoint}"
        
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d")
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Oura API: {e}")
            raise
    
    def extract_data(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Extract all data types from Oura API"""
        data = {}
        
        for data_type, endpoint in self.config.endpoints.items():
            logger.info(f"Extracting {data_type} data")
            data[data_type] = self._make_request(endpoint, start_date, end_date)
            
        return data
