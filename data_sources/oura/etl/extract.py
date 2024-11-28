import os
import requests
from datetime import datetime
from typing import Dict, Any, Union
import yaml
import logging

logger = logging.getLogger(__name__)

class OuraExtractor:
    def __init__(self, config: Union[str, Dict[str, Any]]):
        if isinstance(config, str):
            self.config = self._load_config(config)
        else:
            self.config = config
        
        self.token = os.environ.get('OURA_API_TOKEN')
        if not self.token:
            raise ValueError("OURA_API_TOKEN environment variable not set")
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _make_request(self, endpoint: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.token}"}
        url = f"{self.config['api']['base_url']}{endpoint}"
        
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
    
    def extract_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Extract all data types from Oura API"""
        data = {}
        
        for data_type, endpoint in self.config['api']['endpoints'].items():
            logger.info(f"Extracting {data_type} data")
            data[data_type] = self._make_request(endpoint, start_date, end_date)
            
        return data
