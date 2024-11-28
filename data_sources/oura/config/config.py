from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import yaml
import os

@dataclass
class OuraConfig:
    """Configuration class for Oura Ring ETL pipeline"""
    api_base_url: str
    api_token: str
    bucket_name: str
    dataset_id: str
    project_id: str
    location: str
    raw_data_path: str
    processed_data_path: str
    endpoints: Dict[str, str]

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'OuraConfig':
        """Create config from dictionary"""
        try:
            return cls(
                api_base_url=config['api']['base_url'],
                api_token=config['api']['token'],
                bucket_name=config['gcp']['bucket_name'],
                dataset_id=config['gcp']['dataset_id'],
                project_id=config['gcp']['project_id'],
                location=config['gcp']['location'],
                raw_data_path=config['gcp']['raw_data_path'],
                processed_data_path=config['gcp']['processed_data_path'],
                endpoints=config['api']['endpoints']
            )
        except KeyError as e:
            raise ValueError(f"Missing required configuration field: {e}")

    @classmethod
    def from_yaml(cls, config_path: Path) -> 'OuraConfig':
        """Create config from YAML file"""
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)
            
        # Replace environment variables
        if '${OURA_API_TOKEN}' in config_dict['api']['token']:
            config_dict['api']['token'] = os.environ.get('OURA_API_TOKEN')
            if not config_dict['api']['token']:
                raise ValueError("OURA_API_TOKEN environment variable not set")
                
        return cls.from_dict(config_dict)
