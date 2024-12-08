from typing import Final, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto

class DataCategory(Enum):
    DAILY = auto()
    DETAILED = auto()
    SPECIAL = auto()

@dataclass
class DataTypeConfig:
    category: DataCategory
    endpoint: str
    special_params: Dict[str, Any] = field(default_factory=dict)

# Data range configuration
HISTORICAL_DAYS: Final[int] = 365  # 1 year of historical data

# Data type configurations
DATA_TYPES: Final[Dict[str, DataTypeConfig]] = {
    # Daily summaries
    'daily_activity': DataTypeConfig(DataCategory.DAILY, '/usercollection/daily_activity'),
    'daily_sleep': DataTypeConfig(DataCategory.DAILY, '/usercollection/daily_sleep'),
    'daily_readiness': DataTypeConfig(DataCategory.DAILY, '/usercollection/daily_readiness'),
    'daily_stress': DataTypeConfig(DataCategory.DAILY, '/usercollection/daily_stress'),
    'daily_resilience': DataTypeConfig(DataCategory.DAILY, '/usercollection/daily_resilience'),
    'daily_cardiovascular_age': DataTypeConfig(DataCategory.DAILY, '/usercollection/daily_cardiovascular_age'),
    
    # Detailed data
    'workout': DataTypeConfig(DataCategory.DETAILED, '/usercollection/workout'),
    'session': DataTypeConfig(DataCategory.DETAILED, '/usercollection/session'),
    'sleep': DataTypeConfig(DataCategory.DETAILED, '/usercollection/sleep'),
    'sleep_time': DataTypeConfig(DataCategory.DETAILED, '/usercollection/sleep_time'),
    'rest_mode_period': DataTypeConfig(DataCategory.DETAILED, '/usercollection/rest_mode_period'),
    'enhanced_tag': DataTypeConfig(DataCategory.DETAILED, '/usercollection/enhanced_tag'),
    'vO2_max': DataTypeConfig(DataCategory.DETAILED, '/usercollection/vO2_max'),
    
    # Special data (with different parameters)
    'heartrate': DataTypeConfig(
        DataCategory.SPECIAL, 
        '/usercollection/heartrate',
        special_params={'uses_datetime': True}
    ),
    'daily_spo2': DataTypeConfig(
        DataCategory.SPECIAL, 
        '/usercollection/daily_spo2',
        special_params={'id_day': True}
    ),
}

# BigQuery table prefixes
BQ_TABLE_PREFIX: Final[str] = 'oura_'

# GCS path templates
RAW_DATA_PATH_TEMPLATE: Final[str] = 'raw/oura/{data_type}/{start_date}_{end_date}'
