from typing import Final

# Data range configuration
HISTORICAL_DAYS: Final[int] = 365  # 1 year of historical data

# Data types
DATA_TYPES: Final[tuple[str, ...]] = ('activity', 'sleep', 'readiness')

# BigQuery table prefixes
BQ_TABLE_PREFIX: Final[str] = 'oura_'

# GCS path templates
RAW_DATA_PATH_TEMPLATE: Final[str] = 'raw/oura/{data_type}/{date}'
PROCESSED_DATA_PATH_TEMPLATE: Final[str] = 'processed/oura/{data_type}/{date}'
