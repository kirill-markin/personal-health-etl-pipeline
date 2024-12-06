from typing import Dict, Any, Optional
import pandas as pd
from datetime import date, timedelta
import logging
import sys
import os
from pathlib import Path

from ..utils.common_utils import get_raw_data_dates, get_config
from ..config.constants import DATA_TYPES, BQ_TABLE_PREFIX, DataCategory
from ..config.config import OuraConfig

from .load import OuraLoader

logger = logging.getLogger(__name__)

class OuraTransformer:
    def _convert_to_date(self, date_str: str) -> Optional[date]:
        """Convert string date to date object"""
        try:
            return pd.to_datetime(date_str).date()
        except Exception as e:
            logger.warning(f"Error converting date {date_str}: {e}")
            return None

    def transform_data(self, raw_data: Dict[str, Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        Transform all data types based on their category
        
        Args:
            raw_data: Dictionary containing raw data for each data type
            Example: {'daily_readiness': {'data': [...], 'is_processed': True}}
        
        Returns:
            Dictionary mapping data types to transformed DataFrames
        """
        
        transformed_data: Dict[str, pd.DataFrame] = {}
        
        try:
            # First pass: Process all daily and detailed data
            for data_type, data in raw_data.items():
                if data_type not in DATA_TYPES:
                    logger.warning(f"Unknown data type: {data_type}")
                    continue
                    
                config = DATA_TYPES[data_type]
                logger.info(f"Processing {data_type} data (category: {config.category})")
                
                if config.category in [DataCategory.DAILY, DataCategory.DETAILED]:
                    records = []
                    
                    for record in data.get('data', []):
                        if not all(key in record for key in ['id', 'day']):
                            logger.warning(f"Missing required fields in record: {record}")
                            continue
                            
                        day = self._convert_to_date(record.get('day'))
                        if day is None:
                            logger.warning(f"Invalid day in record: {record}")
                            continue
                        
                        # Create base record with common fields
                        transformed_record = {
                            'id': record.get('id'),
                            'day': day,
                            'data_type': data_type
                        }
                        
                        # Add all other fields with data_type prefix
                        for key, value in record.items():
                            if key not in ['id', 'day']:
                                field_name = f"{data_type}_{key}"
                                transformed_record[field_name] = value
                        
                        records.append(transformed_record)
                    
                    if records:
                        df = pd.DataFrame(records)
                        
                        # FIXME: Add type conversion for 
                        # - time columns (float)
                        # - date columns (datetime)
                        # - float columns (float)
                        # - int columns (int)
                        # - bool columns (bool)
                        # - timestamp columns (datetime)

                        transformed_data[data_type] = df
                        logger.info(f"Transformed {len(records)} records for {data_type}")
                        
                elif config.category == DataCategory.SPECIAL:
                    logger.info(f"Special data type {data_type} will be processed separately")
                    # TODO: Implement special data processing
                    transformed_data[data_type] = pd.DataFrame()
            
            # Second pass: Join all daily and detailed data
            daily_dfs = []
            for data_type, df in transformed_data.items():
                if not df.empty and DATA_TYPES[data_type].category in [DataCategory.DAILY, DataCategory.DETAILED]:
                    daily_dfs.append(df)
            
            if daily_dfs:
                # Merge all DataFrames on day
                final_df = daily_dfs[0]
                for df in daily_dfs[1:]:
                    final_df = pd.merge(final_df, df, on='day', how='outer', suffixes=('', '_right'))
                
                transformed_data['combined_daily'] = final_df
                logger.info(f"Created combined daily data with shape: {final_df.shape}")
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error in transform_data: {str(e)}", exc_info=True)
            raise

def run_transform_pipeline(**context) -> None:
    """Transform and load new data to BigQuery"""
    try:

        logger.info("Starting transform pipeline")
        config = get_config()
        oura_config = OuraConfig.from_dict(config)
        transformer = OuraTransformer()
        loader = OuraLoader(oura_config)
        
        
        # Get raw data path
        raw_data_path = f"{config['gcp']['bucket_name']}/raw/oura"
        raw_dates = get_raw_data_dates(Path(raw_data_path))
        
        # Calculate end date (yesterday)
        end_date = date.today() - timedelta(days=1)
        
        # Get latest dates from BigQuery for daily and detailed data types
        bq_dates = {}
        for data_type, config in DATA_TYPES.items():
            if config.category in [DataCategory.DAILY, DataCategory.DETAILED]:
                table_name = f"{BQ_TABLE_PREFIX}{data_type}"
                bq_dates[data_type] = loader.get_existing_dates(table_name)
                
                if bq_dates[data_type]:
                    logger.info(
                        f"Found {len(bq_dates[data_type])} existing {data_type} records "
                        f"in BigQuery: {min(bq_dates[data_type])} to {max(bq_dates[data_type])}"
                    )
        
        # Process each data type that needs updating
        raw_data_to_transform: Dict[str, Dict[str, Any]] = {}
        
        for data_type, config in DATA_TYPES.items():
            if config.category == DataCategory.SPECIAL:
                logger.info(f"Skipping special data type: {data_type}")
                continue
                
            if not raw_dates.get(data_type):
                logger.info(f"No raw data found for {data_type}")
                raise ValueError(f"No raw data found for {data_type}")
                
            # Get latest date for this specific data type
            latest_date = max(bq_dates.get(data_type, set()) or {date.min})
            
            # Get dates after the latest BigQuery date
            new_dates = {d for d in raw_dates[data_type] if latest_date < d <= end_date}
            logger.info(f"Found {len(new_dates)} new dates for {data_type}")
            
            if not new_dates:
                logger.info(f"No new {data_type} data to transform")
                continue
            
            start_date = min(new_dates)
            logger.info(f"Processing {data_type} data from {start_date} to {end_date}")
            
            # Get raw data for the entire period
            raw_data = loader.get_raw_data(data_type, start_date, end_date + timedelta(days=1))
            if raw_data and raw_data.get('data'):
                logger.info(f"Got {len(raw_data['data'])} raw records for {data_type}")
                raw_data_to_transform[data_type] = raw_data
            else:
                logger.warning(f"No raw data retrieved for {data_type}")
        
        if raw_data_to_transform:
            # Transform all collected data
            transformed_data = transformer.transform_data(raw_data_to_transform)
            
            # Load transformed data to BigQuery
            for data_type, df in transformed_data.items():
                if df.empty:
                    logger.warning(f"No transformed data available for {data_type}")
                    continue
                    
                table_name = f"{BQ_TABLE_PREFIX}{data_type}"
                logger.info(f"Loading {len(df)} records for {data_type} to {table_name}")
                loader.load_to_bigquery(df, table_name)
                logger.info(f"Successfully loaded {len(df)} records for {data_type}")
        
        logger.info("Transform pipeline completed successfully")
            
    except Exception as e:
        logger.error(f"Transform pipeline failed: {e}", exc_info=True)
        raise

__all__ = ['run_transform_pipeline', 'OuraTransformer']
