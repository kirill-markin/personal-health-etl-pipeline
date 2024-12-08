from typing import Dict, Any, Optional
import pandas as pd
from datetime import date, timedelta
import logging
from pathlib import Path
import json

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
            raise ValueError(f"Error converting date {date_str}: {e}")

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
        
        def flatten_dict(prefix: str, d: Dict[str, Any]) -> Dict[str, Any]:
            """
            Recursively flatten nested dictionaries and arrays with prefix
            """
            items: Dict[str, Any] = {}
            for key, value in d.items():
                new_key = f"{prefix}__{key}" if prefix else key
                if isinstance(value, dict):
                    items.update(flatten_dict(new_key, value))
                elif isinstance(value, list):
                    # Convert arrays to JSON strings
                    items[new_key] = json.dumps(value)
                else:
                    items[new_key] = value
            return items
        
        try:
            # First pass: Process all daily and detailed data
            for data_type, data in raw_data.items():
                if data_type not in DATA_TYPES:
                    logger.warning(f"Unknown data type: {data_type}")
                    continue
                    
                config = DATA_TYPES[data_type]
                logger.info(f"Processing {data_type} data (category: {config.category})")
                
                if config.category in [DataCategory.DAILY]:
                    records = []
                    
                    for record in data.get('data', []):
                        if not all(key in record for key in ['day']):
                            logger.warning(f"Missing required fields in record: {record}")
                            continue
                            
                        day = self._convert_to_date(record.get('day'))
                        if day is None:
                            logger.warning(f"Invalid day in record: {record}")
                            continue
                        
                        # Create base record with day
                        transformed_record = {'day': day}
                        
                        # Flatten all other fields with data_type prefix
                        for key, value in record.items():
                            if key != 'day':
                                if isinstance(value, dict):
                                    # Recursively flatten nested dictionaries
                                    flattened = flatten_dict(f"{data_type}__{key}", value)
                                    transformed_record.update(flattened)
                                else:
                                    field_name = f"{data_type}__{key}"
                                    transformed_record[field_name] = value
                        
                        records.append(transformed_record)
                    
                    if records:
                        df = pd.DataFrame(records)
                        
                        # Get schema for this specific table to identify timestamp columns
                        schema_path = Path("/home/airflow/gcs/data/schemas/oura/oura_day.json")
                        try:
                            with open(schema_path, 'r') as f:
                                schema = json.load(f)
                                timestamp_columns = [
                                    field['name'] 
                                    for field in schema 
                                    if field.get('type') == 'TIMESTAMP'
                                ]
                                
                                # Convert timestamp columns
                                for col in timestamp_columns:
                                    if col in df.columns:
                                        df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce')
                                        logger.info(f"Converted {col} to timestamp type")

                            transformed_data[data_type] = df
                            logger.info(f"Transformed {len(records)} records for {data_type}")
                            
                        except Exception as e:
                            logger.error(f"Error processing schema for timestamp conversion: {e}")
                            raise
                    
                elif config.category == DataCategory.SPECIAL:
                    logger.info(f"Special data type {data_type} will be processed separately")
                    # TODO: Implement special data processing
                    # transformed_data[data_type] = pd.DataFrame()

                elif config.category == DataCategory.DETAILED:
                    logger.info(f"Detailed data type {data_type} will be processed separately")
                    # TODO: Implement detailed data processing
                    # transformed_data[data_type] = pd.DataFrame()
            
            # Second pass: Join all daily and detailed data
            daily_dfs = []
            for data_type, df in transformed_data.items():
                if not df.empty and DATA_TYPES[data_type].category in [DataCategory.DAILY, DataCategory.DETAILED]:
                    daily_dfs.append(df)
            
            if daily_dfs:
                # Merge all DataFrames on day
                final_df = daily_dfs[0]
                for df in daily_dfs[1:]:
                    final_df = pd.merge(final_df, df, on='day', how='outer')
                
                # Convert array/list columns to JSON strings before returning
                for column in final_df.columns:
                    if final_df[column].dtype == 'object':
                        # Check if the column contains lists or dicts
                        sample_value = final_df[column].iloc[0] if not final_df[column].empty else None
                        if isinstance(sample_value, (list, dict)):
                            final_df[column] = final_df[column].apply(
                                lambda x: json.dumps(x) if pd.notnull(x) else None
                            )
                
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
        
        
        # Get raw data path and dates
        raw_data_path = f"{config['gcp']['bucket_name']}/raw/oura"
        raw_dates = get_raw_data_dates(Path(raw_data_path))
        end_date = date.today() - timedelta(days=1)

        # Get latest date from the combined table
        combined_table_name = f"{BQ_TABLE_PREFIX}day"
        combined_dates = loader.get_existing_dates(combined_table_name)
        latest_combined_date = max(combined_dates or {date.min})
        logger.info(f"Latest date in BigQuery: {latest_combined_date}")

        # Get all new dates across all data types
        all_new_dates = set()
        for data_type, dates in raw_dates.items():
            if DATA_TYPES[data_type].category in [DataCategory.DAILY, DataCategory.DETAILED]:
                new_dates = {d for d in dates if latest_combined_date < d <= end_date}
                if new_dates:
                    logger.info(f"Found {len(new_dates)} new dates for {data_type}: {min(new_dates)} to {max(new_dates)}")
                all_new_dates.update(new_dates)

        if not all_new_dates:
            logger.info(f"No new dates found after {latest_combined_date}")
            return

        start_date = min(all_new_dates)
        logger.info(f"Processing all data types from {start_date} to {end_date}")

        # Collect raw data for all types in the date range
        raw_data_to_transform: Dict[str, Dict[str, Any]] = {}
        for data_type, config in DATA_TYPES.items():
            if config.category not in [DataCategory.DAILY]:
                continue

            # FIXME: move logic of error if day overlap from get_raw_data to this place

            raw_data = loader.get_raw_data(data_type, start_date, end_date + timedelta(days=1))
            if raw_data and raw_data.get('data'):
                logger.info(f"Got {len(raw_data['data'])} raw records for {data_type}")
                raw_data_to_transform[data_type] = raw_data

        if raw_data_to_transform:
            # Transform and combine all data
            transformed_data = transformer.transform_data(raw_data_to_transform)
            
            # Load only the combined daily data
            if 'combined_daily' in transformed_data:
                combined_daily = transformed_data['combined_daily']
                if not combined_daily.empty:

                    data_info = {
                        'shape': transformed_data['combined_daily'].shape,
                        'columns': transformed_data['combined_daily'].columns.tolist(),
                    }
                    logger.info(f"Transformed data 'combined_daily' structure: \n{json.dumps(data_info, indent=2)}")
                    logger.info(f"Sample data: \n{combined_daily.head(2).to_string()}")

                    logger.info(f"Loading {len(combined_daily)} combined records to {combined_table_name}")
                    loader.load_to_bigquery(combined_daily, combined_table_name)
                    logger.info("Successfully loaded combined data")
                else:
                    logger.warning("No combined data available")
            else:
                logger.warning("No combined data available")

        logger.info("Transform pipeline completed successfully")
            
    except Exception as e:
        logger.error(f"Transform pipeline failed: {e}", exc_info=True)
        raise

__all__ = ['run_transform_pipeline', 'OuraTransformer']
