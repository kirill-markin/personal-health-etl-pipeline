from datetime import date
import logging
from data_sources.oura.etl.extract import OuraExtractor
from data_sources.oura.etl.transform import OuraTransformer
from data_sources.oura.etl.load import OuraLoader
from data_sources.oura.config.config import OuraConfig
from typing import Union, Dict, Any, Final
from pathlib import Path

logger = logging.getLogger(__name__)

class OuraPipeline:
    def __init__(self, config: Union[str, Dict[str, Any], OuraConfig]) -> None:
        if isinstance(config, OuraConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = OuraConfig.from_dict(config)
        else:
            self.config = OuraConfig.from_yaml(Path(config))
            
        self.extractor = OuraExtractor(self.config)
        self.transformer = OuraTransformer()
        self.loader = OuraLoader(self.config)
        self.raw_data_path_str: str = f"{self.config.bucket_name}/raw/oura"
    
    def transform(self, dates_to_transform: Dict[str, set]) -> None:
        """
        Transform data for specific dates
        
        Args:
            dates_to_transform: Dictionary mapping data types to sets of dates to transform
        """
        logger.info(f"Starting transformation for dates: {dates_to_transform}")
        
        try:
            for data_type, dates in dates_to_transform.items():
                if not dates:
                    continue
                    
                raw_data_by_date = {}
                for date in dates:
                    # First check if data already exists in BigQuery
                    existing_data = self.loader.check_existing_data(data_type, date)
                    if existing_data:
                        logger.info(f"Data already exists for {data_type} on {date}, skipping")
                        continue
                    
                    raw_data = self.loader.get_raw_data(data_type, date, date)
                    if raw_data:
                        raw_data_by_date[date] = raw_data
                
                if raw_data_by_date:
                    transformed_data = self.transformer.transform_data({data_type: raw_data_by_date})
                    if data_type in transformed_data and not transformed_data[data_type].empty:
                        self.loader.load_to_bigquery(
                            transformed_data[data_type], 
                            f"oura_{data_type}"
                        )
                        logger.info(f"Loaded {len(transformed_data[data_type])} records for {data_type}")
            
            logger.info("Transformation completed successfully")
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def run(self, start_date: date, end_date: date) -> None:
        """
        Run the complete ETL pipeline
        
        Args:
            start_date: Start date for data extraction
            end_date: End date for data extraction
        """
        logger.info(f"Starting Oura pipeline run: start_date={start_date}, end_date={end_date}")
        
        try:
            # Extract
            logger.info("Starting data extraction")
            raw_data = self.extractor.extract_data(start_date, end_date)
            
            # Save raw data to GCS with new path format
            for data_type, data in raw_data.items():
                if data.get('data'):
                    self.loader.save_to_gcs(
                        data,
                        data_type,
                        start_date,
                        end_date
                    )
                    logger.info(f"Saved raw data for {data_type}")
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
