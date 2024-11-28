from datetime import datetime
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
        self.raw_data_path = f"{self.config.bucket_name}/raw/oura"
    
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
                    raw_data = self.loader.get_raw_data(data_type, date)
                    if raw_data:
                        raw_data_by_date[date] = raw_data
                
                if raw_data_by_date:
                    # Transform data for this type
                    transformed_data = self.transformer.transform_data({data_type: raw_data_by_date})
                    if data_type in transformed_data and not transformed_data[data_type].empty:
                        # Load to BigQuery
                        self.loader.load_to_bigquery(
                            transformed_data[data_type], 
                            f"oura_{data_type}"
                        )
                        logger.info(f"Loaded {len(transformed_data[data_type])} records for {data_type}")
            
            logger.info("Transformation completed successfully")
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def run(self, start_date: datetime, end_date: datetime, existing_dates: Dict[str, set] | None = None) -> None:
        """
        Run the complete ETL pipeline
        
        Args:
            start_date: Start date for data extraction
            end_date: End date for data extraction
            existing_dates: Dictionary of existing dates per data type
        """
        logger.info(
            f"Starting Oura pipeline run: start_date={start_date}, "
            f"end_date={end_date}, existing_dates_count={sum(len(d) for d in existing_dates.values() if d)}"
        )
        try:
            # Extract
            logger.info("Starting data extraction")
            raw_data = self.extractor.extract_data(start_date, end_date)
            
            # Transform
            logger.info("Transforming data")
            transformed_data = self.transformer.transform_data(raw_data)
            
            # Filter out existing dates
            if existing_dates:
                for data_type, df in transformed_data.items():
                    if not df.empty and 'date' in df.columns and data_type in existing_dates:
                        new_df = df[~df['date'].isin(existing_dates[data_type])]
                        transformed_data[data_type] = new_df
                        logger.info(f"Filtered {len(df) - len(new_df)} existing records from {data_type}")
            
            # Save raw data to GCS and load to BigQuery only if we have new data
            for data_type, df in transformed_data.items():
                if len(df) > 0:
                    self.loader.save_to_gcs(raw_data[data_type], data_type, start_date)
                    self.loader.load_to_bigquery(df, f"oura_{data_type}")
                    logger.info(f"Loaded {len(df)} new records to {data_type}")
                else:
                    logger.info(f"No new data to load for {data_type}")
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
