from datetime import datetime
import logging
from data_sources.oura.etl.extract import OuraExtractor
from data_sources.oura.etl.transform import OuraTransformer
from data_sources.oura.etl.load import OuraLoader
from typing import Union, Dict, Any

logger = logging.getLogger(__name__)

class OuraPipeline:
    def __init__(self, config: Union[str, Dict[str, Any]]):
        self.config = config
        self.extractor = OuraExtractor(config)
        self.transformer = OuraTransformer()
        self.loader = OuraLoader(config)
    
    def run(self, start_date: datetime, end_date: datetime, existing_dates: Dict[str, set] = None):
        """Run the complete ETL pipeline"""
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
                    if data_type in existing_dates:
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
