from datetime import datetime
import logging
from data_sources.oura.etl.extract import OuraExtractor
from data_sources.oura.etl.transform import OuraTransformer
from data_sources.oura.etl.load import OuraLoader

logger = logging.getLogger(__name__)

class OuraPipeline:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.extractor = OuraExtractor(config_path)
        self.transformer = OuraTransformer()
        self.loader = OuraLoader(config_path)
    
    def run(self, start_date: datetime, end_date: datetime):
        """Run the complete ETL pipeline"""
        try:
            # Extract
            logger.info("Starting data extraction")
            raw_data = self.extractor.extract_data(start_date, end_date)
            
            # Save raw data to GCS
            for data_type, data in raw_data.items():
                self.loader.save_to_gcs(data, data_type, start_date)
            
            # Transform
            logger.info("Transforming data")
            transformed_data = self.transformer.transform_data(raw_data)
            
            # Load to BigQuery
            logger.info("Loading data to BigQuery")
            for data_type, df in transformed_data.items():
                self.loader.load_to_bigquery(df, f"oura_{data_type}")
                
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
