import sys
import os
# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from data_sources.oura.pipelines.dataflow_pipeline import OuraPipeline

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv('.config/development/.env')

def main():
    try:
        # Initialize pipeline with config
        logger.info("Initializing pipeline...")
        pipeline = OuraPipeline("data_sources/oura/configs/oura_config.yaml")
        
        # Set date range (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        logger.info(f"Running pipeline for date range: {start_date} to {end_date}")
        
        # Run pipeline
        pipeline.run(start_date, end_date)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
