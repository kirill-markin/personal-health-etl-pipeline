import sys
import os
# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from data_sources.oura.pipelines.dataflow_pipeline import OuraPipeline
from data_sources.oura.utils.common_utils import get_date_range, get_existing_dates

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv('.config/development/.env')

# def main():
#     try:
#         # Initialize pipeline with config
#         logger.info("Initializing pipeline...")
#         pipeline = OuraPipeline("data_sources/oura/configs/oura_config.yaml")
        
#         # Get existing dates and calculate date range
#         existing_dates = get_existing_dates(pipeline)
#         start_date, end_date = get_date_range(existing_dates)
        
#         # Only proceed if we have dates to fetch
#         if start_date <= end_date:
#             logger.info(f"Running pipeline for date range: {start_date} to {end_date}")
#             logger.info(f"Found existing dates: {len(existing_dates['activity'])} activity, "
#                        f"{len(existing_dates['sleep'])} sleep, "
#                        f"{len(existing_dates['readiness'])} readiness records")
            
#             pipeline.run(start_date, end_date, existing_dates)
#         else:
#             logger.info("No new data to fetch - we're up to date")
            
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")

# if __name__ == "__main__":
#     main()
