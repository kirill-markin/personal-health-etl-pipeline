from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from data_sources.oura.etl.extract import OuraExtractor
from data_sources.oura.etl.transform import OuraTransformer
from data_sources.oura.etl.load import OuraLoader
from data_sources.oura.config.config import OuraConfig
import logging
import os
import json
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
from data_sources.oura.utils.common_utils import get_raw_data_dates, get_dates_to_extract, get_dates_for_transform
from typing import Dict, Any
from pathlib import Path
from airflow.models.variable import Variable
from data_sources.oura.config.constants import DATA_TYPES
logger = logging.getLogger(__name__)

def get_config() -> Dict[str, Dict[str, Any]]:
    """Get configuration for Oura ETL pipeline"""
    # Get GCP config from Airflow variables
    gcp_config = Variable.get("oura_gcp_config", deserialize_json=True)
    
    # Get API token from environment
    api_token = os.environ.get('OURA_API_TOKEN')
    if not api_token:
        raise ValueError("OURA_API_TOKEN environment variable not set")
    
    # Build API config using DATA_TYPES from constants
    api_config = {
        "base_url": "https://api.ouraring.com/v2",
        "token": api_token,
        "endpoints": {
            data_type: config.endpoint 
            for data_type, config in DATA_TYPES.items()
        }
    }
    
    return {"api": api_config, "gcp": gcp_config}

def get_etl_components(config: Dict[str, Dict[str, Any]]) -> tuple[OuraExtractor, OuraTransformer, OuraLoader]:
    """
    Initialize ETL components from config
    
    Returns:
        Tuple of (extractor, transformer, loader)
    """
    oura_config = OuraConfig.from_dict(config)
    return (
        OuraExtractor(oura_config),
        OuraTransformer(),
        OuraLoader(oura_config)
        OuraLoader(oura_config)
    )    )

def run_extract_pipeline(**context) -> None:
    """Extract data from Oura API starting from the latest existing date"""
    logger.info("Starting extract pipeline")
    config = get_config()
    extractor, _, loader = get_etl_components(config)
    
    # Get existing dates from raw data
    raw_data_path = f"{config['gcp']['bucket_name']}/raw/oura"
    raw_dates = get_raw_data_dates(Path(raw_data_path))
    
    # Get end date (today)
    end_date = datetime.now().date()
    
    try:
        # Extract date-based endpoints
        for data_type in raw_dates.keys():
            if data_type == 'heartrate':
                continue  # Skip heartrate here, handle separately
                
            logger.info(f"Extracting {data_type} data")
            # Start from day after the latest date, or 30 days ago if no data exists
            type_dates = raw_dates[data_type]
            start_date = max(type_dates) + timedelta(days=1) if type_dates else end_date - timedelta(days=365)
            
            if start_date > end_date:
                logger.info(f"No new {data_type} data to extract: already up to date")
                continue
            
            logger.info(f"Extracting {data_type} data from {start_date} to {end_date}")
            endpoint = extractor.config.endpoints[data_type]
            raw_data = extractor._make_request(endpoint, start_date, end_date)
            
            if raw_data.get('data'):
                loader.save_to_gcs(raw_data, data_type, start_date, end_date)
                logger.info(f"Saved raw data for {data_type}")
            else:
                logger.info(f"No new data received for {data_type}")

        # Extract heartrate data in chunks of 7 days
        if 'heartrate' in extractor.config.endpoints:
            logger.info("Extracting heartrate data")
            hr_dates = raw_dates.get('heartrate', set())
            start_date = max(hr_dates) + timedelta(days=1) if hr_dates else end_date - timedelta(days=365)
            
            current_start = start_date
            chunk_size = timedelta(days=7)
            
            while current_start <= end_date:
                chunk_end = min(current_start + chunk_size, end_date)
                logger.info(f"Extracting heartrate chunk: {current_start} to {chunk_end}")
                
                start_datetime = datetime.combine(current_start, datetime.min.time())
                end_datetime = datetime.combine(chunk_end, datetime.max.time())
                
                try:
                    raw_data = extractor._make_datetime_request(
                        extractor.config.endpoints['heartrate'],
                        start_datetime,
                        end_datetime
                    )
                    
                    if raw_data.get('data'):
                        loader.save_to_gcs(raw_data, 'heartrate', current_start, chunk_end)
                        logger.info(f"Saved raw heartrate data for {current_start} to {chunk_end}")
                    else:
                        logger.info(f"No heartrate data for period {current_start} to {chunk_end}")
                        
                except Exception as e:
                    logger.error(f"Error extracting heartrate data for period {current_start} to {chunk_end}: {e}")
                    
                current_start = chunk_end + timedelta(days=1)
                
        logger.info("Extract pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Extract pipeline failed: {e}")
        raise

def run_transform_pipeline(**context) -> None:
    """Transform and load new data to BigQuery"""
    try:
        logger.info("Starting transform pipeline")
        config = get_config()
        _, transformer, loader = get_etl_components(config)
        
        # Get latest dates from BigQuery for each data type
        bq_dates = {
            'activity': loader.get_existing_dates('oura_activity'),
            'sleep': loader.get_existing_dates('oura_sleep'),
            'readiness': loader.get_existing_dates('oura_readiness')
        }
        
        # Log BigQuery dates for each type
        for data_type, dates in bq_dates.items():
            logger.info(f"Found {len(dates)} existing {data_type} records in BigQuery")
            if dates:
                logger.info(f"BigQuery {data_type} date range: {min(dates)} to {max(dates)}")
        
        # Get raw data path
        raw_data_path = f"{config['gcp']['bucket_name']}/raw/oura"
        raw_dates = get_raw_data_dates(Path(raw_data_path))
        
        # Calculate end date (yesterday)
        end_date = date.today() - timedelta(days=1)
        
        # Process each data type independently
        for data_type in ('activity', 'sleep', 'readiness'):
            if not raw_dates.get(data_type):
                logger.info(f"No raw data found for {data_type}")
                raise ValueError(f"No raw data found for {data_type}")
                
            # Get latest date for this specific data type
            latest_date_for_type = max(bq_dates[data_type]) if bq_dates[data_type] else date.min
            
            # Get dates after the latest BigQuery date for this type
            new_dates = {d for d in raw_dates[data_type] if latest_date_for_type < d <= end_date}
            logger.info(f"Found {len(new_dates)} new dates for {data_type}")
            
            if not new_dates:
                logger.info(f"No new {data_type} data to transform")
                continue
            
            start_date = min(new_dates)
            logger.info(f"Processing {data_type} data from {start_date} to {end_date}")
            
            # Get raw data for the entire period
            raw_data = loader.get_raw_data(data_type, start_date, end_date + timedelta(days=1))
            if raw_data:
                logger.info(f"Got {len(raw_data.get('data', []))} raw records for {data_type}")
                logger.info(f"Raw data structure being passed to transformer: {raw_data.keys()}")
                transformed_data = transformer.transform_data({data_type: raw_data})
                logger.info(f"Transformed data for {data_type}: {transformed_data.get(data_type, 'No data')}")
                
                if data_type in transformed_data and not transformed_data[data_type].empty:
                    logger.info(f"Loading {len(transformed_data[data_type])} records for {data_type} to BigQuery")
                    loader.load_to_bigquery(
                        transformed_data[data_type], 
                        f"oura_{data_type}"
                    )
                    logger.info(f"Successfully loaded {len(transformed_data[data_type])} records for {data_type}")
                else:
                    logger.warning(f"No transformed data available for {data_type}")
            else:
                logger.warning(f"No raw data retrieved for {data_type}")
        logger.info("Transform pipeline completed successfully")
            
    except Exception as e:
        logger.error(f"Transform pipeline failed: {e}", exc_info=True)
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'oura_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Oura Ring data',
    schedule_interval='0 4 * * *',  # Run daily at 4 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oura', 'health'],
)

extract_task = PythonOperator(
    task_id='extract_data_and_load',
    python_callable=run_extract_pipeline,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=run_transform_pipeline,
    dag=dag,
)

extract_task >> transform_task
