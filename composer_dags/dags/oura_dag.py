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

logger = logging.getLogger(__name__)

def get_config() -> Dict[str, Dict[str, Any]]:
    """
    Retrieve configuration from Secret Manager and environment variables.
    
    Returns:
        Dict containing API and GCP configurations
    
    Raises:
        ValueError: If required environment variables are missing
        Exception: If Secret Manager access fails
    """
    try:
        project_id = os.environ.get('GCP_PROJECT_ID') or os.environ.get('AIRFLOW_VAR_PROJECT_ID')
        if not project_id:
            raise ValueError("Neither GCP_PROJECT_ID nor AIRFLOW_VAR_PROJECT_ID environment variable is set")
            
        sm_hook = GoogleCloudSecretManagerHook()
        secret_response = sm_hook.access_secret(
            secret_id="oura_gcp_config",
            project_id=project_id
        )
        
        gcp_config = json.loads(secret_response.payload.data.decode('UTF-8'))
        
        api_token = os.environ.get('OURA_API_TOKEN') or os.environ.get('AIRFLOW_VAR_OURA_API_TOKEN')
        if not api_token:
            raise ValueError("Neither OURA_API_TOKEN nor AIRFLOW_VAR_OURA_API_TOKEN environment variable is set")

        api_config = {
            "base_url": "https://api.ouraring.com/v2",
            "token": api_token,
            "endpoints": {
                "sleep": "/usercollection/sleep",
                "activity": "/usercollection/daily_activity",
                "readiness": "/usercollection/daily_readiness",
                # FIXME: add more tables
                # "personal_info": "/usercollection/personal_info",
                # "sessions": "/usercollection/session",
                # "tags": "/usercollection/tag",
                # "workouts": "/usercollection/workout",
                # "heart_rate": "/usercollection/heartrate",
                # "daily_spo2": "/usercollection/daily_spo2",
                # "stress": "/usercollection/daily_stress"
            }
        }
        return {"api": api_config, "gcp": gcp_config}
        
    except Exception as e:
        logger.error(f"Failed to get config: {e}")
        raise

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
    )

def run_extract_pipeline(**context) -> None:
    """Extract data from Oura API"""
    logger.info("Starting extract pipeline")
    config = get_config()
    extractor, _, loader = get_etl_components(config)
    
    # Get existing dates from raw data
    raw_data_path = f"{config['gcp']['bucket_name']}/raw/oura"
    raw_dates = get_raw_data_dates(Path(raw_data_path))
    logger.info(f"Raw dates: {raw_dates}")
    
    # Log existing data stats
    for data_type, dates in raw_dates.items():
        logger.info(f"Found {len(dates)} existing {data_type} records")
        if dates:
            logger.info(f"Existing {data_type} date range: {min(dates)} to {max(dates)}")
    
    # Calculate date range
    start_date, end_date = get_dates_to_extract(raw_dates)
    
    if start_date > end_date:
        logger.info(f"No new data to extract: start_date ({start_date}) > end_date ({end_date})")
        return
            
    logger.info(f"Starting extraction for date range: {start_date} to {end_date}")
    logger.info(f"Will extract {(end_date - start_date).days + 1} days of data")
    
    try:
        # Extract data for each type independently
        for data_type in raw_dates.keys():
            logger.info(f"Extracting {data_type} data")
            # Get latest date for this type
            type_dates = raw_dates[data_type]
            type_start_date = max(type_dates) + timedelta(days=1) if type_dates else start_date
            
            if type_start_date > end_date:
                logger.info(f"No new {data_type} data to extract: start_date ({type_start_date}) > end_date ({end_date})")
                continue
                
            logger.info(f"Extracting {data_type} from {type_start_date} to {end_date}")
            
            # Extract data for this type
            endpoint = extractor.config.endpoints[data_type]
            raw_data = extractor._make_request(endpoint, type_start_date, end_date)
            
            # Save raw data if not empty
            if raw_data.get('data'):
                loader.save_to_gcs(raw_data, data_type, type_start_date, end_date)
                logger.info(f"Saved raw data for {data_type}")
            else:
                logger.info(f"No new data received for {data_type}")
                
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
