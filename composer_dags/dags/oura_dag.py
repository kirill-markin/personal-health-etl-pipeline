from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_sources.oura.pipelines.dataflow_pipeline import OuraPipeline
import logging
import os
import json
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
from data_sources.oura.utils.common_utils import get_raw_data_dates, get_dates_to_extract, get_dates_for_transform
from typing import Dict, Any
from pathlib import Path
logger = logging.getLogger(__name__)

# Get configurations from Airflow Variables
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

def run_extract_pipeline(**context) -> None:
    """Extract data from Oura API"""
    logger.info("Starting extract pipeline")
    config = get_config()
    pipeline = OuraPipeline(config)
    
    # Get existing dates from raw data only
    raw_dates = get_raw_data_dates(Path(pipeline.raw_data_path_str))
    
    # Log existing data stats
    for data_type, dates in raw_dates.items():
        logger.info(f"Found {len(dates)} existing {data_type} records")
        if dates:
            logger.info(f"Existing {data_type} date range: {min(dates)} to {max(dates)}")
    
    # Calculate date range based on raw data dates only
    start_date, end_date = get_dates_to_extract(raw_dates)
    
    if start_date > end_date:
        logger.info(f"No new data to extract: start_date ({start_date}) > end_date ({end_date})")
        return
            
    logger.info(f"Starting extraction for date range: {start_date} to {end_date}")
    logger.info(f"Will extract {(end_date - start_date).days + 1} days of data")
    
    pipeline.run(start_date, end_date)
    logger.info("Extract pipeline completed successfully")

def run_transform_pipeline(**context) -> None:
    """Transform and load data to BigQuery"""
    try:
        logger.info("Starting transform pipeline")
        config = get_config()
        pipeline = OuraPipeline(config)
        
        # Get dates from raw data and BigQuery
        raw_dates = get_raw_data_dates(Path(pipeline.raw_data_path_str))
        logger.info("Raw data statistics:")
        for data_type, dates in raw_dates.items():
            logger.info(f"- {data_type}: {len(dates)} records")
            if dates:
                logger.info(f"  Date range: {min(dates)} to {max(dates)}")
        
        bq_dates = {
            'activity': pipeline.loader.get_existing_dates('oura_activity'),
            'sleep': pipeline.loader.get_existing_dates('oura_sleep'),
            'readiness': pipeline.loader.get_existing_dates('oura_readiness')
        }
        
        logger.info("BigQuery data statistics:")
        for data_type, dates in bq_dates.items():
            logger.info(f"- {data_type}: {len(dates)} records")
            if dates:
                logger.info(f"  Date range: {min(dates)} to {max(dates)}")
        
        # Find dates to transform
        dates_to_transform = get_dates_for_transform(raw_dates, bq_dates)
        
        if any(dates for dates in dates_to_transform.values()):
            logger.info("Dates requiring transformation:")
            for data_type, dates in dates_to_transform.items():
                if dates:
                    logger.info(f"- {data_type}: {len(dates)} dates")
                    logger.info(f"  Range: {min(dates)} to {max(dates)}")
                    logger.info(f"  Specific dates: {sorted(dates)}")
            
            logger.info("Starting data transformation")
            pipeline.transform(dates_to_transform)
            logger.info("Transform pipeline completed successfully")
        else:
            logger.info("No new data to transform - all raw data is already in BigQuery")
            
    except Exception as e:
        logger.error(f"Transform pipeline failed with error: {str(e)}")
        raise

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=run_extract_pipeline,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_load_data',
    python_callable=run_transform_pipeline,
    dag=dag,
)

extract_task >> transform_task
