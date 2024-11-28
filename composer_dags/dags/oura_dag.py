from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from data_sources.oura.pipelines.dataflow_pipeline import OuraPipeline
import logging
import os
import json
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
from data_sources.oura.utils.common_utils import get_date_range, get_existing_dates

logger = logging.getLogger(__name__)

# Get configurations from Airflow Variables
def get_config():
    try:
        # Initialize Secret Manager Hook
        sm_hook = GoogleCloudSecretManagerHook()
        
        # Get secret from Secret Manager
        secret_response = sm_hook.access_secret(
            secret_id="oura_gcp_config",
            project_id="stefans-body-etl"
        )
        
        # Access the payload from the response
        gcp_config = json.loads(secret_response.payload.data.decode('UTF-8'))
        
    except Exception as e:
        logger.error(f"Failed to get config from Secret Manager: {e}")
        raise

    if not os.environ.get('OURA_API_TOKEN'):
        logger.error("Missing required environment variable: OURA_API_TOKEN")
        raise ValueError("OURA_API_TOKEN environment variable must be set")

    api_config = {
        "base_url": "https://api.ouraring.com/v2",
        "token": os.environ.get('OURA_API_TOKEN'),
        "endpoints": {
            "sleep": "/usercollection/sleep",
            "activity": "/usercollection/daily_activity",
            "readiness": "/usercollection/daily_readiness"
        }
    }
    return {"api": api_config, "gcp": gcp_config}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
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

def run_oura_pipeline(**context):
    try:
        config = get_config()
        pipeline = OuraPipeline(config)
        
        # Get existing dates and calculate date range
        existing_dates = get_existing_dates(pipeline)
        start_date, end_date = get_date_range(existing_dates)
        
        # Only proceed if we have dates to fetch
        if start_date <= end_date:
            logger.info(f"Running pipeline for date range: {start_date} to {end_date}")
            logger.info(f"Found existing dates: {existing_dates}")
            
            pipeline.run(start_date, end_date, existing_dates)
        else:
            logger.info("No new data to fetch - we're up to date")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

extract_load_task = PythonOperator(
    task_id='extract_transform_load',
    python_callable=run_oura_pipeline,
    dag=dag,
)
