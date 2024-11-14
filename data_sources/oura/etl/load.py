from google.cloud import storage, bigquery
from google.api_core import exceptions
import pandas as pd
from typing import Dict, List
import logging
import json
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)

class OuraLoader:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self._ensure_bucket_exists()
        self._ensure_dataset_exists()
        
    def _load_config(self, config_path: str):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _ensure_bucket_exists(self):
        """Create the GCS bucket if it doesn't exist"""
        bucket_name = self.config['gcp']['bucket_name']
        project_id = self.config['gcp']['project_id']
        location = self.config['gcp']['location']
        
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
        except Exception:
            logger.info(f"Creating bucket {bucket_name}")
            bucket = self.storage_client.create_bucket(
                bucket_name,
                project=project_id,
                location=location
            )
            logger.info(f"Created bucket {bucket_name}")
        
        return bucket
    
    def _ensure_dataset_exists(self):
        """Create the BigQuery dataset if it doesn't exist"""
        project_id = self.config['gcp']['project_id']
        dataset_id = self.config['gcp']['dataset_id']
        location = self.config['gcp']['location']
        
        dataset_ref = f"{project_id}.{dataset_id}"
        
        try:
            dataset = self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except exceptions.NotFound:
            logger.info(f"Creating dataset {dataset_ref}")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_ref}")
        
        return dataset
    
    def save_to_gcs(self, data: Dict, data_type: str, date: datetime) -> str:
        """Save raw data to Google Cloud Storage"""
        bucket = self.storage_client.bucket(self.config['gcp']['bucket_name'])
        
        # Format the blob path using the config template
        blob_path = self.config['gcp']['raw_data_path'].format(
            data_type=data_type,
            date=date.strftime("%Y-%m-%d")
        )
        
        blob = bucket.blob(f"{blob_path}/data.json")
        blob.upload_from_string(
            json.dumps(data),
            content_type='application/json'
        )
        
        return f"gs://{self.config['gcp']['bucket_name']}/{blob_path}"
    
    def _get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get the appropriate schema based on table name"""
        schemas = {
            'oura_sleep': [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("duration", "FLOAT"),
                bigquery.SchemaField("sleep_score", "FLOAT"),
                bigquery.SchemaField("restfulness", "FLOAT"),
                bigquery.SchemaField("hr_average", "FLOAT"),
                bigquery.SchemaField("hrv_average", "FLOAT"),
                bigquery.SchemaField("temperature_delta", "FLOAT"),
            ],
            'oura_activity': [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("calories_active", "FLOAT"),
                bigquery.SchemaField("calories_total", "FLOAT"),
                bigquery.SchemaField("steps", "INTEGER"),
                bigquery.SchemaField("daily_movement", "FLOAT"),
                bigquery.SchemaField("activity_score", "FLOAT"),
                bigquery.SchemaField("inactivity_alerts", "INTEGER"),
                bigquery.SchemaField("average_met", "FLOAT"),
            ],
            'oura_readiness': [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("score", "FLOAT"),
                bigquery.SchemaField("temperature_trend_deviation", "FLOAT"),
                bigquery.SchemaField("hrv_balance_score", "FLOAT"),
            ]
        }
        
        return schemas.get(table_name, [])

    def load_to_bigquery(self, df: pd.DataFrame, table_name: str):
        """Load transformed data to BigQuery"""
        dataset_ref = self.bq_client.dataset(self.config['gcp']['dataset_id'])
        table_ref = dataset_ref.table(table_name)
        
        # Get schema for this specific table
        schema = self._get_table_schema(table_name)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema if schema else None  # Use schema if available, otherwise auto-detect
        )
        
        try:
            job = self.bq_client.load_table_from_dataframe(
                df,
                table_ref,
                job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            logger.info(f"Loaded {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {e}")
            raise
