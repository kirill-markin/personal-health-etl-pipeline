from google.cloud import storage, bigquery
from google.api_core import exceptions
import pandas as pd
from typing import Dict, List, Union, Any
import logging
import json
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)

class OuraLoader:
    def __init__(self, config: Union[str, Dict[str, Any]]):
        if isinstance(config, str):
            self.config = self._load_config(config)
        else:
            self.config = config
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self._verify_bucket_exists()
        self._verify_dataset_exists()
        
    def _load_config(self, config_path: str):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _verify_bucket_exists(self):
        """Verify the GCS bucket exists"""
        bucket_name = self.config['gcp']['bucket_name']
        try:
            self.storage_client.get_bucket(bucket_name)
            logger.info(f"Bucket {bucket_name} exists")
        except Exception:
            logger.error(f"Bucket {bucket_name} does not exist")
            raise
    
    def _verify_dataset_exists(self):
        """Verify the BigQuery dataset exists"""
        project_id = self.config['gcp']['project_id']
        dataset_id = self.config['gcp']['dataset_id']
        dataset_ref = f"{project_id}.{dataset_id}"
        
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} exists")
        except exceptions.NotFound:
            logger.error(f"Dataset {dataset_ref} does not exist")
            raise
    
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
        schema_path = f"schemas/oura/{table_name}.json"
        try:
            with open(schema_path, 'r') as f:
                schema_dict = json.load(f)
                return [
                    bigquery.SchemaField(
                        field['name'],
                        field['type'],
                        mode=field['mode']
                    )
                    for field in schema_dict['fields']
                ]
        except FileNotFoundError:
            logger.warning(f"Schema file not found for {table_name}")
            return []

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

    def get_existing_dates(self, table_name: str) -> set:
        """Get set of dates that already exist in BigQuery table"""
        query = f"""
        SELECT DISTINCT date
        FROM `{self.config['gcp']['project_id']}.{self.config['gcp']['dataset_id']}.{table_name}`
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            if not df.empty:
                # Convert the date column to datetime if it's not already
                df['date'] = pd.to_datetime(df['date'])
                return set(df['date'].dt.date)
            return set()
        except Exception as e:
            logger.warning(f"Error getting existing dates: {e}")
            return set()
