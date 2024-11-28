from google.cloud import storage, bigquery
from google.api_core import exceptions
import pandas as pd
from typing import Dict, List, Union, Any, Optional
import logging
import json
from datetime import datetime
import yaml
from data_sources.oura.config.config import OuraConfig
from pathlib import Path

logger = logging.getLogger(__name__)

class OuraLoader:
    def __init__(self, config: Union[str, Dict[str, Any], OuraConfig]):
        if isinstance(config, OuraConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = OuraConfig.from_dict(config)
        else:
            self.config = OuraConfig.from_yaml(Path(config))
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self._verify_bucket_exists()
        self._verify_dataset_exists()
        
    def _verify_bucket_exists(self):
        """Verify the GCS bucket exists"""
        bucket_name = self.config.bucket_name
        try:
            self.storage_client.get_bucket(bucket_name)
            logger.info(f"Bucket {bucket_name} exists")
        except Exception:
            logger.error(f"Bucket {bucket_name} does not exist")
            raise
    
    def _verify_dataset_exists(self):
        """Verify the BigQuery dataset exists"""
        project_id = self.config.project_id
        dataset_id = self.config.dataset_id
        dataset_ref = f"{project_id}.{dataset_id}"
        
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} exists")
        except exceptions.NotFound:
            logger.error(f"Dataset {dataset_ref} does not exist")
            raise
    
    def save_to_gcs(self, data: Dict, data_type: str, date: datetime) -> str:
        """Save raw data to Google Cloud Storage"""
        bucket = self.storage_client.bucket(self.config.bucket_name)
        
        # Format the blob path using the config template
        blob_path = self.config.raw_data_path.format(
            data_type=data_type,
            date=date.strftime("%Y-%m-%d")
        )
        
        blob = bucket.blob(f"{blob_path}/data.json")
        blob.upload_from_string(
            json.dumps(data),
            content_type='application/json'
        )
        
        return f"gs://{self.config.bucket_name}/{blob_path}"
    
    def _get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get the appropriate schema based on table name"""
        # In Composer environment, schemas are in /home/airflow/gcs/data/schemas/
        composer_schema_path = Path("/home/airflow/gcs/data/schemas/oura") / f"{table_name}.json"
        local_schema_path = Path(__file__).parents[3] / "schemas" / "oura" / f"{table_name}.json"
        
        # Try Composer path first, then fall back to local path
        schema_path = composer_schema_path if composer_schema_path.exists() else local_schema_path
        
        try:
            with open(schema_path, 'r') as f:
                schema_dict = json.load(f)
                if not isinstance(schema_dict, list):
                    raise ValueError(f"Invalid schema format in {schema_path}")
                return [
                    bigquery.SchemaField(
                        name=field['name'],
                        field_type=field['type'],
                        mode=field.get('mode', 'NULLABLE')
                    )
                    for field in schema_dict
                ]
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error loading schema for {table_name}: {e}")
            raise

    def load_to_bigquery(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Load transformed data to BigQuery using WRITE_APPEND disposition
        
        Args:
            df: DataFrame to load
            table_name: Target table name
        """
        dataset_ref = self.bq_client.dataset(self.config.dataset_id)
        table_ref = dataset_ref.table(table_name)
        
        # Get schema for this specific table
        schema = self._get_table_schema(table_name)
        
        # Configure job to append data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema if schema else None
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
        FROM `{self.config.project_id}.{self.config.dataset_id}.{table_name}`
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

    def get_raw_data(self, data_type: str, date: datetime.date) -> Optional[Dict[str, Any]]:
        """Get raw data from GCS for specific date and data type"""
        try:
            bucket = self.storage_client.bucket(self.config.bucket_name)
            
            blob_path = self.config.raw_data_path.format(
                data_type=data_type,
                date=date.strftime("%Y-%m-%d")
            )
            
            blob = bucket.blob(f"{blob_path}/data.json")
            
            if not blob.exists():
                logger.info(f"No data found for {data_type} on {date}")
                return None
            
            content = blob.download_as_string()
            return json.loads(content)
            
        except Exception as e:
            logger.error(f"Error reading raw data from GCS for {data_type} on {date}: {e}")
            return None
