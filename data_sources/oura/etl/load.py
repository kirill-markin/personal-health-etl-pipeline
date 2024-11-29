from google.cloud import storage, bigquery
from google.api_core import exceptions
import pandas as pd
from typing import Dict, List, Union, Any, Optional
import logging
import json
from datetime import datetime, date
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
    
    def save_to_gcs(self, data: Dict, data_type: str, start_date: date, end_date: date) -> str:
        """Save raw data to Google Cloud Storage"""
        bucket = self.storage_client.bucket(self.config.bucket_name)
        
        # Format the blob path using the config template
        blob_path = self.config.raw_data_path_str.format(
            data_type=data_type,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
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

    def get_raw_data(self, data_type: str, start_date: date, end_date: date) -> Optional[Dict[str, Any]]:
        """
        Get raw data from GCS for specific date range and data type.
        Searches through all available blobs and combines data points that fall within the requested range.
        
        Args:
            data_type: Type of data (activity, sleep, etc.)
            start_date: Start date of the range
            end_date: End date of the range
            
        Returns:
            Optional[Dict[str, Any]]: Combined data for the requested date range, or None if no data found
        """
        try:
            bucket = self.storage_client.bucket(self.config.bucket_name)
            base_path = self.config.raw_data_path_str.format(
                data_type=data_type,
                date="*",  # Use wildcard to list all blobs for this data type
                start_date="*",
                end_date="*"
            ).rsplit('/', 1)[0]  # Remove the 'data.json' part
            
            # List all blobs in the data type directory
            blobs = bucket.list_blobs(prefix=base_path)
            
            combined_data: List[Dict[str, Any]] = []
            
            for blob in blobs:
                if not blob.name.endswith('data.json'):
                    continue
                    
                try:
                    content = blob.download_as_string()
                    blob_data = json.loads(content)
                    
                    # Filter data points within the requested date range
                    for record in blob_data.get('data', []):
                        if 'day' not in record:
                            raise ValueError(f"Missing 'day' field in record: {record}")
                            
                        record_date = datetime.strptime(record['day'], '%Y-%m-%d').date()
                        if start_date <= record_date <= end_date:
                            combined_data.append(record)
                            
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Error parsing data from blob {blob.name}: {e}")
                    continue
            
            if not combined_data:
                logger.info(f"No data found for {data_type} between {start_date} and {end_date}")
                return None
                
            # Return in the same format as the original data
            return {
                'data': combined_data,
                'is_processed': True,  # Add flag to indicate this is processed data
                'date_range': {
                    'start': start_date.isoformat(),
                    'end': end_date.isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error reading raw data from GCS for {data_type} between {start_date} and {end_date}: {e}")
            return None

    def check_existing_data(self, data_type: str, date: str) -> bool:
        """
        Check if data already exists in BigQuery for given date and data type
        
        Args:
            data_type: Type of Oura data (activity, sleep, etc.)
            date: Date to check in YYYY-MM-DD format
        
        Returns:
            bool: True if data exists, False otherwise
        """
        query = f"""
            SELECT COUNT(*) as count 
            FROM `{self.config.project_id}.{self.config.dataset_id}.oura_{data_type}`
            WHERE date = '{date}'
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            row = next(results)
            return row.count > 0
        except Exception as e:
            logger.error(f"Error checking existing data: {e}")
            return False
