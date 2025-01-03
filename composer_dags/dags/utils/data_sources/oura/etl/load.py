from google.cloud import storage, bigquery
from google.api_core import exceptions
import pandas as pd
from typing import Dict, List, Any, Optional
import logging
import json
from datetime import datetime, date
import yaml
from pathlib import Path

from ..config.config import OuraConfig

logger = logging.getLogger(__name__)

class OuraLoader:
    def __init__(self, oura_config: OuraConfig):
        self.config = oura_config
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
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name}, skipping load")
            return
        
        dataset_ref = self.bq_client.dataset(self.config.dataset_id)
        table_ref = dataset_ref.table(table_name)
        
        # Get schema for this specific table
        schema = self._get_table_schema(table_name)
        
        # Find fields present in schema but missing in DataFrame
        schema_fields = {field.name: field.field_type for field in schema}
        missing_fields = {
            name: field_type 
            for name, field_type in schema_fields.items() 
            if name not in df.columns
        }
        
        # Find fields present in DataFrame but not in schema
        extra_fields = {
            col: str(df[col].dtype) 
            for col in df.columns 
            if col not in schema_fields
        }
        
        # Log schema differences and df info
        logger.info(f"DataFrame has {len(df)} rows")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        logger.info(f"DataFrame data types: {df.dtypes.to_dict()}")
    
        # Log schema differences and df not empty
        if missing_fields:
            logger.warning(
                f"Fields in schema but missing from DataFrame:\n"
                f"{'Field':<30} {'Schema Type':<15}\n" +
                "\n".join(f"{k:<30} {v:<15}" for k, v in sorted(missing_fields.items()))
            )
        
        if extra_fields:
            logger.warning(
                f"Fields in DataFrame but missing from schema:\n"
                f"{'Field':<30} {'DataFrame Type':<15}\n" +
                "\n".join(f"{k:<30} {v:<15}" for k, v in sorted(extra_fields.items()))
            )
            raise ValueError(f"Extra fields in DataFrame: {sorted(extra_fields)}")
        
        # Filter schema to only include fields present in the DataFrame
        filtered_schema = [
            field for field in schema 
            if field.name in df.columns or field.mode == 'NULLABLE'
        ]
        
        logger.info(
            f"Schema comparison for {table_name}:\n"
            f"Original schema fields: {len(schema)}\n"
            f"DataFrame columns: {len(df.columns)}\n"
            f"Filtered schema fields: {len(filtered_schema)}\n"
            f"Missing fields: {len(missing_fields)}\n"
            f"Extra fields: {len(extra_fields)}"
        )
        
        # Log DataFrame info
        logger.info(f"DataFrame info for {table_name}:")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Data types: {df.dtypes.to_dict()}")
        logger.info(f"Row count: {len(df)}")
        
        # Verify all object columns are properly stringified
        object_columns = df.select_dtypes(include=['object']).columns
        for col in object_columns:
            sample_value = df[col].iloc[0] if not df[col].empty else None
            if isinstance(sample_value, (list, dict)):
                logger.error(f"Column {col} contains non-stringified arrays or dicts")
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if pd.notnull(x) else None
                )
                logger.info(f"Converted column {col} to JSON strings")

        # Log column types for debugging
        logger.debug("DataFrame column types before loading:")
        for col, dtype in df.dtypes.items():
            logger.debug(f"Column: {col}, Type: {dtype}")

        # Add missing columns to DataFrame with NULL values
        for field_name in missing_fields:
            df[field_name] = None
            logger.info(f"Added missing column '{field_name}' with NULL values")
        
        # Configure job to append data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema
        )

        try:
            job = self.bq_client.load_table_from_dataframe(
                df,
                table_ref,
                job_config=job_config
            )
            
            # Wait for the job to complete and get detailed status
            job.result()
            logger.info(f"BigQuery job {job.job_id} completed with status: {job.state}")
            
            if job.errors:
                logger.error(f"Job errors: {job.errors}")
            else:
                logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
                
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {str(e)}", exc_info=True)
            raise

    def get_existing_dates(self, table_name: str) -> set:
        """Get set of dates that already exist in BigQuery table"""
        query = f"""
        SELECT DISTINCT day
        FROM `{self.config.project_id}.{self.config.dataset_id}.{table_name}`
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            if not df.empty:
                # Convert the day column to datetime if it's not already
                df['day'] = pd.to_datetime(df['day'])
                return set(df['day'].dt.date)
            return set()
        except Exception as e:
            raise ValueError(f"Error getting existing dates: {e}")

    def get_raw_data(self, data_type: str, start_date: date, end_date: date) -> Optional[Dict[str, Any]]:
        """
        Get raw data from GCS for specific date range and data type
        
        Args:
            data_type: Type of data (sleep, activity, readiness)
            start_date: Start date to fetch
            end_date: End date to fetch (not inclusive)
            
        Returns:
            Optional[Dict[str, Any]]: Matching records if found, None otherwise
        """
        try:
            bucket = self.storage_client.bucket(self.config.bucket_name)
            
            # List blobs with date range filter
            prefix = f"raw/oura/{data_type}/"
            blobs = bucket.list_blobs(prefix=prefix)
            
            # Filter blobs based on date range in path
            relevant_blobs = []
            for blob in blobs:
                if not blob.name.endswith('data.json'):
                    raise ValueError(f"Invalid blob name: {blob.name}")
                    
                # Extract date range from blob path
                # Path format: raw/oura/{data_type}/{start_date}_{end_date}/data.json
                path_parts = blob.name.split('/')
                if len(path_parts) < 4:
                    raise ValueError(f"Invalid blob path: {blob.name}")
                    
                date_range = path_parts[-2].split('_')
                if len(date_range) != 2:
                    raise ValueError(f"Invalid date range in blob path: {blob.name}")
                    
                try:
                    blob_start = datetime.strptime(date_range[0], '%Y-%m-%d').date()
                    blob_end = datetime.strptime(date_range[1], '%Y-%m-%d').date()
                    
                    # Check if date ranges overlap
                    if not (blob_end < start_date or blob_start > end_date):
                        relevant_blobs.append(blob)

                except ValueError:
                    raise ValueError(f"Invalid date format in blob path: {blob.name}")
            
            if not relevant_blobs:
                logger.info(f"No relevant blobs found for {data_type} between {start_date} and {end_date}")
                return None
                
            # Process relevant blobs
            records_by_day: Dict[str, Dict[str, Any]] = {}  # Dictionary to store records by day with blob info
            for blob in relevant_blobs:
                try:
                    content = blob.download_as_string()
                    data = json.loads(content)

                    # FIXME: move logic of error if day overlap from get_raw_data to run_transform_pipeline only for DAILY data types
                    
                    for record in data.get('data', []):
                        if 'day' in record:
                            record_date = datetime.strptime(record['day'], '%Y-%m-%d').date()
                            if start_date <= record_date < end_date:
                                if record['day'] in records_by_day:
                                    # We found an overlap - raise detailed error
                                    existing_record = records_by_day[record['day']]
                                    raise ValueError(
                                        f"Found overlapping data for date {record['day']}:\n"
                                        f"First record from blob: {existing_record['blob_path']}\n"
                                        f"Duplicate record from blob: {blob.name}\n"
                                        f"First record data: {existing_record['record']}\n"
                                        f"Duplicate record data: {record}"
                                    )
                                records_by_day[record['day']] = {
                                    'record': record,
                                    'blob_path': blob.name
                                }
                    
                except (json.JSONDecodeError, ValueError) as e:
                    raise ValueError(f"Error processing blob {blob.name}: {e}")
            
            all_matching_records = [info['record'] for info in records_by_day.values()]
            
            if all_matching_records:
                return {
                    'data': all_matching_records,
                    'is_processed': True,
                    'date_range': {
                        'start': start_date.isoformat(),
                        'end': end_date.isoformat()
                    }
                }
            
            logger.info(f"No data found for {data_type} between {start_date} and {end_date}")
            return None
            
        except Exception as e:
            raise ValueError(f"Error reading raw data: {e}")

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
            raise ValueError(f"Error checking existing data: {e}")
