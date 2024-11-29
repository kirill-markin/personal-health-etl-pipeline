from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime, date
import logging
import json

logger = logging.getLogger(__name__)

class OuraTransformer:
    def _convert_to_date(self, date_str: str) -> Optional[date]:
        """Convert string date to date object"""
        try:
            return pd.to_datetime(date_str).date()
        except Exception as e:
            logger.warning(f"Error converting date {date_str}: {e}")
            return None

    def _convert_to_timestamp(self, timestamp_str: str) -> Optional[pd.Timestamp]:
        """Convert string timestamp to pandas Timestamp"""
        try:
            return pd.to_datetime(timestamp_str)
        except Exception as e:
            logger.warning(f"Error converting timestamp {timestamp_str}: {e}")
            return None

    def transform_sleep_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform sleep data into a structured format"""
        sleep_records = []
        
        for record in data.get('data', []):
            # Skip if missing required fields
            if not all(key in record for key in ['id', 'day']):
                raise ValueError(f"Missing required fields 'id' or 'day': {record}")
            
            date = self._convert_to_date(record.get('day'))
            if date is None:
                raise ValueError(f"Invalid date: {record.get('day')}")
            
            transformed_record = {
                'id': record.get('id'),
                'date': date,
                'duration': record.get('total_sleep_duration'),  # in seconds
                'restfulness': 100 - (record.get('restless_periods', 0) / 2),  # Derived metric
                'hr_average': record.get('average_heart_rate'),
                'hrv_average': record.get('average_hrv'),
                'temperature_delta': record.get('readiness', {}).get('temperature_deviation'),
                'efficiency': record.get('efficiency'),
                'latency': record.get('latency'),
                'light_sleep_duration': record.get('light_sleep_duration'),
                'rem_sleep_duration': record.get('rem_sleep_duration'),
                'deep_sleep_duration': record.get('deep_sleep_duration'),
                'restless_periods': record.get('restless_periods'),
                'average_breath': record.get('average_breath'),
                'lowest_heart_rate': record.get('lowest_heart_rate'),
                'time_in_bed': record.get('time_in_bed'),
                'awake_time': record.get('awake_time'),
                'bedtime_start': self._convert_to_timestamp(record.get('bedtime_start')),
                'bedtime_end': self._convert_to_timestamp(record.get('bedtime_end')),
                'sleep_phase_5_min': record.get('sleep_phase_5_min'),
                'sleep_algorithm_version': record.get('sleep_algorithm_version'),
                'type': record.get('type'),
                'period': record.get('period'),
                'readiness_score_delta': record.get('readiness_score_delta'),
                'sleep_score_delta': record.get('sleep_score_delta'),
                'low_battery_alert': record.get('low_battery_alert'),
                'movement_30_sec': record.get('movement_30_sec'),
                'heart_rate_interval': record.get('heart_rate', {}).get('interval'),
                'heart_rate_items': json.dumps(record.get('heart_rate', {}).get('items')),
                'heart_rate_timestamp': self._convert_to_timestamp(record.get('heart_rate', {}).get('timestamp')),
                'hrv_interval': record.get('hrv', {}).get('interval'),
                'hrv_items': json.dumps(record.get('hrv', {}).get('items')),
                'hrv_timestamp': self._convert_to_timestamp(record.get('hrv', {}).get('timestamp'))
            }
            
            sleep_records.append(transformed_record)
        
        df = pd.DataFrame(sleep_records)
        
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
            
            # Convert durations from seconds to hours
            duration_columns = ['duration', 'light_sleep_duration', 
                              'rem_sleep_duration', 'deep_sleep_duration', 
                              'time_in_bed', 'awake_time']
            for col in duration_columns:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: float(x) / 3600 if x is not None else None
                    )
        
        return df
    
    def transform_activity_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform activity data into a structured format"""
        activity_records = []
        
        for record in data.get('data', []):
            # Skip if missing required fields
            if not all(key in record for key in ['id', 'day']):
                raise ValueError(f"Missing required fields 'id' or 'day': {record}")
            
            date = self._convert_to_date(record.get('day'))
            if date is None:
                raise ValueError(f"Invalid date: {record.get('day')}")
            
            transformed_record = {
                'id': record.get('id'),
                'date': date,
                'calories_active': record.get('active_calories'),
                'calories_total': record.get('total_calories'),
                'steps': record.get('steps'),
                'daily_movement': record.get('equivalent_walking_distance'),
                'activity_score': record.get('score'),
                'inactivity_alerts': record.get('inactivity_alerts'),
                'average_met': record.get('average_met_minutes'),
                'high_activity_time': record.get('high_activity_time'),
                'medium_activity_time': record.get('medium_activity_time'),
                'low_activity_time': record.get('low_activity_time'),
                'sedentary_time': record.get('sedentary_time'),
                'resting_time': record.get('resting_time'),
                'non_wear_time': record.get('non_wear_time'),
                'high_activity_met_minutes': record.get('high_activity_met_minutes'),
                'medium_activity_met_minutes': record.get('medium_activity_met_minutes'),
                'low_activity_met_minutes': record.get('low_activity_met_minutes'),
                'sedentary_met_minutes': record.get('sedentary_met_minutes'),
                'target_calories': record.get('target_calories'),
                'target_meters': record.get('target_meters'),
                'meters_to_target': record.get('meters_to_target')
            }
            
            activity_records.append(transformed_record)
        
        df = pd.DataFrame(activity_records)
        
        if not df.empty:
            # Convert time fields from seconds to hours
            time_columns = ['high_activity_time', 'medium_activity_time', 
                           'low_activity_time', 'sedentary_time', 
                           'resting_time', 'non_wear_time']
            
            for col in time_columns:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: float(x) / 3600 if x is not None else None
                    )
        
        return df

    def transform_readiness_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform readiness data into a structured format"""
        try:
            logger.info(f"Starting readiness transformation with {len(data.get('data', []))} records")
            readiness_records = []
            
            for record in data.get('data', []):
                logger.info(f"Processing readiness record: {record.get('id')}")
                
                # Skip if missing required fields
                if not all(key in record for key in ['id', 'day']):
                    raise ValueError(f"Missing required fields 'id' or 'day': {record}")
                
                date = self._convert_to_date(record.get('day'))
                if date is None:
                    raise ValueError(f"Invalid date: {record.get('day')}")
                
                transformed_record = {
                    'id': record.get('id'),
                    'date': date,
                    'score': record.get('score'),
                    'temperature_trend_deviation': record.get('temperature_trend_deviation'),
                    'hrv_balance_score': record.get('contributors', {}).get('hrv_balance'),
                    'temperature_deviation': record.get('temperature_deviation')
                }
                
                readiness_records.append(transformed_record)
            
            logger.info(f"Created {len(readiness_records)} transformed records")
            
            df = pd.DataFrame(readiness_records)
            logger.info(f"Created DataFrame with shape: {df.shape}")
            
            # Ensure date column is date type
            if not df.empty and 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
                logger.info("Successfully converted date column")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in transform_readiness_data: {str(e)}", exc_info=True)
            raise
    
    def transform_data(self, raw_data: Dict[str, Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        Transform all data types
        
        Args:
            raw_data: Dictionary containing raw data for each data type
            Example: {'readiness': {'data': [...], 'is_processed': True}}
        
        Returns:
            Dictionary mapping data types to transformed DataFrames
        """
        transformed_data: Dict[str, pd.DataFrame] = {}
        
        try:
            for data_type, data in raw_data.items():
                logger.info(f"Processing {data_type} data")
                
                if data_type == 'sleep':
                    transformed_data['sleep'] = self.transform_sleep_data(data)
                    
                elif data_type == 'activity':
                    transformed_data['activity'] = self.transform_activity_data(data)
                    
                elif data_type == 'readiness':
                    transformed_data['readiness'] = self.transform_readiness_data(data)
                
                if data_type in transformed_data:
                    df = transformed_data[data_type]
                    if not df.empty:
                        logger.info(f"Transformed {data_type} data shape: {df.shape}")
                        logger.info(f"Sample transformed record: {df.iloc[0].to_dict()}")
                    else:
                        logger.warning(f"Empty DataFrame for {data_type}")
        
        except Exception as e:
            logger.error(f"Error in transform_data: {str(e)}", exc_info=True)
            raise
        
        return transformed_data
