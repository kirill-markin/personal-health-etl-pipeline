from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

class OuraTransformer:
    def _convert_to_date(self, date_str: str) -> Optional[date]:
        """Convert string date to date object"""
        try:
            return pd.to_datetime(date_str).date()
        except Exception as e:
            logger.warning(f"Error converting date {date_str}: {e}")
            return None

    def transform_sleep_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform sleep data into a structured format"""
        sleep_records = []
        
        for record in data.get('data', []):
            transformed_record = {
                'id': record.get('id'),
                'date': self._convert_to_date(record.get('day')),
                'duration': record.get('duration'),
                'sleep_score': record.get('sleep_score'),
                'restfulness': record.get('restfulness'),
                'hr_average': record.get('heart_rate_average'),
                'hrv_average': record.get('hrv_average'),
                'temperature_delta': record.get('temperature_delta'),
            }
            if transformed_record['date'] is not None:
                sleep_records.append(transformed_record)
            
        df = pd.DataFrame(sleep_records)
        
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        return df
    
    def transform_activity_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform activity data into a structured format"""
        activity_records = []
        
        for record in data.get('data', []):
            date = self._convert_to_date(record.get('day'))
            if date is not None:  # Only add records with valid dates
                transformed_record = {
                    'id': record.get('id'),
                    'date': date,
                    'calories_active': record.get('calories_active'),
                    'calories_total': record.get('calories_total'),
                    'steps': record.get('steps'),
                    'daily_movement': record.get('daily_movement'),
                    'activity_score': record.get('activity_score'),
                    'inactivity_alerts': record.get('inactivity_alerts'),
                    'average_met': record.get('average_met'),
                }
                activity_records.append(transformed_record)
            
        df = pd.DataFrame(activity_records)
        
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date  # Ensure date column is date type
        
        return df

    def transform_readiness_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform readiness data into a structured format"""
        readiness_records = []
        
        for record in data.get('data', []):
            transformed_record = {
                'id': record.get('id'),
                'date': self._convert_to_date(record.get('day')),
                'score': record.get('score'),
                'temperature_trend_deviation': record.get('temperature_trend_deviation'),
                'hrv_balance_score': record.get('hrv_balance_score'),
            }
            if transformed_record['date'] is not None:  # Only add records with valid dates
                readiness_records.append(transformed_record)
            
        df = pd.DataFrame(readiness_records)
        
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date  # Ensure date column is date type
        
        return df
    
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Transform all data types
        
        Args:
            raw_data: Dictionary containing raw data for each data type
        
        Returns:
            Dictionary mapping data types to transformed DataFrames
        """
        transformed_data: Dict[str, pd.DataFrame] = {}
        
        # Transform each data type if present in raw_data
        if 'sleep' in raw_data and raw_data['sleep'].get('data'):
            transformed_data['sleep'] = self.transform_sleep_data(raw_data['sleep'])
            
        if 'activity' in raw_data and raw_data['activity'].get('data'):
            transformed_data['activity'] = self.transform_activity_data(raw_data['activity'])
            
        if 'readiness' in raw_data and raw_data['readiness'].get('data'):
            transformed_data['readiness'] = self.transform_readiness_data(raw_data['readiness'])
        
        # Ensure each DataFrame has required columns
        for data_type, df in transformed_data.items():
            if df.empty:
                # Create empty DataFrame with required columns
                transformed_data[data_type] = pd.DataFrame(columns=['id', 'date'])
            elif 'date' not in df.columns:
                logger.warning(f"Missing 'date' column in {data_type} DataFrame")
                df['date'] = None
            
        return transformed_data
