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
            sleep_records.append(transformed_record)
            
        df = pd.DataFrame(sleep_records)
        df['date'] = pd.to_datetime(df['date']).dt.date  # Ensure date column is date type
        return df
    
    def transform_activity_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform activity data into a structured format"""
        activity_records = []
        
        for record in data.get('data', []):
            transformed_record = {
                'id': record.get('id'),
                'date': self._convert_to_date(record.get('day')),
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
            readiness_records.append(transformed_record)
            
        df = pd.DataFrame(readiness_records)
        df['date'] = pd.to_datetime(df['date']).dt.date  # Ensure date column is date type
        return df
    
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform all data types"""
        transformed_data = {}
        
        if 'sleep' in raw_data:
            transformed_data['sleep'] = self.transform_sleep_data(raw_data['sleep'])
            
        if 'activity' in raw_data:
            transformed_data['activity'] = self.transform_activity_data(raw_data['activity'])
            
        if 'readiness' in raw_data:
            transformed_data['readiness'] = self.transform_readiness_data(raw_data['readiness'])
            
        return transformed_data
