from typing import Dict, Any, List
from datetime import datetime

def transform_api_response(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform API response to database format"""
    transformed_data = []
    
    for record in raw_data.get('data', []):
        transformed_record = {
            'coordinate_id': record.get('coordinate_id'),
            'date': datetime.strptime(record['date'], '%Y-%m-%d').date(),
            'hour': record.get('hour'),
            'tc': record.get('temperature'),
            'rh': record.get('humidity'),
            'slp': record.get('pressure'),
            'rain': record.get('rainfall'),
            'cloudlow': record.get('cloud_low'),
            'cloudmed': record.get('cloud_medium'),
            'cloudhigh': record.get('cloud_high'),
            'cond': record.get('condition_code'),
            'wdfn': record.get('wind_direction_name'),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        # Handle wind speed and direction at different levels
        wind_levels = ['10m', '925', '850', '700', '500', '200']
        for level in wind_levels:
            ws_key = f'ws_{level}'
            wd_key = f'wd_{level}'
            transformed_record[ws_key] = record.get(f'wind_speed_{level}')
            transformed_record[wd_key] = record.get(f'wind_direction_{level}')
        
        transformed_data.append(transformed_record)
    
    return transformed_data

def transform_to_db_format(forecast_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Transform forecast dictionary to database format"""
    # Remove None values and ensure proper data types
    cleaned_data = {k: v for k, v in forecast_dict.items() if v is not None}
    
    # Ensure datetime fields are properly formatted
    if 'created_at' not in cleaned_data:
        cleaned_data['created_at'] = datetime.utcnow()
    if 'updated_at' not in cleaned_data:
        cleaned_data['updated_at'] = datetime.utcnow()
    
    return cleaned_data