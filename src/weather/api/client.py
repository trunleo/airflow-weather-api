import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import date
from .models import WeatherForecastResponse
from ..utils.transformations import transform_api_response

logger = logging.getLogger(__name__)

class WeatherAPIClient:
    """Client for weather API interactions"""
    
    def __init__(self, api_key: str, base_url: str, timeout: int = 30):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create configured requests session"""
        session = requests.Session()
        session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'Airflow-Weather-Pipeline/1.0'
        })
        return session
    
    def get_forecast_data(
        self, 
        coordinate_id: int, 
        start_date: date, 
        end_date: date
    ) -> List[Dict[str, Any]]:
        """Fetch forecast data for coordinate and date range"""
        
        endpoint = f"{self.base_url}/forecast"
        params = {
            'coordinate_id': coordinate_id,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        }
        
        try:
            response = self.session.get(
                endpoint, 
                params=params, 
                timeout=self.timeout
            )
            response.raise_for_status()
            
            raw_data = response.json()
            forecast_response = WeatherForecastResponse.from_api_response(raw_data)
            
            # Convert to dict format for database operations
            return [forecast.__dict__ for forecast in forecast_response.forecasts]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for coordinate {coordinate_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing API response: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check API health status"""
        try:
            response = self.session.get(f"{self.base_url}/health")
            return response.status_code == 200
        except:
            return False