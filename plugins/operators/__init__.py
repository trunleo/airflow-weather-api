from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Any, Dict, Optional
from datetime import date, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from weather.api.client import WeatherAPIClient
from weather.database.operations import WeatherDatabaseOperations
from common.config.settings import get_config

class WeatherIngestionOperator(BaseOperator):
    """Custom operator for weather data ingestion"""
    
    template_fields = ['start_date', 'end_date']
    
    @apply_defaults
    def __init__(
        self,
        coordinate_id: int,
        start_date: str = "{{ ds }}",
        end_date: str = "{{ next_ds }}",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.coordinate_id = coordinate_id
        self.start_date = start_date
        self.end_date = end_date
    
    def execute(self, context: Dict[str, Any]) -> int:
        """Execute weather data ingestion for a specific coordinate"""
        config = get_config()
        
        # Parse dates
        start_date = date.fromisoformat(self.start_date)
        end_date = date.fromisoformat(self.end_date)
        
        # Initialize clients
        api_client = WeatherAPIClient(
            api_key=config.weather_api_key,
            base_url=config.weather_api_base_url
        )
        
        db_ops = WeatherDatabaseOperations(config.database_url)
        
        # Fetch and store data
        forecast_data = api_client.get_forecast_data(
            coordinate_id=self.coordinate_id,
            start_date=start_date,
            end_date=end_date
        )
        
        records_processed = db_ops.upsert_forecast_data(forecast_data)
        
        self.log.info(
            f"Processed {records_processed} records for coordinate {self.coordinate_id}"
        )
        
        return records_processed
