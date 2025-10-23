from dataclasses import dataclass
from typing import Optional
from airflow.models import Variable
import os

@dataclass
class Config:
    """Application configuration class"""
    
    # Database
    database_url: str
    
    # Weather API
    weather_api_key: str
    weather_api_base_url: str
    weather_api_timeout: int = 30
    
    # Data retention
    data_retention_days: int = 30
    
    # Logging
    log_level: str = "INFO"
    
    @classmethod
    def from_airflow_variables(cls) -> 'Config':
        """Load configuration from Airflow Variables"""
        return cls(
            database_url=Variable.get('weather_db_connection'),
            weather_api_key=Variable.get('weather_api_key'),
            weather_api_base_url=Variable.get('weather_api_base_url'),
            weather_api_timeout=int(Variable.get('weather_api_timeout', '30')),
            data_retention_days=int(Variable.get('data_retention_days', '30')),
            log_level=Variable.get('log_level', 'INFO')
        )
    
    @classmethod
    def from_environment(cls) -> 'Config':
        """Load configuration from environment variables"""
        return cls(
            database_url=os.getenv('DATABASE_URL'),
            weather_api_key=os.getenv('WEATHER_API_KEY'),
            weather_api_base_url=os.getenv('WEATHER_API_BASE_URL'),
            weather_api_timeout=int(os.getenv('WEATHER_API_TIMEOUT', '30')),
            data_retention_days=int(os.getenv('DATA_RETENTION_DAYS', '30')),
            log_level=os.getenv('LOG_LEVEL', 'INFO')
        )

def get_config() -> Config:
    """Get configuration based on environment"""
    try:
        # Try Airflow Variables first
        return Config.from_airflow_variables()
    except:
        # Fallback to environment variables
        return Config.from_environment()