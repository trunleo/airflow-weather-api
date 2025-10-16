"""Application settings and configuration."""

import os
from typing import Dict, Any

class WeatherConfig:
    """Weather application configuration."""
    
    # Database settings
    DEFAULT_POSTGRES_CONN_ID = "WEATHER_POSTGRES_CONN"
    
    # Alert thresholds
    HEAVY_RAIN_MIN = 35.0
    HEAVY_RAIN_MAX = 90.0
    HEAT_WAVE_TEMP = 35.0
    HEAT_WAVE_DAYS = 2
    
    # Data retention
    DATA_RETENTION_DAYS = 90
    
    # Webhook settings
    WEBHOOK_TIMEOUT = 30
    
    @classmethod
    def get_airflow_variables(cls) -> Dict[str, Any]:
        """Get required Airflow variables."""
        return {
            "WEATHER_WEBHOOK_URL": "",
            "WEATHER_WEBHOOK_API_KEY": None,
            "WEATHER_WEBHOOK_SECRET": None,
            "WEATHER_DATA_RETENTION_DAYS": "90",
            "WEATHER_ALERT_EMAIL": "admin@example.com",
            "WEATHER_FORECAST_DAYS": "7",
        }
