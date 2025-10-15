from weather.db_manage import WeatherDBManager
import logging
from typing import Any

logger = logging.getLogger(__name__)

def create_connection(postgres_conn_id: str = "WEATHER_POSTGRES_CONN") -> WeatherDBManager:
    """Create and return a WeatherDBManager instance."""
    try:
        db_manager = WeatherDBManager(
            postgres_conn_id=postgres_conn_id
        )
        logger.info("Database connection created successfully.")
        return db_manager
    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        raise
    
def get_reference_table(db_manager: WeatherDBManager) -> Any:
    """Fetch and log all records from the specified reference table."""
    try:
        station_df = db_manager.get_stations()
        logger.info(f"Stations: {station_df}")
        return station_df
    except Exception as e:
        logger.error(f"Error fetching records from stations: {e}")
        raise
        return []
    
    
    
    
