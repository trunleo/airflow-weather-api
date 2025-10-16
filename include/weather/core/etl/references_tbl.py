from include.weather.core.database.manager import WeatherDBManager
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
    
def get_reference_table(db_manager: WeatherDBManager, ref_tbl_name: str = "", **kwargs) -> Any:
    """Fetch and log all records from the specified reference table."""
    ref_tbl_name = ref_tbl_name.strip().lower()
    if not ref_tbl_name:
        raise ValueError("Reference table name must be provided.")
        return []
    
    if ref_tbl_name == "station" or ref_tbl_name == "stations":
        try:
            records = db_manager.get_stations()
            return records
        except Exception as e:
            logger.error(f"Error fetching stations: {e}")
            raise
    elif ref_tbl_name == "coordinates" or ref_tbl_name == "coordinates":
        try:
            records = db_manager.get_coordinates()
            return records
        except Exception as e:
            logger.error(f"Error fetching coordinates: {e}")
            raise
        
    return []

    # elif ref_tbl_name == "province" or ref_tbl_name == "provinces":
    #     records = db_manager.get_all_provinces()
    # elif ref_tbl_name == "district" or ref_tbl_name == "districts":
    #     records = db_manager.get_all_districts()
    # elif ref_tbl_name == "weather_condition" or ref_tbl_name == "weather_conditions":
    #     records = db_manager.get_all_weather_conditions()
    # else:
    #     logger.warning(f"Unknown reference table: {ref_tbl_name}")
    #     records = []

    logger.info(f"Fetched {len(records)} records from {ref_tbl_name}.")
    return records
    
    
    
