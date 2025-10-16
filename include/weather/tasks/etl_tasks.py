"""ETL task functions."""

def get_reference_data(**context) -> dict:
    """Get reference data using the database connection."""
    from include.weather.core.database.manager import WeatherDBManager
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Check if connection was successful from upstream task
    connection_info = context['ti'].xcom_pull(task_ids='create_connection')
    
    if not connection_info or not connection_info.get('connected'):
        logger.error("❌ Database connection not available")
        return {"status": "failed", "error": "No database connection"}
    
    try:
        db_manager = WeatherDBManager(postgres_conn_id="WEATHER_POSTGRES_CONN")
        
        stations = db_manager.get_stations()
        provinces = db_manager.get_provinces() 
        coordinates = db_manager.get_coordinates()
        
        logger.info("📊 Retrieved reference data:")
        logger.info(f"  - Stations: {len(stations)}")
        logger.info(f"  - Provinces: {len(provinces)}")
        logger.info(f"  - Coordinates: {len(coordinates)}")
        
        return {
            "status": "success",
            "stations_count": len(stations),
            "provinces_count": len(provinces),
            "coordinates_count": len(coordinates)
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get reference data: {e}")
        return {"status": "error", "error": str(e)}


def extract_weather_current(**context) -> dict:
    """Extract current weather data."""
    # Implementation for current weather ETL
    pass


def extract_weather_forecast(**context) -> dict:
    """Extract weather forecast data."""
    # Implementation for forecast weather ETL
    pass
