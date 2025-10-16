"""Connection management tasks."""

def create_and_test_connection(**context) -> dict:
    """Create database connection and test it."""
    from include.weather.core.database.manager import WeatherDBManager
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        db_manager = WeatherDBManager(postgres_conn_id="WEATHER_POSTGRES_CONN")
        is_connected = db_manager.test_connection()
        
        if is_connected:
            logger.info("✅ Database connection successful!")
            return {
                "status": "success",
                "conn_id": "WEATHER_POSTGRES_CONN",
                "connected": True
            }
        else:
            logger.error("❌ Database connection test failed")
            return {
                "status": "failed", 
                "connected": False
            }
            
    except Exception as e:
        logger.error(f"❌ Failed to create database connection: {e}")
        return {
            "status": "error",
            "error": str(e),
            "connected": False
        }
