from datetime import datetime, timedelta
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from dags.common.dag_factory import DagFactory
from weather.api.client import WeatherAPIClient
from weather.database.operations import WeatherDatabaseOperations
from common.config.settings import get_config
import logging

logger = logging.getLogger(__name__)

def fetch_weather_data(**context):
    """Fetch weather forecast data from API"""
    config = get_config()
    
    api_client = WeatherAPIClient(
        api_key=config.weather_api_key,
        base_url=config.weather_api_base_url
    )
    
    db_ops = WeatherDatabaseOperations(config.database_url)
    
    # Get date range for fetching data
    execution_date = context['execution_date']
    start_date = execution_date.date()
    end_date = start_date + timedelta(days=7)
    
    # Get active coordinates
    coordinate_ids = db_ops.get_active_coordinates()
    total_records = 0
    
    # Process each coordinate
    for coord_id in coordinate_ids:
        try:
            forecast_data = api_client.get_forecast_data(
                coordinate_id=coord_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if forecast_data:
                records_processed = db_ops.upsert_forecast_data(forecast_data)
                total_records += records_processed
                
        except Exception as e:
            logger.error(f"Error processing coordinate {coord_id}: {str(e)}")
            continue
    
    logger.info(f"Weather data fetched successfully. Total records: {total_records}")
    return total_records

def validate_weather_data(**context):
    """Validate weather data quality"""
    config = get_config()
    db_ops = WeatherDatabaseOperations(config.database_url)
    
    # Perform validation checks
    validation_results = db_ops.validate_recent_data()
    
    logger.info("Weather data validation completed")
    return validation_results

def cleanup_old_weather_data(**context):
    """Cleanup old weather data"""
    config = get_config()
    db_ops = WeatherDatabaseOperations(config.database_url)
    
    deleted_count = db_ops.delete_old_forecasts(days_to_keep=config.data_retention_days)
    
    logger.info(f"Old weather data cleaned up. Deleted {deleted_count} records")
    return deleted_count

# Create DAG using factory
weather_dag = DagFactory.create_ingestion_dag(
    dag_id='weather_forecast_ingestion',
    data_source='weather_api',
    ingestion_callable=fetch_weather_data,
    validation_callable=validate_weather_data,
    cleanup_callable=cleanup_old_weather_data,
    schedule_interval='0 */6 * * *'
)

# Export the DAG object
globals()[weather_dag.dag_id] = weather_dag.dag