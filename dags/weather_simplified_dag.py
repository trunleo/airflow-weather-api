"""
Simplified Weather Forecast DAG
Uses existing weather modules with standard PythonOperator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook 
import sys
import os
import logging

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from weather.api.client import WeatherAPIClient
from weather.database.operations import WeatherDatabaseOperations
from weather.utils.transformations import transform_to_db_format

logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'weather_simplified_pipeline',
    default_args=default_args,
    description='Simplified weather data pipeline using existing modules',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    max_active_runs=1,
    catchup=False,
    tags=['weather', 'simplified', 'pipeline']
)

def get_coordinate_ids_from_db(db_ops):
    """
    Get active coordinate IDs from database
    """
    try:
        coordinate_ids = db_ops.get_active_coordinates()
        logger.info(f"Retrieved {len(coordinate_ids)} active coordinates from database: {coordinate_ids}")
        return coordinate_ids
    except Exception as e:
        logger.warning(f"Failed to get coordinates from database: {str(e)}")
        return []

def get_coordinate_ids_from_variable():
    """
    Get coordinate IDs from Airflow Variable
    """
    try:
        coordinates_str = Variable.get('weather_coordinates', default_var=None)
        if coordinates_str:
            coordinate_ids = [int(coord.strip()) for coord in coordinates_str.split(',')]
            logger.info(f"Retrieved {len(coordinate_ids)} coordinates from variable: {coordinate_ids}")
            return coordinate_ids
        else:
            logger.info("No weather_coordinates variable found")
            return []
    except Exception as e:
        logger.warning(f"Failed to get coordinates from variable: {str(e)}")
        return []

def fetch_and_store_weather_data(**context):
    """
    Combined task to fetch weather data from API and store in database
    Uses WeatherAPIClient and WeatherDatabaseOperations modules
    
    Coordinate Selection Logic:
    1. First try to get coordinates from 'weather_coordinates' variable (specific list)
    2. If variable not set or empty, get all active coordinates from database
    3. If both fail, use default fallback coordinates
    """
    logger.info("Starting weather data fetch and store process")
    
    # Get configuration from Airflow Variables
    try:
        api_key = Variable.get('weather_api_key')
        api_url = Variable.get('weather_api_base_url', 'https://api.weather.com/v1')
        db_connection_string = Variable.get('weather_db_connection_string')
        days_ahead = int(Variable.get('weather_forecast_days', '7'))
        
    except Exception as e:
        logger.error(f"Failed to get configuration: {str(e)}")
        raise
    
    # Initialize database operations
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    # Ensure tables exist
    try:
        db_ops.create_tables()
        logger.info("Database tables verified/created")
    except Exception as e:
        logger.warning(f"Table creation warning: {str(e)}")
    
    # Get coordinate IDs with priority logic
    coordinate_ids = []
    
    # 1. Try to get from variable first (specific list override)
    coordinate_ids = get_coordinate_ids_from_variable()
    coordinate_source = "variable"
    
    # 2. If no variable or empty, get all active from database
    if not coordinate_ids:
        coordinate_ids = get_coordinate_ids_from_db(db_ops)
        coordinate_source = "database"
    
    # 3. Fallback to default if both methods fail
    if not coordinate_ids:
        coordinate_ids = [1, 2, 3, 4, 5]  # Default fallback
        coordinate_source = "default_fallback"
        logger.warning("Using default fallback coordinates")
    
    logger.info(f"Using {len(coordinate_ids)} coordinates from {coordinate_source}: {coordinate_ids}")
    
    # Initialize API client
    api_client = WeatherAPIClient(
        api_key=api_key,
        base_url=api_url,
        timeout=60
    )
    
    # Determine date range
    execution_date = context['execution_date']
    start_date = execution_date.date()
    end_date = (execution_date + timedelta(days=days_ahead)).date()
    
    logger.info(f"Fetching weather data for {len(coordinate_ids)} coordinates from {start_date} to {end_date}")
    
    # Fetch and store data for each coordinate
    total_records = 0
    successful_coords = []
    failed_coords = []
    
    for coord_id in coordinate_ids:
        try:
            logger.info(f"Processing coordinate {coord_id}")
            
            # Fetch data from API
            forecast_data = api_client.get_forecast_data(
                coordinate_id=coord_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if not forecast_data:
                logger.warning(f"No data returned for coordinate {coord_id}")
                continue
            
            # Transform data for database storage
            transformed_data = [transform_to_db_format(record) for record in forecast_data]
            
            # Store in database
            rows_affected = db_ops.upsert_forecast_data(transformed_data)
            
            total_records += len(transformed_data)
            successful_coords.append(coord_id)
            
            logger.info(f"Coordinate {coord_id}: fetched {len(forecast_data)} records, stored {rows_affected} rows")
            
        except Exception as e:
            logger.error(f"Failed to process coordinate {coord_id}: {str(e)}")
            failed_coords.append(coord_id)
            continue
    
    # Log summary
    logger.info(f"Processing completed: {len(successful_coords)} successful, {len(failed_coords)} failed")
    logger.info(f"Total records processed: {total_records}")
    
    if failed_coords:
        logger.warning(f"Failed coordinates: {failed_coords}")
    
    # Store summary in XCom
    summary = {
        'total_records': total_records,
        'successful_coordinates': successful_coords,
        'failed_coordinates': failed_coords,
        'coordinate_source': coordinate_source,
        'coordinates_used': coordinate_ids,
        'date_range': f"{start_date} to {end_date}",
        'execution_time': datetime.now().isoformat()
    }
    
    context['task_instance'].xcom_push(key='processing_summary', value=summary)
    
    return summary

def validate_weather_data(**context):
    """
    Validate the quality of stored weather data
    Uses WeatherDatabaseOperations module
    """
    logger.info("Starting data validation")
    
    # Get database connection
    db_connection_string = Variable.get('weather_db_connection_string')
    
    # Initialize database operations
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    try:
        # Run data validation
        validation_results = db_ops.validate_recent_data()
        
        logger.info(f"Validation results: {validation_results}")
        
        # Check quality thresholds
        quality_score = validation_results['data_quality_score']
        recent_count = validation_results['recent_records_count']
        
        # Define thresholds
        min_quality_score = float(Variable.get('min_data_quality_score', '80.0'))
        min_recent_records = int(Variable.get('min_recent_records', '100'))
        
        # Validate against thresholds
        if quality_score < min_quality_score:
            raise ValueError(f"Data quality below threshold: {quality_score:.1f}% < {min_quality_score}%")
        
        if recent_count < min_recent_records:
            raise ValueError(f"Insufficient recent records: {recent_count} < {min_recent_records}")
        
        logger.info(f"Data validation passed - Quality: {quality_score:.1f}%, Records: {recent_count}")
        
        # Store validation results
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise

def cleanup_old_weather_data(**context):
    """
    Clean up old weather data based on retention policy
    Uses WeatherDatabaseOperations module
    """
    logger.info("Starting data cleanup")
    
    # Get configuration
    db_connection_string = Variable.get('weather_db_connection_string')
    retention_days = int(Variable.get('weather_data_retention_days', '30'))
    
    # Initialize database operations
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    try:
        # Perform cleanup
        deleted_count = db_ops.delete_old_forecasts(days_to_keep=retention_days)
        
        logger.info(f"Cleanup completed - deleted {deleted_count} old records (retention: {retention_days} days)")
        
        # Store cleanup results
        cleanup_summary = {
            'deleted_records': deleted_count,
            'retention_days': retention_days,
            'cleanup_time': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='cleanup_summary', value=cleanup_summary)
        
        return cleanup_summary
        
    except Exception as e:
        logger.error(f"Data cleanup failed: {str(e)}")
        raise

def check_coordinate_configuration(**context):
    """
    Check and log coordinate configuration before processing
    This task shows which coordinates will be used and from what source
    """
    logger.info("Checking coordinate configuration")
    
    # Get database connection
    db_connection_string = Variable.get('weather_db_connection_string')
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    # Check coordinates from variable
    variable_coords = get_coordinate_ids_from_variable()
    
    # Check coordinates from database
    db_coords = get_coordinate_ids_from_db(db_ops)
    
    # Determine which will be used
    if variable_coords:
        coords_to_use = variable_coords
        source = "Airflow Variable 'weather_coordinates'"
    elif db_coords:
        coords_to_use = db_coords
        source = "Database (active coordinates)"
    else:
        coords_to_use = [1, 2, 3, 4, 5]
        source = "Default fallback"
    
    # Log detailed information
    logger.info("=== COORDINATE CONFIGURATION ===")
    logger.info(f"Variable coordinates: {variable_coords if variable_coords else 'Not set'}")
    logger.info(f"Database coordinates: {db_coords if db_coords else 'None found'}")
    logger.info(f"WILL USE: {coords_to_use} from {source}")
    logger.info("================================")
    
    # Store configuration info in XCom
    config_info = {
        'variable_coordinates': variable_coords,
        'database_coordinates': db_coords,
        'coordinates_to_use': coords_to_use,
        'source': source,
        'check_time': datetime.now().isoformat()
    }
    
    context['task_instance'].xcom_push(key='coordinate_config', value=config_info)
    
    return config_info

def check_api_health(**context):
    """
    Check weather API health status
    Uses WeatherAPIClient module
    """
    logger.info("Checking API health")
    
    # Get API configuration
    api_key = Variable.get('weather_api_key')
    api_url = Variable.get('weather_api_base_url', 'https://api.weather.com/v1')
    
    # Initialize API client
    api_client = WeatherAPIClient(
        api_key=api_key,
        base_url=api_url,
        timeout=30
    )
    
    try:
        # Perform health check
        is_healthy = api_client.health_check()
        
        if not is_healthy:
            raise ValueError("Weather API health check failed")
        
        logger.info("API health check passed")
        
        health_status = {
            'status': 'healthy',
            'api_url': api_url,
            'check_time': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='health_status', value=health_status)
        
        return health_status
        
    except Exception as e:
        logger.error(f"API health check failed: {str(e)}")
        raise

# Define tasks
coordinate_config_task = PythonOperator(
    task_id='check_coordinate_configuration',
    python_callable=check_coordinate_configuration,
    dag=dag
)

api_health_task = PythonOperator(
    task_id='check_api_health',
    python_callable=check_api_health,
    dag=dag
)

fetch_store_task = PythonOperator(
    task_id='fetch_and_store_weather_data',
    python_callable=fetch_and_store_weather_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_weather_data',
    python_callable=validate_weather_data,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_weather_data,
    dag=dag
)

# Define task dependencies
coordinate_config_task >> api_health_task >> fetch_store_task >> validate_task >> cleanup_task