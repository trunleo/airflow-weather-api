"""
Weather Forecast DAG using Custom Operators
This DAG demonstrates the use of custom weather operators with existing modules
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add plugins path for custom operators
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from operators.weather_operator import (
    WeatherDataFetchOperator,
    WeatherDataStoreOperator,
    WeatherDataValidationOperator,
    WeatherDataCleanupOperator,
    WeatherHealthCheckOperator
)

# DAG Configuration
DAG_ID = 'weather_forecast_custom_operators'
SCHEDULE_INTERVAL = '0 */6 * * *'  # Every 6 hours
MAX_ACTIVE_RUNS = 1
CATCHUP = False

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_tis_per_dag': 10
}

# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Weather forecast data pipeline using custom operators and existing modules',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['weather', 'forecast', 'custom-operators', 'data-pipeline']
)

# Configuration function
def get_coordinates_from_config(**context):
    """Get coordinate IDs from Airflow Variable"""
    try:
        coordinates_str = Variable.get('weather_coordinates', '1,2,3,4,5')
        coordinates = [int(coord.strip()) for coord in coordinates_str.split(',')]
        return coordinates
    except Exception as e:
        # Fallback to default coordinates
        return [1, 2, 3, 4, 5]

# Task 1: API Health Check
api_health_check = WeatherHealthCheckOperator(
    task_id='check_api_health',
    api_key_var='weather_api_key',
    api_url_var='weather_api_base_url',
    api_timeout=30,
    dag=dag
)

# Task 2: Get coordinates configuration
get_coordinates_task = PythonOperator(
    task_id='get_coordinates_config',
    python_callable=get_coordinates_from_config,
    dag=dag
)

# Task 3: Fetch weather data
fetch_weather_data = WeatherDataFetchOperator(
    task_id='fetch_weather_forecast_data',
    coordinate_ids="{{ task_instance.xcom_pull(task_ids='get_coordinates_config') }}",
    days_ahead=7,
    api_key_var='weather_api_key',
    api_url_var='weather_api_base_url',
    api_timeout=60,
    xcom_key='weather_forecast_data',
    dag=dag
)

# Task 4: Store weather data
store_weather_data = WeatherDataStoreOperator(
    task_id='store_weather_data',
    connection_string_var='weather_db_connection_string',
    xcom_key='weather_forecast_data',
    source_task_id='fetch_weather_forecast_data',
    create_tables=True,
    dag=dag
)

# Task 5: Validate stored data
validate_data = WeatherDataValidationOperator(
    task_id='validate_stored_data',
    connection_string_var='weather_db_connection_string',
    min_records_threshold=50,
    max_missing_temp_percentage=10.0,
    dag=dag
)

# Task 6: Clean up old data
cleanup_old_data = WeatherDataCleanupOperator(
    task_id='cleanup_old_weather_data',
    connection_string_var='weather_db_connection_string',
    days_to_keep="{{ var.value.get('weather_data_retention_days', 30) }}",
    dag=dag
)

# Task 7: Final summary
def generate_pipeline_summary(**context):
    """Generate a summary of the entire pipeline execution"""
    
    # Collect data from all previous tasks
    fetch_summary = context['task_instance'].xcom_pull(
        task_ids='fetch_weather_forecast_data', 
        key='fetch_summary'
    )
    
    storage_summary = context['task_instance'].xcom_pull(
        task_ids='store_weather_data', 
        key='storage_summary'
    )
    
    validation_results = context['task_instance'].xcom_pull(
        task_ids='validate_stored_data', 
        key='validation_results'
    )
    
    cleanup_summary = context['task_instance'].xcom_pull(
        task_ids='cleanup_old_weather_data', 
        key='cleanup_summary'
    )
    
    health_check = context['task_instance'].xcom_pull(
        task_ids='check_api_health', 
        key='health_check_result'
    )
    
    # Create comprehensive summary
    pipeline_summary = {
        'execution_date': context['execution_date'].isoformat(),
        'dag_run_id': context['dag_run'].dag_id,
        'api_health': health_check,
        'data_fetch': fetch_summary,
        'data_storage': storage_summary,
        'data_validation': validation_results,
        'data_cleanup': cleanup_summary,
        'pipeline_status': 'completed_successfully',
        'total_processing_time': datetime.now().isoformat()
    }
    
    # Log summary
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Weather Pipeline Summary: {pipeline_summary}")
    
    # Store final summary
    context['task_instance'].xcom_push(key='pipeline_summary', value=pipeline_summary)
    
    return pipeline_summary

pipeline_summary = PythonOperator(
    task_id='generate_pipeline_summary',
    python_callable=generate_pipeline_summary,
    dag=dag
)

# Define task dependencies
api_health_check >> get_coordinates_task
get_coordinates_task >> fetch_weather_data
fetch_weather_data >> store_weather_data
store_weather_data >> validate_data
validate_data >> cleanup_old_data
cleanup_old_data >> pipeline_summary

# Alternative: Parallel execution for some tasks
# You can also run validation and cleanup in parallel after storage:
# store_weather_data >> [validate_data, cleanup_old_data] >> pipeline_summary