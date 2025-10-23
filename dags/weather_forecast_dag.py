"""
File: weather_forecast_dag.py
Path: ./dags/weather_forecast_dag.py
Description: Weather forecast data ingestion DAG - fetches weather data from API and stores in database
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
import sys
import os
import logging
import requests
import pandas as pd
from typing import List, Dict, Any, Optional

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

logger = logging.getLogger(__name__)

# DAG Default Arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'weather_forecast_ingestion',
    default_args=default_args,
    description='Weather forecast data ingestion from API to database',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    max_active_runs=1,
    tags=['weather', 'forecast', 'ingestion', 'api']
)

# Configuration functions
def get_config():
    """Get configuration from Airflow Variables"""
    try:
        return {
            'weather_api_key': Variable.get('weather_api_key'),
            'weather_api_base_url': Variable.get('weather_api_base_url', 'https://api.weather.com/v1'),
            'weather_api_timeout': int(Variable.get('weather_api_timeout', '30')),
            'db_connection_id': Variable.get('weather_db_connection_id', 'weather_postgres'),
            'data_retention_days': int(Variable.get('data_retention_days', '30')),
            'coordinates_to_fetch': Variable.get('coordinates_to_fetch', '1,2,3,4,5').split(',')
        }
    except Exception as e:
        logger.error(f"Error getting configuration: {e}")
        raise

# Task Functions
def fetch_weather_forecast_data(**context):
    """Main task to fetch weather forecast data from API"""
    config = get_config()
    
    # Initialize API client
    api_client = WeatherAPIClient(
        api_key=config['weather_api_key'],
        base_url=config['weather_api_base_url'],
        timeout=config['weather_api_timeout']
    )
    
    # Get date range for fetching
    execution_date = context['execution_date']
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = (execution_date + timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Process each coordinate
    all_forecast_data = []
    coordinates = [int(coord.strip()) for coord in config['coordinates_to_fetch']]
    
    for coord_id in coordinates:
        try:
            logger.info(f"Fetching data for coordinate {coord_id}")
            forecast_data = api_client.get_forecast_data(coord_id, start_date, end_date)
            all_forecast_data.extend(forecast_data)
            logger.info(f"Successfully fetched {len(forecast_data)} records for coordinate {coord_id}")
            
        except Exception as e:
            logger.error(f"Failed to fetch data for coordinate {coord_id}: {e}")
            # Continue with other coordinates rather than failing the entire task
            continue
    
    # Store data in XCom for next task
    context['task_instance'].xcom_push(key='forecast_data', value=all_forecast_data)
    
    logger.info(f"Total forecast records fetched: {len(all_forecast_data)}")
    return len(all_forecast_data)

def store_forecast_data(**context):
    """Store forecast data in database"""
    config = get_config()
    
    # Get data from previous task
    forecast_data = context['task_instance'].xcom_pull(
        task_ids='fetch_weather_data', 
        key='forecast_data'
    )
    
    if not forecast_data:
        logger.warning("No forecast data to store")
        return 0
    
    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id=config['db_connection_id'])
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(forecast_data)
    
    # Prepare SQL for upsert (PostgreSQL)
    upsert_sql = """
    INSERT INTO forecasts (
        coordinate_id, date, hour, tc, rh, slp, rain,
        ws_10m, ws_925, ws_850, ws_700, ws_500, ws_200,
        wd_10m, wd_925, wd_850, wd_700, wd_500, wd_200,
        cloudlow, cloudmed, cloudhigh, cond, wdfn,
        created_at, updated_at
    ) VALUES %s
    ON CONFLICT (coordinate_id, date, hour)
    DO UPDATE SET
        tc = EXCLUDED.tc,
        rh = EXCLUDED.rh,
        slp = EXCLUDED.slp,
        rain = EXCLUDED.rain,
        ws_10m = EXCLUDED.ws_10m,
        ws_925 = EXCLUDED.ws_925,
        ws_850 = EXCLUDED.ws_850,
        ws_700 = EXCLUDED.ws_700,
        ws_500 = EXCLUDED.ws_500,
        ws_200 = EXCLUDED.ws_200,
        wd_10m = EXCLUDED.wd_10m,
        wd_925 = EXCLUDED.wd_925,
        wd_850 = EXCLUDED.wd_850,
        wd_700 = EXCLUDED.wd_700,
        wd_500 = EXCLUDED.wd_500,
        wd_200 = EXCLUDED.wd_200,
        cloudlow = EXCLUDED.cloudlow,
        cloudmed = EXCLUDED.cloudmed,
        cloudhigh = EXCLUDED.cloudhigh,
        cond = EXCLUDED.cond,
        wdfn = EXCLUDED.wdfn,
        updated_at = EXCLUDED.updated_at;
    """
    
    try:
        # Prepare data for bulk insert
        data_tuples = []
        for _, row in df.iterrows():
            data_tuple = (
                row.get('coordinate_id'),
                row.get('date'),
                row.get('hour'),
                row.get('tc'),
                row.get('rh'),
                row.get('slp'),
                row.get('rain'),
                row.get('ws_10m'),
                row.get('ws_925'),
                row.get('ws_850'),
                row.get('ws_700'),
                row.get('ws_500'),
                row.get('ws_200'),
                row.get('wd_10m'),
                row.get('wd_925'),
                row.get('wd_850'),
                row.get('wd_700'),
                row.get('wd_500'),
                row.get('wd_200'),
                row.get('cloudlow'),
                row.get('cloudmed'),
                row.get('cloudhigh'),
                row.get('cond'),
                row.get('wdfn'),
                row.get('created_at'),
                row.get('updated_at')
            )
            data_tuples.append(data_tuple)
        
        # Execute bulk insert
        from psycopg2.extras import execute_values
        
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    upsert_sql,
                    data_tuples,
                    template=None,
                    page_size=1000
                )
                conn.commit()
        
        logger.info(f"Successfully stored {len(data_tuples)} forecast records")
        return len(data_tuples)
        
    except Exception as e:
        logger.error(f"Error storing forecast data: {e}")
        raise

def cleanup_old_forecast_data(**context):
    """Clean up old forecast data based on retention policy"""
    config = get_config()
    postgres_hook = PostgresHook(postgres_conn_id=config['db_connection_id'])
    
    cleanup_sql = """
    DELETE FROM forecasts
    WHERE created_at < NOW() - INTERVAL '%s days'
    """
    
    try:
        result = postgres_hook.run(
            cleanup_sql % config['data_retention_days'],
            autocommit=True
        )
        
        # Get count of deleted records
        deleted_count_sql = """
        SELECT COUNT(*)
        FROM forecasts
        WHERE created_at < NOW() - INTERVAL '%s days'
        """ % config['data_retention_days']
        
        deleted_count = postgres_hook.get_first(deleted_count_sql)[0] if result else 0
        
        logger.info(f"Cleanup completed. Deleted {deleted_count} old forecast records")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise

# Define Tasks
fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_forecast_data,
    dag=dag
)

store_data_task = PythonOperator(
    task_id='store_forecast_data',
    python_callable=store_forecast_data,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_forecast_data,
    dag=dag
)

# Set Task Dependencies
fetch_weather_task >> store_data_task >> validate_data_task >> cleanup_task