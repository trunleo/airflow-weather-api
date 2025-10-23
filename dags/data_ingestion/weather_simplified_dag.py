from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.operators.weather_operator import WeatherIngestionOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_simplified_ingestion',
    default_args=default_args,
    description='Simplified weather data ingestion using custom operators',
    schedule_interval='0 */6 * * *',
    catchup=False,
    tags=['weather', 'ingestion', 'simplified']
)

# Dynamic task creation for multiple coordinates
coordinate_ids = [1, 2, 3, 4, 5]  # Could be loaded from config

tasks = []
for coord_id in coordinate_ids:
    task = WeatherIngestionOperator(
        task_id=f'ingest_weather_coord_{coord_id}',
        coordinate_id=coord_id,
        dag=dag
    )
    tasks.append(task)

# Validation task
def validate_all_data(**context):
    """Validate all ingested data"""
    # Implementation here
    pass

validation_task = PythonOperator(
    task_id='validate_all_data',
    python_callable=validate_all_data,
    dag=dag
)

# Set dependencies - all ingestion tasks must complete before validation
for task in tasks:
    task >> validation_task