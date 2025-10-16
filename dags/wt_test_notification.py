"""Weather daily pipeline DAG."""

from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from include.weather.core.etl.references_tbl import create_connection, get_reference_table
# Functions ETL

# Create the database connection using the create_connection function
db_manager = create_connection(postgres_conn_id="ac-weather-backend")

default_args = {
    "owner": "trung.tran@vnsilicon.net,khai.do@vnsilicon.net",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 10, 12),
    "catchup": False,
    "schedule_interval": "0 0 * * *",
    "pool": "data-engineer", # Add pool to Airflow if not exist
}

with DAG(
    "wt_test_daily_pipeline",
    description="Test Weather daily pipeline",
    default_args=default_args,
    tags=["weather", "data-engineer"],
) as dag:
    
    # TaskGroup to group ETL tasks
    with TaskGroup("load_ref", tooltip="Load Reference tbl") as load_ref:
        get_ref_tbl = PythonOperator(
        task_id="get_reference_table",
        python_callable=get_reference_table,
        op_kwargs={
            "db_manager": db_manager,
            "ref_tbl_name": "coordinate",
            },
        )
        
        get_ref_tbl = PythonOperator(
            task_id="get_reference_table_station",
            python_callable=get_reference_table,
            op_kwargs={
                "db_manager": db_manager,
                "ref_tbl_name": "station",
            },
        )
        
        # Additional ETL tasks can be added here in the future
    # Task to trigger notification webhook (placeholder)
    # trigger_notify = PythonOperator(
    #     task_id="trigger_notification_webhook",
    #     python_callable=lambda: print("Triggering notification webhook..."),
    # )

    
    # Trigger notify webhook
    # weather_etl >> trigger_notify
    # create_conn >> get_ref_tbl
