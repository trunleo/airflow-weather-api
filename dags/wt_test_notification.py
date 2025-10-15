"""Weather daily pipeline DAG."""

from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# Functions ETL
from weather.current import etl_weather_current
from weather.forecast import etl_weather_forecast
from weather.webhook import send_notification_webhook
from weather.references_tbl import create_postgres_connection, get_reference_table

default_args = {
    "owner": "trung.tran@vnsilicon.net,khai.do@vnsilicon.net",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
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

    # with TaskGroup(
    #     "weather_etl", tooltip="Extract weather data"
    # ) as weather_etl:
    #     forecast_task = PythonOperator(
    #         task_id="forecast_weather",
    #         python_callable=etl_weather_forecast,
    #         op_kwargs={"run_date": "{{ next_ds }}"},
    #     )

    #     current_task = PythonOperator(
    #         task_id="current_weather",
    #         python_callable=etl_weather_current,
    #         op_kwargs={"run_date": "{{ next_ds }}"},
    #     )

    # trigger_notify = PythonOperator(
    #     task_id="trigger_notify",
    #     python_callable=send_notification_webhook,
    #     op_kwargs={
    #         "run_date": "{{ next_ds }}",
    #         "webhook_url": Variable.get("WEATHER_WEBHOOK_URL", default_var=""),
    #         "postgres_conn_id": "WEATHER_POSTGRES_CONN",
    #         "webhook_api_key": Variable.get(
    #             "WEATHER_WEBHOOK_API_KEY", default_var=None
    #         ),
    #         "webhook_secret": Variable.get(
    #             "WEATHER_WEBHOOK_SECRET", default_var=None
    #         ),
    #     },
    # )
    create_conn = PythonOperator(
        task_id="create_connection",
        python_callable=create_postgres_connection,
        op_kwargs={
            "conn_id": Variable.get("WEATHER_POSTGRES_CONN", default_var="WEATHER_POSTGRES_CONN"),
        },
    )
    
    get_ref_tbl = PythonOperator(
        task_id="get_reference_table",
        python_callable=get_reference_table,
        op_kwargs={
            "db_manager": "{{ ti.xcom_pull(task_ids='create_connection') }}",
        },
    )

    # Trigger notify webhook
    # weather_etl >> trigger_notify
    create_conn >> get_ref_tbl
