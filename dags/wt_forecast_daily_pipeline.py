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

default_args = {
    "owner": "trung.tran@vnsilicon.net,khai.do@vnsilicon.net",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 10, 23),
    "catchup": False,
    "schedule_interval": "30 7 * * *",
    "pool": "data-engineer"
}

with DAG(
    "wt_forecast_daily_pipeline",
    description="Weather forecast pipeline",
    default_args=default_args,
    tags=["weather", "data-engineer"],
) as dag:

    with TaskGroup(
        "weather_etl", tooltip="Extract weather data"
    ) as weather_etl:
        forecast_task = PythonOperator(
            task_id="forecast_weather",
            python_callable=etl_weather_forecast,
            op_kwargs={"run_date": "{{ next_ds }}"},
        )

    # trigger_notify = PythonOperator(
    #     task_id="trigger_notify",
    #     python_callable=send_notification_webhook,
    #     op_kwargs={
    #         "run_date": "{{ next_ds }}",
    #         "webhook_url": Variable.get("WEATHER_WEBHOOK_URL"),
    #         "postgres_conn_id": "WEATHER_POSTGRES_CONN",
    #         "weather_webkook_key": Variable.get(
    #             "WEATHER_WEBHOOK_KEY", default_var=None
    #         ),
    #         "webhook_secret": Variable.get(
    #             "WEATHER_WEBHOOK_SECRET", default_var=None
    #         ),
    #     },
    # )

    # Trigger notify webhook
    weather_etl 
    # >> trigger_notify