"""Weather daily pipeline DAG."""

from airflow.models.param import Param
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# Functions ETL
from weather.current import etl_weather_current
from weather.forecast import etl_weather_forecast
from weather.alert_service import send_weather_alert

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

default_params = {
    "event_api": Param(
        default="weather/internal/webhooks/weather-notification",
        type="string",
        description="The base URL for the alert service API",
    )
}

with DAG(
    "wt_forecast_notification_test",
    description="Weather forecast notification test pipeline",
    default_args=default_args,
    tags=["weather", "data-engineer"],
    params=default_params
) as dag:

    with TaskGroup(
        "weather_etl", tooltip="Extract weather data"
    ) as weather_etl:
        trigger_hook = PythonOperator(
            task_id="trigger_hook",
            python_callable=send_weather_alert,
            op_args=["{{ params.event_api }}"],
        )
    
    
