"""Weather daily pipeline DAG."""

from datetime import datetime, timedelta

from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Functions ETL
from marketprice.fetch_trino_data import (
    check_trino_connection,
    fetch_dim_tables,
    fetch_fact_tables,
)

from airflow import DAG

start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
default_args = {
    "owner": "trung.tran@vnsilicon.net,khai.do@vnsilicon.net",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "start_date": start_date,
    "schedule_interval": "30 7 * * *"
}

default_params = {
    "dim_tables": Param(
        default="dim_categories, dim_countries, dim_date, dim_products, dim_units",
        type="string",
        description="The base URL for the alert service API",
    ),
    "fact_tables": Param(
        default="fact_daily_prices",
        type="string",
        description="The base URL for the alert service API",
    ),
    "start_date": Param(
        default=start_date,
        type="string",
        description="The base URL for the alert service API",
    ),
    "end_date": Param(
        default=start_date,
        type="string",
        description="The base URL for the alert service API",
    ),
}

with DAG(
    "mkp_all_trino_tables_pipeline",
    description="Marketprice all trino tables pipeline",
    default_args=default_args,
    params=default_params,
    tags=["marketprice", "data-engineer"],
) as dag:

    check_trino_connection_task = PythonOperator(
        task_id="check_trino_connection",
        python_callable=check_trino_connection,
    )

    with TaskGroup(
        "fetch_trino_tables", tooltip="Fetch Trino tables"
    ) as fetch_trino_tables:
        fetch_dim_tables_task = PythonOperator(
            task_id="fetch_dim_tables",
            python_callable=fetch_dim_tables,
            op_kwargs={"run_date": "{{ next_ds }}","dim_tables":"{{ params.dim_tables }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}"},
        )
        fetch_fact_tables_task = PythonOperator(
            task_id="fetch_fact_tables",
            python_callable=fetch_fact_tables,
            op_kwargs={"run_date": "{{ next_ds }}","fact_tables":"{{ params.fact_tables }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}"},
        )
    
    check_trino_connection_task >> fetch_dim_tables_task >> fetch_fact_tables_task