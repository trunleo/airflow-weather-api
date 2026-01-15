"""Weather daily pipeline DAG."""

from datetime import datetime, timedelta

from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Functions ETL
from marketprice.fetch_trino_data import (
    check_pg_connection,
    check_trino_connection,
    fetch_dim_tables,
    fetch_fact_tables,
    fetch_gold_tables
)

from marketprice.transform_data import (
    transform_product_tbl,
    transform_product_prices_tbl
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
        default="dim_categories, dim_products, dim_units",
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
    "gold_tables": Param(
        default="daily_product_prices",
        type="string",
        description="The base URL for the alert service API",
    ),
    "skip_check_connection": Param(
        default=True,
        type="boolean",
        description="Skip check connection",
    ),
    "skip_dim_tables": Param(
        default=True,
        type="boolean",
        description="Skip fetching dimension tables",
    ),
    "skip_fact_tables": Param(
        default=True,
        type="boolean",
        description="Skip fetching fact tables",
    ),
    "skip_gold_tables": Param(
        default=True,
        type="boolean",
        description="Skip fetching gold tables",
    )
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
        op_kwargs={"skip_check_connection": "{{ params.skip_check_connection }}"}
    )

    check_pg_connection_task = PythonOperator(
        task_id="check_pg_connection",
        python_callable=check_pg_connection,
        op_kwargs={"skip_check_connection": "{{ params.skip_check_connection }}"}
    )

    with TaskGroup(
        "fetch_trino_tables", tooltip="Fetch Trino tables"
    ) as fetch_trino_tables:
        fetch_dim_tables_task = PythonOperator(
            task_id="fetch_dim_tables",
            python_callable=fetch_dim_tables,
            op_kwargs={"run_date": "{{ next_ds }}","dim_tables":"{{ params.dim_tables }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}", "skip_dim_tables": "{{ params.skip_dim_tables }}"},
        )
        fetch_fact_tables_task = PythonOperator(
            task_id="fetch_fact_tables",
            python_callable=fetch_fact_tables,
            op_kwargs={"run_date": "{{ next_ds }}","fact_tables":"{{ params.fact_tables }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}", "skip_fact_tables": "{{ params.skip_fact_tables }}"},
        )

        fetch_gold_tables_task = PythonOperator(
            task_id="fetch_gold_tables",
            python_callable=fetch_gold_tables,
            op_kwargs={"run_date": "{{ next_ds }}","gold_tables":"{{ params.gold_tables }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}", "skip_gold_tables": "{{ params.skip_gold_tables }}"},
        )
    
    with TaskGroup(
        "transform_data", tooltip="Transform data based on defined schema"
    ) as transform_data:
        transform_product_data = PythonOperator(
            task_id="transform_product_data",
            python_callable=transform_product_tbl,
            op_kwargs={"run_date": "{{ next_ds }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}"},
        )
        transform_product_prices_data = PythonOperator(
            task_id="transform_product_prices_data",
            python_callable=transform_product_prices_tbl,
            op_kwargs={"run_date": "{{ next_ds }}", "start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}"},
        )


    fetch_dim_tables_task >> fetch_fact_tables_task >> fetch_gold_tables_task
    [check_trino_connection_task, check_pg_connection_task] >> fetch_trino_tables >> transform_data