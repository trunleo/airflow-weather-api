import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from hooks.postgres_hook import ForecastPostgresHook
from hooks.trino_hook import TrinoHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_CONN_ID_OUT = Variable.get(
    "POSTGRES_CONN_ID_OUT",
    default_var=Variable.get("POSTGRES_CONN_ID", default_var="marketprice-pg"),
)
POSTGRES_CONN_ID_IN = Variable.get(
    "POSTGRES_CONN_ID_IN", default_var=POSTGRES_CONN_ID_OUT
)

conn = TrinoHook(
    host=Variable.get("TRINO_HOST", default_var=""),
    port=Variable.get("TRINO_PORT", default_var=443),
    user=Variable.get("TRINO_USER", default_var=""),
    password=Variable.get("TRINO_PASSWORD", default_var=""),
    catalog=Variable.get("TRINO_CATALOG", default_var=""),
    schema=Variable.get("TRINO_SCHEMA", default_var=""),
)

pg_hook_out = ForecastPostgresHook(postgres_conn_id=POSTGRES_CONN_ID_OUT)


def check_trino_connection():
    if not conn.check_connect():
        raise AirflowSkipException("Failed to connect to Trino")


def fetch_trino_table(table_name: str = "", conflict_key: list = ["id"], **context):
    conditions = f"price_date >= {context['start_date']} AND price_date <= {context['end_date']}" if "start_date" in context and "end_date" in context else ""
    if table_name:
        df = conn.get_table(table_name, condition=conditions)
        count = pg_hook_out.upsert_table(
            table=table_name,
            df=df,
            conflict_key=conflict_key,
            column_name=df.columns.tolist(),
        )
        logger.info("Inserted %s rows into %s", count, table_name)
        return count


def fetch_dim_tables(**context):
    logger.info("Fetching dim tables")
    # Get list of dim tables from context
    start_date = context.get("start_date")
    end_date = context.get("end_date")
    logger.info("Fech data from %s to %s", start_date, end_date)
    if "dim_tables" in context:
        list_dim_tables = [
            tbl.strip() for tbl in context["dim_tables"].split(",") if tbl.strip()
        ]
    else:
        list_dim_tables = [
            "dim_categories",
            "dim_countries",
            "dim_date",
            "dim_products",
            "dim_units",
        ]
    for table_name in list_dim_tables:
        fetch_trino_table(table_name, conflict_key=["id"], start_date=start_date, end_date=end_date)
    logger.info("Inserted %s rows into %s", count, table_name)


def fetch_fact_tables(**context):
    logger.info("Fetching fact tables")
    # Get list of fact tables from context
    start_date = context.get("start_date")
    end_date = context.get("end_date")
    logger.info("Fech data from %s to %s", start_date, end_date)
    if "fact_tables" in context:
        list_fact_tables = [
            tbl.strip() for tbl in context["fact_tables"].split(",") if tbl.strip()
        ]
    else:
        list_fact_tables = ["fact_daily_prices"]
    for table_name in list_fact_tables:
        fetch_trino_table(table_name, conflict_key=["price_date", "product_id"], start_date=start_date, end_date=end_date)
    logger.info("Inserted %s rows into %s", count, table_name)
