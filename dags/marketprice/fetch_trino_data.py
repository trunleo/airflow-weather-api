import logging

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from hooks.postgres_hook import ForecastPostgresHook
from hooks.trino_hook import TrinoHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


conn = TrinoHook(
    host=str(Variable.get("TRINO_HOST", default_var="")),
    port=int(Variable.get("TRINO_PORT", default_var=443)),
    user=str(Variable.get("TRINO_USER", default_var="")),
    password=str(Variable.get("TRINO_PASSWORD", default_var="")),
    catalog=str(Variable.get("TRINO_CATALOG", default_var=""))
    )

pg_hook_out = ForecastPostgresHook(postgres_conn_id="weather_db_connection_id")


def check_trino_connection():
    if not conn.check_connect():
        raise AirflowSkipException("Failed to connect to Trino")
    logger.info("Connected to Trino")

def check_pg_connection():
    if not pg_hook_out.check_connection():
        raise AirflowSkipException("Failed to connect to Postgres")
    logger.info("Connected to Postgres")
    


def fetch_trino_table(table_name: str = "", conflict_key: list = ["id"], **context):
    conditions = f"price_date >= DATE '{context['start_date']}' AND price_date <= DATE '{context['end_date']}'" if "start_date" in context and "end_date" in context else ""
    schema = context.get("schema", "")
    if table_name:
        df = conn.get_table(table_name, condition=conditions, schema = schema)
        count = pg_hook_out.upsert_table(
            table=table_name,
            df=df,
            conflict_key=conflict_key,
            column_name=df.columns.tolist(),
        )
        logger.info("Inserted %s rows into %s", count, table_name)
        return count


def fetch_dim_tables(schema = "dp_silver", **context):
    skip_dim_tables = context.get("skip_dim_tables", "True")
    if str(skip_dim_tables).lower() == "true":
        logger.info("Skipping dim tables fetching")
        return

    logger.info("Fetching dim tables")
    # Get list of dim tables from context
    # start_date = context.get("start_date")
    # end_date = context.get("end_date")
    if "dim_tables" in context:
        list_dim_tables = [
            tbl.strip() for tbl in context["dim_tables"].split(",") if tbl.strip()
        ]
    else:
        list_dim_tables = [
            "dim_categories",
            "dim_countries",
            "dim_products",
            "dim_units",
        ]
    for table_name in list_dim_tables:
        logger.info("Fetching %s", table_name)
        # count = fetch_trino_table(table_name, conflict_key=["id"], start_date=start_date, end_date=end_date)
        count = fetch_trino_table(table_name, conflict_key=["id"], schema = schema)
        logger.info("Inserted %s rows into %s", count, table_name)


def fetch_fact_tables(schema = "dp_silver", **context):
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
        logger.info("Fetching %s", table_name)
        count = fetch_trino_table(table_name, conflict_key=["price_date", "product_id"], start_date=start_date, end_date=end_date, schema = schema)
        logger.info("Inserted %s rows into %s", count, table_name)


def fetch_gold_tables(schema = "dp_gold", **context):
    logger.info("Fetching gold tables")
    # Get list of gold tables from context
    start_date = context.get("start_date")
    end_date = context.get("end_date")
    logger.info("Fech data from %s to %s", start_date, end_date)
    if "gold_tables" in context:
        list_gold_tables = [
            tbl.strip() for tbl in context["gold_tables"].split(",") if tbl.strip()
        ]
    else:
        list_gold_tables = ["fact_daily_prices"]
    for table_name in list_gold_tables:
        logger.info("Fetching %s", table_name)
        count = fetch_trino_table(table_name, conflict_key=["date_time", "product_id"], start_date=start_date, end_date=end_date, schema = schema)
        logger.info("Inserted %s rows into %s", count, table_name)