import logging
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from hooks.postgres_hook import ForecastPostgresHook
from hooks.trino_hook import TrinoHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


pg_hook_out = ForecastPostgresHook(postgres_conn_id="marketprice-pg")


def check_pg_connection():
    if not pg_hook_out.check_connection():
        raise AirflowSkipException("Failed to connect to Postgres")
    logger.info("Connected to Postgres")
    


def get_table(table_name: str = "", **context):
    if "date_col" in context:
        conditions = f"{context['date_col']} >= DATE '{context['start_date']}' AND {context['date_col']} <= DATE '{context['end_date']}'" if "start_date" in context and "end_date" in context else ""
    else:
        conditions = f"price_date >= DATE '{context['start_date']}' AND price_date <= DATE '{context['end_date']}'" if "start_date" in context and "end_date" in context else ""
    schema = context.get("schema", "")
    df = pg_hook_out.get_table(table_name, condition=conditions, schema=schema)
    return df

def upsert_table(df, table_name: str = "", conflict_key: list = ["id"], **context) -> int :
    count = pg_hook_out.upsert_table(
        table=table_name,
            df=df,
            conflict_key=conflict_key,
            column_name=df.columns.tolist(),
        )
    logger.info("Inserted %s rows into %s", count, table_name)
    logger.info("First 10 rows in table: %s", df.head(10))
    return count


def transform_product_tbl(**context):
    mapping_df = get_table("mapping_list", schema="public")
    logger.info("First 10 rows of mapping table: %s", mapping_df.head(10))

    
    daily_product_prices_df = get_table("daily_product_prices", schema="public")
    re_col_list = ['category_name', 'price_type', 'product_id', 'product_name']
    product_df = daily_product_prices_df[re_col_list].drop_duplicates()

    product_df.rename(
        columns={
            "category_name": "group_name",
            "price_type": "category_name",
            "product_id": "id",
            "product_name": "name_th"
        }, inplace=True
    )

    product_df["name_en"] = product_df["name_th"].map(mapping_df.set_index("NAME_TH")["NAME_EN"].to_dict())
    product_df["group_name_en"] = product_df["group_name"].map(mapping_df.set_index("GROUP_TYPE_TH")["GROUP_TYPE_EN"].to_dict())
    product_df["category_name_en"] = product_df["category_name"].map(mapping_df.set_index("CATEGORY_TH")["CATEGORY_EN"].to_dict())
    product_df["created_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    product_df["updated_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    product_df["product_desc_en"] = None
    product_df["product_desc_th"] = None
    
    logger.info("First 10 rows of product table: %s", product_df.head(10))
    try:
        count = upsert_table(product_df, "products", ["id"])
        logger.info("Inserted %s rows into products", count)
    except Exception as e:
        logger.error("Failed to insert into products: %s", e)
        raise
    return count
    
# def transform_product_prices(**context):    

# def fetch_dim_tables(**context):
#     skip_dim_tables = context.get("skip_dim_tables", "True")
#     if str(skip_dim_tables).lower() == "true":
#         logger.info("Skipping dim tables fetching")
#         return

#     logger.info("Fetching dim tables")
#     # Get list of dim tables from context
#     # start_date = context.get("start_date")
#     # end_date = context.get("end_date")
#     if "dim_tables" in context:
#         list_dim_tables = [
#             tbl.strip() for tbl in context["dim_tables"].split(",") if tbl.strip()
#         ]
#     else:
#         list_dim_tables = [
#             "dim_categories",
#             "dim_countries",
#             "dim_products",
#             "dim_units",
#         ]
#     for table_name in list_dim_tables:
#         logger.info("Fetching %s", table_name)
#         # count = fetch_trino_table(table_name, conflict_key=["id"], start_date=start_date, end_date=end_date)
#         count = fetch_trino_table(table_name, conflict_key=["id"])
#         logger.info("Inserted %s rows into %s", count, table_name)


# def fetch_fact_tables(**context):
#     logger.info("Fetching fact tables")
#     # Get list of fact tables from context
#     start_date = context.get("start_date")
#     end_date = context.get("end_date")
#     logger.info("Fech data from %s to %s", start_date, end_date)
#     if "fact_tables" in context:
#         list_fact_tables = [
#             tbl.strip() for tbl in context["fact_tables"].split(",") if tbl.strip()
#         ]
#     else:
#         list_fact_tables = ["fact_daily_prices"]
#     for table_name in list_fact_tables:
#         logger.info("Fetching %s", table_name)
#         count = fetch_trino_table(table_name, conflict_key=["price_date", "product_id"], start_date=start_date, end_date=end_date)
#         logger.info("Inserted %s rows into %s", count, table_name)
