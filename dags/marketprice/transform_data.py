import logging
import os
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from hooks.postgres_hook import ForecastPostgresHook
from hooks.trino_hook import TrinoHook
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


pg_hook_out = ForecastPostgresHook(postgres_conn_id="marketprice-pg")
dag_path = os.path.dirname(__file__)

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

    
    daily_product_prices_df = get_table(
        "daily_product_prices", 
        schema="public", 
        date_col="date_time",
        start_date=context.get("start_date"),
        end_date=context.get("end_date")
        )
    logger.info("First 10 rows of daily_product_prices table: %s", daily_product_prices_df.head(10))
    
    re_col_list = ['category_name', 'price_type', 'product_id', 'product_name']
    product_df = daily_product_prices_df[re_col_list].drop_duplicates()

    logger.info("First 10 rows of product table: %s", product_df.head(10))

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

def transform_product_prices_tbl(**context):
    mapping_df = get_table("mapping_list", schema="public")
    logger.info("First 10 rows of mapping table: %s", mapping_df.head(10))
    mapping_df['UNIT_TH_SHORT'] = mapping_df['UNIT_TH'].str.split('/').str[1].str.strip()
    mapping_df['UNIT_TH_SHORT'] = mapping_df['UNIT_TH_SHORT'].str.replace('.', '')

    
    daily_product_prices_df = get_table(
        "daily_product_prices", 
        schema="public", 
        date_col="date_time",
        start_date=context.get("start_date"),
        end_date=context.get("end_date")
        )
    logger.info("First 10 rows of daily_product_prices table: %s", daily_product_prices_df.head(10))
    
    re_col_list = [
        'date_time',
        'product_id',
        'max_price',
        'min_price',
        'unit_name',
        'currency_code',
        'unit_name_en'
    ]
    product_price_df = daily_product_prices_df[re_col_list].drop_duplicates()

    logger.info("First 10 rows of product table: %s", product_price_df.head(10))

    product_price_df.rename(
        columns={
            "date_time": "date",
            "product_id": "product_id",
            "max_price": "price_max",
            "min_price": "price_min",
            "unit_name": "unit_th"
        }, inplace=True
    )

    product_price_df["unit"] = product_price_df["unit_th"].map(mapping_df.set_index("UNIT_TH_SHORT")["UNIT_EN"].to_dict())
    for i, row in product_price_df[product_price_df["unit"].isnull()].iterrows():
        row["unit"] = f"{row["currency_code"]}/{row["unit_name_en"]}"


    product_price_df["created_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    product_price_df["updated_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    product_price_df["date"] = product_price_df["date"].apply(lambda x: datetime.strftime(x, "%Y-%m-%d %H:%M:%S"))
    product_price_df.drop(
        columns=["unit_name_en", "currency_code"],
        inplace=True
    )
    # Get latest id
    latest_id = pg_hook_out.get_latest_id("products_price")
    logger.info("Latest ID from database: %s", latest_id)
    product_price_df["id"] = range(latest_id + 1, latest_id + len(product_price_df) + 1)

    logger.info("First 10 rows of product prices table: %s", product_price_df.head(10))
    try:
        count = upsert_table(product_price_df, "products_price", ["product_id", "date"])
        logger.info("Inserted %s rows into products price", count)
    except Exception as e:
        logger.error("Failed to insert into products price: %s", e)
        raise
    return count
    
def check_existing_mapping_list(**context):
    if pg_hook_out.check_exist_table("mapping_list") == False:
        sql_path = os.path.join(dag_path, "sql", "mapping_list.sql")
        

        logger.info("Mapping list table does not exist")
        logger.info("Create mapping list table")

        with open(sql_path, "r") as f:
            sql = f.read()
            pg_hook_out.run(sql)
        
        logger.info("Created mapping list table")
    else:
        logger.info("Mapping list table exists")

def insert_mapping_data(**context):
    # Load mapping data to mapping table
    csv_path = os.path.join(dag_path, "data", "mapping_list.csv")
    mapping_df = pd.read_csv(csv_path)
    logger.info("First 10 rows of mapping list: %s", mapping_df.head(10))
    try:
        count = upsert_table(
            mapping_df, 
            "mapping_list", 
            conflict_key=["ID"],
            column_name=mapping_df.columns.tolist()
            )
        logger.info("Loaded mapping data to mapping table")
    except Exception as e:
        logger.error("Failed to load mapping data to mapping table: %s", e)
        raise