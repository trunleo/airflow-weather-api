import logging

import pandas as pd
from trino.auth import BasicAuthentication
from trino.dbapi import connect

logger = logging.getLogger(__name__)


class TrinoHook:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        catalog: str,
        schema: str,
        request_timeout: float = 300.0,
    ) -> None:
        self.conn = connect(
            host=host,
            port=port,
            user=user,
            auth=BasicAuthentication(user, password),
            catalog=catalog,
            schema=schema,
            request_timeout=request_timeout,
        )
        self.schema = schema
        self.catalog = catalog
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def get_conn(self) -> connect:
        return self.conn

    def check_connect(self):
        try:
            self.conn.cursor()
            logger.info("Connect successfully!")
            return True
        except Exception as e:
            logger.error("Failed to connect to Trino: %s", e)
            return False

    def run(self, sql: str) -> pd.DataFrame:
        """Executes the given SQL query and returns the results as a pandas DataFrame."""
        df = pd.read_sql_query(sql, self.conn)
        return df

    def get_table(self, table_name: str, **context) -> pd.DataFrame:
        """Retrieves the specified table from the database and returns it as a pandas DataFrame."""
        if context['condition']:
            return self.run(f"SELECT * FROM {self.schema}.{table_name} WHERE {context['condition']}")
        return self.run(f"SELECT * FROM {self.schema}.{table_name}")

    def insert_table(self, table_name: str, df: pd.DataFrame) -> None:
        """Inserts the specified DataFrame into the specified table in the database."""
        df.to_sql(table_name, self.conn, if_exists="replace", index=False)
