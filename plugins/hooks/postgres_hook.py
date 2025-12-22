"""Custom Postgres hook for Weather Forecast ETL."""

import logging
from typing import Any

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor, execute_batch

logger = logging.getLogger(__name__)


class ForecastPostgresHook(PostgresHook):
    """Extended Postgres hook providing convenience methods.

    Used for reading coordinate data and inserting forecast results.
    """

    def load_locations(self, sql: str) -> list[dict[str, Any]]:
        """Query the list of coordinates from Postgres.

        Args:
            sql: SQL query to retrieve locations.

        Returns:
            A list of dictionaries with normalized lat/lon strings.
        """
        with self.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            d = {str(k): r[k] for k in r.keys()}
            # Skip invalid rows
            if d.get("latitude") is None or d.get("longitude") is None:
                continue
            # Normalize lat/lon as string for API parameters
            d["lat"], d["lon"] = str(d["latitude"]), str(d["longitude"])
            out.append(d)

        logger.info("Loaded %s coordinate rows from Postgres", len(out))
        return out

    def insert_forecast(
        self,
        table: str,
        rows: list[dict[str, Any]],
        on_conflict: str = "",
        page_size: int = 2000,
        column_name: list[str] | None = None,
        column_value: str="",
    ) -> int:
        """Insert weather forecast data into a target table.

        Args:
            table: The name of the target table.
            rows: List of dictionaries representing forecast rows.
            on_conflict: Optional ON CONFLICT clause.
            page_size: Batch size for psycopg2 execute_batch().
            column_name: List of column names to insert data into.
            column_value: String of placeholders for the values.

        Returns:
            Number of inserted rows.
        """
        if not rows:
            logger.info("No rows to insert into %s", table)
            return 0

        if column_name is None:
            column_name = []

        params = [{c: r.get(c) for c in column_name} for r in rows]

        conflict_clause = f" ON CONFLICT {on_conflict}" if on_conflict else ""

        sql = f"""
        INSERT INTO {table} ({", ".join(column_name)})
        VALUES (
            {column_value}
        ){conflict_clause};
        """
        logger.info("SQL to execute: %s", sql)

        with self.get_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, sql, params, page_size=page_size)
            conn.commit()

        logger.info("Inserted %s rows into %s", len(params), table)
        return len(params)

    def get_latest_id(self, table: str) -> int:
        """Get the latest ID from a table.

        Args:
            table: The name of the table.

        Returns:
            The latest ID value.
        """
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(id) FROM {table}")
                result = cur.fetchone()
                return result[0] if result[0] is not None else 0

    def upsert_table(self,
        table: str,
        df: pd.DataFrame,
        conflict_key: list[str] = [],
        page_size: int = 2000,
        column_name: list[str] = [],
    ) -> int:
        """Insert data into a target table.

        Args:
            table: The name of the target table.
            df: DataFrame representing rows.
            conflict_key: List of column names to use as conflict key.
            page_size: Batch size for psycopg2 execute_batch().

        Returns:
            Number of inserted rows.
        """
        if df.empty:
            logger.info("No rows to insert into %s", table)
            return 0

        params = [{c: r.get(c) for c in column_name} for r in df.values.tolist()]
        if conflict_key:
            conflict_clause = f" ON CONFLICT ({", ".join(conflict_key)}) DO UPDATE SET {', '.join([f'{c} = EXCLUDED.{c}' for c in column_name])}"
        else:
            conflict_clause = ""
        
        column_value = df.values.tolist()

        sql = f"""
        INSERT INTO {table} ({", ".join(column_name)})
        VALUES (
            {column_value}
        ){conflict_clause};
        """
        logger.info("SQL to execute: %s", sql)

        with self.get_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, sql, params, page_size=page_size)
            conn.commit()

        logger.info("Inserted %s rows into %s", len(params), table)
        return len(params)

    def check_exist_table(self, table: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table: The name of the table.

        Returns:
            True if the table exists, False otherwise.
        """
        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table}'")
                result = cur.fetchone()
                return result is not None
