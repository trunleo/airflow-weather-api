"""Custom Postgres hook for Weather Forecast ETL."""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor, execute_batch
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class ForecastPostgresHook(PostgresHook):
    """
    Extended Postgres hook providing convenience methods
    for reading coordinate data and inserting forecast results.
    """

    def load_locations(self, sql: str) -> List[Dict[str, Any]]:
        """
        Query the list of coordinates from Postgres.

        Args:
            sql: SQL query to retrieve locations (must include latitude & longitude columns).

        Returns:
            A list of dictionaries with normalized lat/lon strings.
        """
        with self.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
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
        rows: List[Dict[str, Any]],
        on_conflict: str = "",
        page_size: int = 2000,
        column_name: List[str] = [],
        column_value: str="",
    ) -> int:
        """
        Insert weather forecast data into a target table.

        Args:
            table: The name of the target table.
            rows: List of dictionaries representing forecast rows.
            on_conflict: Optional ON CONFLICT clause.
            page_size: Batch size for psycopg2 execute_batch().

        Returns:
            Number of inserted rows.
        """
        if not rows:
            logger.info("No rows to insert into %s", table)
            return 0

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
