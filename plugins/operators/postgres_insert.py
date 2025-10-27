from __future__ import annotations
from typing import Any, Dict, Iterable, List, Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch

class DbInsertOperator(BaseOperator):
    """
    Generic batch INSERT for Postgres.

    :param postgres_conn_id: Airflow Postgres connection id
    :param table: target table (schema-qualified)
    :param mapping: dict[column_name -> key_in_record]
    :param records: list of record dicts (templated via XCom / .expand())
    :param page_size: batch size for execute_batch
    :param on_conflict: e.g. "DO NOTHING" or ' (col1, col2) DO UPDATE SET ...'
                        Pass None to omit ON CONFLICT clause.
    """
    template_fields: Sequence[str] = ("records",)

    def __init__(
        self,
        postgres_conn_id: str,
        table: str,
        mapping: Dict[str, str],
        records: List[Dict[str, Any]] | None = None,
        page_size: int = 1000,
        on_conflict: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.mapping = mapping
        self.records = records or []
        self.page_size = page_size
        self.on_conflict = on_conflict

    def execute(self, context: Context) -> int:
        if not self.records:
            self.log.info("No records to insert.")
            return 0

        cols = list(self.mapping.keys())
        params = [{c: r.get(self.mapping[c]) for c in cols} for r in self.records]

        placeholders = ", ".join([f"%({c})s" for c in cols])
        col_list = ", ".join(cols)
        conflict = f" ON CONFLICT {self.on_conflict}" if self.on_conflict else ""

        sql = f"""
        INSERT INTO {self.table} ({col_list})
        VALUES ({placeholders}){conflict};
        """

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, sql, params, page_size=self.page_size)
            conn.commit()
        self.log.info("Inserted %s rows into %s", len(params), self.table)
        return len(params)