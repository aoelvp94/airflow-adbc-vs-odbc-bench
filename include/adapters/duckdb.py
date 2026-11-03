"""Native DuckDB Arrow adapter (``to_arrow_reader`` / ``from_arrow``).

Complements ``AdbcArrowCapabilityAdapter``: ``duckdb_adbc`` already goes through
AdbcHook + the ADBC DuckDB driver. This class talks to the ``duckdb`` Python
API directly so the same RecordBatch protocols can be advertised without ADBC.

Native DuckDB connections are not safe for a concurrent read stream plus write
on the same file/connection, so ``get_record_batches`` snapshots the reader
into a list before yielding. The ADBC adapter (two connections) is the true
streaming path.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import pyarrow as pa

from utils.local_engines import duckdb_path
from utils.verify_load import sql_ident


class DuckDBArrowCapabilityAdapter:
    """RecordBatchReadable + RecordBatchWritable over a DuckDB file or connection."""

    def __init__(
        self,
        path: str | None = None,
        *,
        connection: Any | None = None,
    ):
        self._path = path
        self._connection = connection

    def _connect(self) -> tuple[Any, bool]:
        if self._connection is not None:
            return self._connection, False
        import duckdb

        return duckdb.connect(self._path or duckdb_path()), True

    def get_record_batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        con, owned = self._connect()
        try:
            # DuckDB 2.0: execute() leaves a streaming result; sql() returns a relation.
            rel = con.sql(sql) if hasattr(con, "sql") else con.execute(sql)
            if hasattr(rel, "to_arrow_reader"):
                reader = rel.to_arrow_reader()
            elif hasattr(rel, "fetch_record_batch"):
                reader = rel.fetch_record_batch()
            else:
                table = (
                    rel.to_arrow_table()
                    if hasattr(rel, "to_arrow_table")
                    else rel.fetch_arrow_table()
                )
                reader = table.to_reader()
            # Snapshot: dest may reuse this connection/file for from_arrow.
            yield from list(reader)
        finally:
            if owned:
                con.close()

    def insert_record_batches(
        self,
        table: str,
        batches: Iterator[pa.RecordBatch],
        **kwargs: Any,
    ) -> None:
        ident = sql_ident(table)
        columns = kwargs.get("target_fields")
        con, owned = self._connect()
        try:
            for batch in batches:
                if batch is None or batch.num_rows == 0:
                    continue
                if columns:
                    missing = [c for c in columns if c not in batch.schema.names]
                    if missing:
                        raise ValueError(
                            f"DuckDB Arrow batch missing columns {missing}; "
                            f"have {batch.schema.names}"
                        )
                    batch = batch.select(list(columns))
                con.from_arrow(batch).insert_into(ident)
        finally:
            if owned:
                con.close()

    def get_records(self, sql: str):
        con, owned = self._connect()
        try:
            return con.execute(sql).fetchall()
        finally:
            if owned:
                con.close()

    def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        **kwargs: Any,
    ) -> None:
        ident = sql_ident(table)
        fields = kwargs.get("target_fields")
        con, owned = self._connect()
        try:
            rows = list(rows)
            if not rows:
                return
            width = len(fields) if fields else len(rows[0])
            placeholders = ", ".join(["?"] * width)
            if fields:
                col_sql = ", ".join(sql_ident(c) for c in fields)
                sql = f"INSERT INTO {ident} ({col_sql}) VALUES ({placeholders})"
            else:
                sql = f"INSERT INTO {ident} VALUES ({placeholders})"
            con.executemany(sql, rows)
        finally:
            if owned:
                con.close()

    def run(self, sql: str | Sequence[str], **kwargs: Any) -> None:
        statements = [sql] if isinstance(sql, str) else list(sql)
        con, owned = self._connect()
        try:
            for statement in statements:
                con.execute(statement)
                if hasattr(con, "complete"):
                    con.complete()
        finally:
            if owned:
                con.close()
