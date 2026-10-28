"""GenericTransfer subclass that loads the destination with ADBC Arrow ingest.

Stock ``GenericTransfer`` always calls ``destination_hook.insert_rows``. That is
row-based even when the dest conn type is ``adbc``, and ``AdbcHook.insert_rows``
cannot bind PostgreSQL NUMERIC. This operator keeps the GenericTransfer
interface (source/dest conn, SQL, preoperator, target_fields) but:

* extracts via ``fetch_arrow_table`` when the source hook is ``AdbcHook``,
  otherwise ``get_records`` plus an Arrow conversion
* appends with ``cursor.adbc_ingest`` and ``conn.commit``
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer


def _as_int32(column: pa.Array | pa.ChunkedArray) -> pa.Array:
    if pa.types.is_int32(column.type):
        return column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    return column.cast(pa.int32())


def _as_timestamp_us(column: pa.Array | pa.ChunkedArray) -> pa.Array:
    if pa.types.is_timestamp(column.type):
        if column.type.unit == "us":
            return column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
        return column.cast(pa.timestamp("us"))
    return pa.array(column.to_pylist(), type=pa.timestamp("us"))


def _as_decimal128(column: pa.Array | pa.ChunkedArray, precision: int = 10, scale: int = 2) -> pa.Array:
    import pyarrow.compute as pc

    target = pa.decimal128(precision, scale)
    if pa.types.is_decimal(column.type):
        if column.type.precision == precision and column.type.scale == scale:
            return column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
        return column.cast(target)
    chunked = column if isinstance(column, pa.ChunkedArray) else pa.chunked_array([column])
    combined = chunked.combine_chunks()
    storage = combined.storage if hasattr(combined, "storage") else combined
    return pc.cast(storage, pa.float64()).cast(target)


def records_to_arrow(rows: Sequence[Sequence[Any]], columns: Sequence[str]) -> pa.Table:
    """Build an ingest-ready Arrow table from DbApiHook.get_records rows."""
    if not columns:
        raise ValueError("target_fields (column names) are required to convert rows to Arrow")
    arrays: dict[str, pa.Array] = {}
    for i, name in enumerate(columns):
        values = [row[i] for row in rows]
        if name == "quantity":
            arrays[name] = pa.array(values, type=pa.int32())
        elif name == "price":
            arrays[name] = pa.array(values, type=pa.decimal128(10, 2))
        elif name == "created_at":
            arrays[name] = pa.array(values, type=pa.timestamp("us"))
        else:
            arrays[name] = pa.array(values)
    return pa.table(arrays)


def prepare_ingest_table(table: pa.Table, columns: Sequence[str] | None) -> pa.Table:
    """Reorder/cast columns so PostgreSQL COPY accepts the batch."""
    if columns:
        missing = [c for c in columns if c not in table.column_names]
        if missing:
            raise ValueError(f"Arrow extract missing columns {missing}; have {table.column_names}")
        table = table.select(list(columns))
    names = table.column_names
    out: dict[str, pa.Array] = {}
    for name in names:
        col = table[name]
        if name == "quantity":
            out[name] = _as_int32(col)
        elif name == "price":
            out[name] = _as_decimal128(col)
        elif name == "created_at":
            out[name] = _as_timestamp_us(col)
        else:
            out[name] = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    return pa.table(out)


class AdbcArrowTransfer(GenericTransfer):
    """Move rows from a SQL source into an ADBC dest using Arrow ingest.

    Destination ``conn_id`` must resolve to ``AdbcHook``. Source may be
    traditional (``PostgresHook`` / any ``DbApiHook``) or ADBC.
    """

    ui_color = "#7cb0f0"

    def _extract_arrow(self, sql: str) -> pa.Table:
        columns = list(self.insert_args.get("target_fields") or [])
        if isinstance(self.source_hook, AdbcHook):
            self.log.info("Extracting Arrow table via AdbcHook from %s", self.source_conn_id)
            with self.source_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    table = cursor.fetch_arrow_table()
            self.log.info("Extracted %s rows as Arrow (%s)", len(table), table.schema)
            return table
        self.log.info("Extracting records via %s from %s", type(self.source_hook).__name__, self.source_conn_id)
        rows = self.source_hook.get_records(sql)
        self.log.info("Extracted %s rows; converting to Arrow", len(rows))
        return records_to_arrow(rows, columns)

    def _ingest_arrow(self, table: pa.Table) -> None:
        if not isinstance(self.destination_hook, AdbcHook):
            raise TypeError(
                "AdbcArrowTransfer destination must be AdbcHook "
                f"(conn_id={self.destination_conn_id!r}, got {type(self.destination_hook).__name__})"
            )
        self.log.info(
            "Ingesting %s Arrow rows into %s via adbc_ingest",
            len(table),
            self.destination_table,
        )
        with self.destination_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.adbc_ingest(
                    table_name=self.destination_table,
                    data=table,
                    mode="append",
                )
            conn.commit()

    def execute(self, context) -> None:
        if self.preoperator:
            self.log.info("Running preoperator")
            self.log.info(self.preoperator)
            self.destination_hook.run(self.preoperator)

        sqls = [self.sql] if isinstance(self.sql, str) else list(self.sql)
        columns = list(self.insert_args.get("target_fields") or [])
        combined: pa.Table | None = None
        for sql in sqls:
            self.log.info("Executing:\n %s", sql)
            chunk = prepare_ingest_table(self._extract_arrow(sql), columns or None)
            combined = chunk if combined is None else pa.concat_tables([combined, chunk])

        if combined is None or len(combined) == 0:
            self.log.info("No rows to ingest into %s", self.destination_table)
            return

        self._ingest_arrow(combined)
        self.log.info("Arrow ingest complete: %s rows -> %s", len(combined), self.destination_table)
