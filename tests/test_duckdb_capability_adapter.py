"""DuckDB native Arrow adapter: RecordBatch round-trip without AdbcHook."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from adapters.duckdb import DuckDBArrowCapabilityAdapter
from operators.capability_generic_transfer import (
    COLUMNAR_PATH,
    ROW_PATH,
    transfer_with_capabilities,
)
from protocols.arrow_capabilities import RecordBatchReadable, RecordBatchWritable

duckdb = pytest.importorskip("duckdb")


def _sample_table() -> pa.Table:
    return pa.table(
        {
            "product_name": pa.array(["Widget", "Gadget"], type=pa.string()),
            "category": pa.array(["Toys", "Tools"], type=pa.string()),
            "quantity": pa.array([3, 5], type=pa.int32()),
        }
    )


def test_duckdb_adapter_advertises_both_protocols():
    con = duckdb.connect(":memory:")
    adapter = DuckDBArrowCapabilityAdapter(connection=con)
    assert isinstance(adapter, RecordBatchReadable)
    assert isinstance(adapter, RecordBatchWritable)


def test_duckdb_columnar_roundtrip_preserves_row_count():
    con = duckdb.connect(":memory:")
    src_table = _sample_table()
    con.execute(
        "CREATE TABLE src (product_name VARCHAR, category VARCHAR, quantity INTEGER)"
    )
    con.execute(
        "CREATE TABLE dest (product_name VARCHAR, category VARCHAR, quantity INTEGER)"
    )
    con.from_arrow(src_table).insert_into("src")

    adapter = DuckDBArrowCapabilityAdapter(connection=con)
    path = transfer_with_capabilities(
        adapter,
        adapter,
        sql="SELECT product_name, category, quantity FROM src",
        destination_table="dest",
        insert_args={"target_fields": ["product_name", "category", "quantity"]},
    )
    assert path == COLUMNAR_PATH
    n, qty = con.execute("SELECT COUNT(*), SUM(quantity) FROM dest").fetchone()
    assert n == 2
    assert qty == 8
    names = [row[0] for row in con.execute("SELECT product_name FROM dest ORDER BY 1").fetchall()]
    assert names == ["Gadget", "Widget"]


def test_duckdb_row_fallback_when_dest_is_records_only():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE src (product_name VARCHAR, quantity INTEGER)")
    con.execute("INSERT INTO src VALUES ('A', 1), ('B', 2)")
    con.execute("CREATE TABLE dest (product_name VARCHAR, quantity INTEGER)")

    class RecordsOnlyDest:
        def __init__(self, connection):
            self.connection = connection
            self.insert_rows = MagicMock(wraps=self._insert)

        def _insert(self, table, rows, **kwargs):
            DuckDBArrowCapabilityAdapter(connection=self.connection).insert_rows(
                table, rows, **kwargs
            )

        def run(self, sql, **kwargs):
            self.connection.execute(sql)

        def get_records(self, sql):
            return self.connection.execute(sql).fetchall()

    source = DuckDBArrowCapabilityAdapter(connection=con)
    dest = RecordsOnlyDest(con)
    # dest has insert_rows/get_records/run only (spec-like: no RecordBatch methods)
    path = transfer_with_capabilities(
        source,
        dest,
        sql="SELECT product_name, quantity FROM src",
        destination_table="dest",
        insert_args={"target_fields": ["product_name", "quantity"]},
    )
    assert path == ROW_PATH
    dest.insert_rows.assert_called_once()
    assert con.execute("SELECT COUNT(*) FROM dest").fetchone()[0] == 2
