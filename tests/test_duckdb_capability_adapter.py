"""DuckDB native Arrow adapter: RecordBatch round-trip without AdbcHook."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

from adapters.adbc import wrap_sql_hook
from adapters.duckdb import DuckDBArrowCapabilityAdapter
from operators.capability_generic_transfer import (
    COLUMNAR_PATH,
    ROW_PATH,
    transfer_with_capabilities,
)
from protocols.arrow_capabilities import RecordBatchReadable, RecordBatchWritable
from utils.local_engines import DUCKDB_CAP_PATH

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


def _product_arrow() -> pa.Table:
    return pa.table(
        {
            "product_name": pa.array(["Widget"], type=pa.string()),
            "category": pa.array(["Toys"], type=pa.string()),
            "price": pa.array([Decimal("10.50")], type=pa.decimal128(10, 2)),
            "quantity": pa.array([3], type=pa.int32()),
            "created_at": pa.array([datetime(2024, 1, 1)], type=pa.timestamp("us")),
        }
    )


def _fake_adbc_hook(arrow_table=None):
    hook = AdbcHook.__new__(AdbcHook)
    ingest = MagicMock(name="adbc_ingest")
    cursor = MagicMock(name="cursor")
    cursor.adbc_ingest = ingest
    cursor.execute = MagicMock()
    table = arrow_table if arrow_table is not None else _product_arrow()
    cursor.fetch_arrow_table = MagicMock(return_value=table)
    cursor.fetch_record_batch = MagicMock(return_value=table.to_reader())
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False
    conn = MagicMock(name="conn")
    conn.cursor.return_value = cursor
    conn.commit = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    hook.get_conn = MagicMock(return_value=conn)
    hook.get_records = MagicMock(return_value=[])
    hook.insert_rows = MagicMock(name="insert_rows")
    hook.run = MagicMock()
    return hook, ingest


def test_wrap_sql_hook_leaves_duckdb_adapter():
    con = duckdb.connect(":memory:")
    adapter = DuckDBArrowCapabilityAdapter(connection=con)
    assert wrap_sql_hook(adapter) is adapter


def test_duckdb_cap_path_is_sibling_of_bench_duckdb():
    assert DUCKDB_CAP_PATH.endswith("bench_cap.duckdb")
    assert DUCKDB_CAP_PATH.rstrip("/").split("/")[-1] == "bench_cap.duckdb"


def test_duckdb_to_adbc_uses_columnar_ingest():
    con = duckdb.connect(":memory:")
    src = _product_arrow()
    con.execute(
        "CREATE TABLE src (product_name VARCHAR, category VARCHAR, "
        "price DECIMAL(10, 2), quantity INTEGER, created_at TIMESTAMP)"
    )
    con.from_arrow(src).insert_into("src")
    dest, ingest = _fake_adbc_hook()
    path = transfer_with_capabilities(
        DuckDBArrowCapabilityAdapter(connection=con),
        dest,
        sql="SELECT product_name, category, price, quantity, created_at FROM src",
        destination_table="bench_cap_ddb_to_pg",
        insert_args={
            "target_fields": [
                "product_name",
                "category",
                "price",
                "quantity",
                "created_at",
            ]
        },
    )
    assert path == COLUMNAR_PATH
    ingest.assert_called_once()
    dest.insert_rows.assert_not_called()
    reader = ingest.call_args.kwargs["data"]
    table = reader.read_all()
    assert len(table) == 1
    assert table.column_names == [
        "product_name",
        "category",
        "price",
        "quantity",
        "created_at",
    ]


def test_adbc_to_duckdb_columnar_preserves_rows():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE dest (product_name VARCHAR, category VARCHAR, "
        "price DECIMAL(10, 2), quantity INTEGER, created_at TIMESTAMP)"
    )
    source, ingest = _fake_adbc_hook()
    path = transfer_with_capabilities(
        source,
        DuckDBArrowCapabilityAdapter(connection=con),
        sql="SELECT product_name, category, price, quantity, created_at FROM bench_source",
        destination_table="dest",
        insert_args={
            "target_fields": [
                "product_name",
                "category",
                "price",
                "quantity",
                "created_at",
            ]
        },
    )
    assert path == COLUMNAR_PATH
    ingest.assert_not_called()
    n, qty = con.execute("SELECT COUNT(*), SUM(quantity) FROM dest").fetchone()
    assert n == 1
    assert qty == 3
