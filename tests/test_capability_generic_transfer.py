"""Unit tests: CapabilityGenericTransfer switches columnar vs row path."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
from airflow.sdk import DAG

from adapters.adbc import AdbcArrowCapabilityAdapter, wrap_sql_hook
from operators.capability_generic_transfer import (
    COLUMNAR_PATH,
    ROW_PATH,
    CapabilityGenericTransfer,
    transfer_with_capabilities,
)
from protocols.arrow_capabilities import (
    RecordBatchReadable,
    RecordBatchWritable,
    both_ends_arrow,
)

SAMPLE_ROWS = [
    ("Widget", "Toys", Decimal("10.50"), 3, datetime(2024, 1, 1)),
]
TARGET_FIELDS = [
    "product_name",
    "category",
    "price",
    "quantity",
    "created_at",
]


def _sample_arrow() -> pa.Table:
    row = SAMPLE_ROWS[0]
    return pa.table(
        {
            "product_name": pa.array([row[0]], type=pa.string()),
            "category": pa.array([row[1]], type=pa.string()),
            "price": pa.array([row[2]], type=pa.decimal128(10, 2)),
            "quantity": pa.array([row[3]], type=pa.int32()),
            "created_at": pa.array([row[4]], type=pa.timestamp("us")),
        }
    )


def _fake_adbc_hook(*, records=None, arrow_table=None):
    """AdbcHook instance that never opens Postgres; spies ingest vs insert_rows."""
    hook = AdbcHook.__new__(AdbcHook)
    ingest = MagicMock(name="adbc_ingest")
    cursor = MagicMock(name="cursor")
    cursor.adbc_ingest = ingest
    cursor.execute = MagicMock()
    table = arrow_table if arrow_table is not None else _sample_arrow()
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
    hook.get_records = MagicMock(return_value=list(records or SAMPLE_ROWS))
    hook.insert_rows = MagicMock(name="insert_rows")
    hook.run = MagicMock()
    return hook, ingest, cursor


def _traditional_source():
    hook = MagicMock(spec=["get_records", "insert_rows", "run"])
    hook.get_records.return_value = list(SAMPLE_ROWS)
    hook.insert_rows = MagicMock(name="insert_rows")
    hook.run = MagicMock()
    return hook


@pytest.fixture
def dag():
    return DAG(
        dag_id="unit_capability_generic_transfer",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
    )


def _make_op(dag, cls, task_id, source, dest, table="bench_cap_columnar"):
    with dag:
        op = cls(
            task_id=task_id,
            sql="SELECT product_name, category, price, quantity, created_at FROM bench_source",
            source_conn_id="postgres_adbc",
            destination_conn_id="postgres_adbc",
            destination_table=table,
            insert_args={"target_fields": TARGET_FIELDS},
        )
    op.source_hook = source
    op.destination_hook = dest
    return op


def test_protocols_are_runtime_checkable():
    dest, _, _ = _fake_adbc_hook()
    adapter = AdbcArrowCapabilityAdapter(dest)
    assert isinstance(adapter, RecordBatchReadable)
    assert isinstance(adapter, RecordBatchWritable)
    traditional = _traditional_source()
    assert not isinstance(traditional, RecordBatchReadable)
    assert not isinstance(traditional, RecordBatchWritable)
    assert not both_ends_arrow(traditional, adapter)
    assert both_ends_arrow(adapter, adapter)


def test_wrap_sql_hook_wraps_adbc_only():
    dest, _, _ = _fake_adbc_hook()
    wrapped = wrap_sql_hook(dest)
    assert isinstance(wrapped, AdbcArrowCapabilityAdapter)
    assert wrap_sql_hook(wrapped) is wrapped
    traditional = _traditional_source()
    assert wrap_sql_hook(traditional) is traditional


def test_capability_both_adbc_uses_record_batch_ingest(dag):
    source, source_ingest, source_cursor = _fake_adbc_hook()
    dest, dest_ingest, dest_cursor = _fake_adbc_hook()
    op = _make_op(dag, CapabilityGenericTransfer, "cap_xfer", source, dest)

    path = op.execute({})

    assert path == COLUMNAR_PATH
    source_cursor.fetch_record_batch.assert_called()
    dest_ingest.assert_called_once()
    kwargs = dest_ingest.call_args.kwargs
    assert kwargs["table_name"] == "bench_cap_columnar"
    assert kwargs["mode"] == "append"
    assert isinstance(kwargs["data"], pa.RecordBatchReader)
    dest.insert_rows.assert_not_called()
    source.insert_rows.assert_not_called()
    source_ingest.assert_not_called()


def test_capability_traditional_source_falls_back_to_insert_rows(dag):
    dest, dest_ingest, _ = _fake_adbc_hook()
    source = _traditional_source()
    op = _make_op(
        dag,
        CapabilityGenericTransfer,
        "cap_fallback",
        source,
        dest,
        table="bench_cap_gt_adbc",
    )

    path = op.execute({})

    assert path == ROW_PATH
    source.get_records.assert_called_once()
    dest.insert_rows.assert_called_once()
    insert_kwargs = dest.insert_rows.call_args.kwargs
    assert insert_kwargs["table"] == "bench_cap_gt_adbc"
    assert list(insert_kwargs["rows"]) == SAMPLE_ROWS
    dest_ingest.assert_not_called()


def test_stock_generic_transfer_still_uses_insert_rows(dag):
    dest, dest_ingest, _ = _fake_adbc_hook()
    source = _traditional_source()
    op = _make_op(
        dag,
        GenericTransfer,
        "stock_xfer",
        source,
        dest,
        table="bench_cap_gt_adbc",
    )

    op.execute({})

    dest.insert_rows.assert_called_once()
    dest_ingest.assert_not_called()


def test_helper_columnar_preserves_row_count():
    source, _, _ = _fake_adbc_hook()
    dest, dest_ingest, _ = _fake_adbc_hook()
    path = transfer_with_capabilities(
        source,
        dest,
        sql="SELECT product_name, category, price, quantity, created_at FROM bench_source",
        destination_table="bench_cap_columnar",
        insert_args={"target_fields": TARGET_FIELDS},
    )
    assert path == COLUMNAR_PATH
    reader = dest_ingest.call_args.kwargs["data"]
    table = reader.read_all()
    assert len(table) == 1


def test_helper_empty_batches_does_not_ingest():
    empty = pa.table(
        {
            "product_name": pa.array([], type=pa.string()),
            "category": pa.array([], type=pa.string()),
            "price": pa.array([], type=pa.decimal128(10, 2)),
            "quantity": pa.array([], type=pa.int32()),
            "created_at": pa.array([], type=pa.timestamp("us")),
        }
    )
    source, _, _ = _fake_adbc_hook(arrow_table=empty)
    dest, dest_ingest, _ = _fake_adbc_hook()
    path = transfer_with_capabilities(
        AdbcArrowCapabilityAdapter(source),
        AdbcArrowCapabilityAdapter(dest),
        sql="SELECT 1 WHERE false",
        destination_table="bench_cap_columnar",
        insert_args={"target_fields": TARGET_FIELDS},
    )
    assert path == COLUMNAR_PATH
    dest_ingest.assert_not_called()
    dest.insert_rows.assert_not_called()
