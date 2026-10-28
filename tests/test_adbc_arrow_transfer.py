"""Unit tests: AdbcArrowTransfer uses adbc_ingest; stock GenericTransfer does not."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
from airflow.sdk import DAG

from operators.adbc_arrow_transfer import AdbcArrowTransfer

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
    cursor.fetch_arrow_table = MagicMock(return_value=arrow_table if arrow_table is not None else _sample_arrow())
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
    return hook, ingest


def _traditional_source():
    hook = MagicMock(name="postgres_hook")
    hook.get_records.return_value = list(SAMPLE_ROWS)
    hook.insert_rows = MagicMock(name="insert_rows")
    hook.run = MagicMock()
    return hook


@pytest.fixture
def dag():
    return DAG(
        dag_id="unit_adbc_arrow_transfer",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
    )


def test_adbc_arrow_transfer_calls_adbc_ingest(dag):
    dest, ingest = _fake_adbc_hook()
    source = _traditional_source()
    with dag:
        op = AdbcArrowTransfer(
            task_id="arrow_xfer",
            sql="SELECT product_name, category, price, quantity, created_at FROM bench_source",
            source_conn_id="postgres_traditional",
            destination_conn_id="postgres_adbc",
            destination_table="bench_op_xfer_arrow_from_pg",
            insert_args={"target_fields": TARGET_FIELDS},
        )
    op.source_hook = source
    op.destination_hook = dest

    op.execute({})

    ingest.assert_called_once()
    kwargs = ingest.call_args.kwargs
    assert kwargs["table_name"] == "bench_op_xfer_arrow_from_pg"
    assert kwargs["mode"] == "append"
    assert isinstance(kwargs["data"], pa.Table)
    assert len(kwargs["data"]) == 1
    dest.insert_rows.assert_not_called()
    dest.get_conn.assert_called()


def test_stock_generic_transfer_uses_insert_rows_not_adbc_ingest(dag):
    dest, ingest = _fake_adbc_hook()
    source = _traditional_source()
    with dag:
        op = GenericTransfer(
            task_id="stock_xfer",
            sql="SELECT product_name, category, price, quantity, created_at FROM bench_source",
            source_conn_id="postgres_adbc",
            destination_conn_id="postgres_adbc",
            destination_table="bench_op_xfer_adbc",
            insert_args={"target_fields": TARGET_FIELDS},
        )
    op.source_hook = source
    op.destination_hook = dest

    op.execute({})

    dest.insert_rows.assert_called_once()
    insert_kwargs = dest.insert_rows.call_args.kwargs
    assert insert_kwargs["table"] == "bench_op_xfer_adbc"
    assert list(insert_kwargs["rows"]) == SAMPLE_ROWS
    ingest.assert_not_called()
    dest.get_conn.assert_not_called()
