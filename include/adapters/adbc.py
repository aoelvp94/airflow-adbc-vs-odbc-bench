"""Wrap ``AdbcHook`` with RecordBatch read/write methods.

Does **not** monkeypatch ``airflow.providers.apache.arrow.hooks.adbc.AdbcHook``
in site-packages. Callers pass an existing hook instance into
``AdbcArrowCapabilityAdapter`` (or use ``wrap_sql_hook``).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import pyarrow as pa

from protocols.arrow_capabilities import RecordBatchReadable, RecordBatchWritable


def _prepare_batch(batch: pa.RecordBatch, columns: Sequence[str] | None) -> pa.RecordBatch:
    """Cast/reorder one batch using the same Postgres ingest rules as AdbcArrowTransfer."""
    from operators.adbc_arrow_transfer import prepare_ingest_batch

    return prepare_ingest_batch(batch, columns)


def _target_fields(kwargs: dict[str, Any]) -> Sequence[str] | None:
    fields = kwargs.get("target_fields")
    if fields:
        return list(fields)
    insert_args = kwargs.get("insert_args") or {}
    nested = insert_args.get("target_fields")
    return list(nested) if nested else None


class AdbcArrowCapabilityAdapter:
    """Thin wrapper: ``fetch_record_batch`` / ``adbc_ingest`` on a live AdbcHook.

    Implements ``RecordBatchReadable`` and ``RecordBatchWritable`` structurally
    (``@runtime_checkable``). Row methods (``get_records``, ``insert_rows``,
    ``run``) are delegated to the wrapped hook so GenericTransfer fallback works.
    """

    def __init__(self, hook: Any):
        object.__setattr__(self, "_hook", hook)

    @property
    def hook(self) -> Any:
        return self._hook

    def get_record_batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        """Stream RecordBatches. Keeps the ADBC cursor open until iteration ends."""
        with self._hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if hasattr(cursor, "fetch_record_batch"):
                    reader = cursor.fetch_record_batch()
                    yield from reader
                    return
                table = cursor.fetch_arrow_table()
                yield from table.to_batches()

    def insert_record_batches(
        self,
        table: str,
        batches: Iterator[pa.RecordBatch],
        **kwargs: Any,
    ) -> None:
        columns = _target_fields(kwargs)
        reader = _reader_from_batches(batches, columns)
        if reader is None:
            return
        with self._hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.adbc_ingest(table_name=table, data=reader, mode="append")
            conn.commit()

    def get_records(self, sql: str, *args: Any, **kwargs: Any):
        return self._hook.get_records(sql, *args, **kwargs)

    def insert_rows(self, table: str, rows, **kwargs: Any):
        return self._hook.insert_rows(table=table, rows=rows, **kwargs)

    def run(self, sql, **kwargs: Any):
        return self._hook.run(sql, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._hook, name)


def _reader_from_batches(
    batches: Iterator[pa.RecordBatch],
    columns: Sequence[str] | None,
) -> pa.RecordBatchReader | None:
    pending = iter(batches)
    first: pa.RecordBatch | None = None
    for batch in pending:
        if batch is not None and batch.num_rows:
            first = _prepare_batch(batch, columns)
            break
    if first is None:
        return None

    def _rest() -> Iterator[pa.RecordBatch]:
        yield first
        for batch in pending:
            if batch is None or batch.num_rows == 0:
                continue
            yield _prepare_batch(batch, columns)

    return pa.RecordBatchReader.from_batches(first.schema, _rest())


def wrap_sql_hook(hook: Any) -> Any:
    """If ``hook`` is an ``AdbcHook``, wrap it; leave other hooks unchanged.

    Already-capable adapters (both Readable and Writable) are returned as-is.
    """
    if hook is None:
        return hook
    if isinstance(hook, RecordBatchReadable) and isinstance(hook, RecordBatchWritable):
        return hook
    try:
        from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
    except ImportError:
        return hook
    if isinstance(hook, AdbcHook):
        return AdbcArrowCapabilityAdapter(hook)
    return hook
