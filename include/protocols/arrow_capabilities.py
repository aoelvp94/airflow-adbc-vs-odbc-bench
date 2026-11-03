"""Arrow RecordBatch capability protocols for the AIP Phase-2 demo.

These live only in this bench repo. They are not patches to apache/airflow
or to installed providers. A GenericTransfer-style helper can switch to a
columnar path when **both** ends advertise these methods.

Stock ``GenericTransfer`` still always uses ``get_records`` + ``insert_rows``.
``AdbcArrowTransfer`` is dest-only Arrow ingest (source may be row-based).
The Phase-2 claim is: when source is ``RecordBatchReadable`` **and** dest is
``RecordBatchWritable``, RecordBatches can move end-to-end without a row
materialization in Python.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable

import pyarrow as pa


@runtime_checkable
class RecordBatchReadable(Protocol):
    """Source that can stream Arrow RecordBatches from SQL."""

    def get_record_batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        """Yield RecordBatches for ``sql``. Connection stays open while iterating."""
        ...


@runtime_checkable
class RecordBatchWritable(Protocol):
    """Destination that can ingest an iterator of Arrow RecordBatches."""

    def insert_record_batches(
        self,
        table: str,
        batches: Iterator[pa.RecordBatch],
        **kwargs: Any,
    ) -> None:
        """Append ``batches`` into ``table``. ``kwargs`` may include ``target_fields``."""
        ...


def both_ends_arrow(source: Any, dest: Any) -> bool:
    """True when both hooks advertise RecordBatch read and write capabilities."""
    return isinstance(source, RecordBatchReadable) and isinstance(
        dest, RecordBatchWritable
    )
