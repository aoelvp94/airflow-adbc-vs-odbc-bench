"""Local Arrow capability protocols (demo stubs; not apache/airflow)."""

from protocols.arrow_capabilities import (
    RecordBatchReadable,
    RecordBatchWritable,
    both_ends_arrow,
)

__all__ = [
    "RecordBatchReadable",
    "RecordBatchWritable",
    "both_ends_arrow",
]
