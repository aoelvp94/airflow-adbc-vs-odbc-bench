"""Local Arrow capability adapters (wrap AdbcHook / DuckDB; do not patch providers)."""

from adapters.adbc import AdbcArrowCapabilityAdapter, wrap_sql_hook
from adapters.duckdb import DuckDBArrowCapabilityAdapter

__all__ = [
    "AdbcArrowCapabilityAdapter",
    "DuckDBArrowCapabilityAdapter",
    "wrap_sql_hook",
]
