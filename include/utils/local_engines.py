"""File-backed local engines used by the DuckDB and SQLite benches."""

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("BENCH_DATA_DIR", "/opt/airflow/include/data"))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(DATA_DIR / "bench.duckdb"))
# Dedicated file for the Phase-2 DuckDB multi-engine DAG (not bench_duckdb).
DUCKDB_CAP_PATH = os.environ.get(
    "DUCKDB_CAP_PATH", str(DATA_DIR / "bench_cap.duckdb")
)
SQLITE_PATH = os.environ.get("SQLITE_PATH", str(DATA_DIR / "bench.sqlite"))


def ensure_data_dir() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR


def duckdb_path() -> str:
    ensure_data_dir()
    return DUCKDB_PATH


def duckdb_cap_path() -> str:
    """File used by bench_capability_duckdb (separate from bench_duckdb)."""
    ensure_data_dir()
    return DUCKDB_CAP_PATH


def sqlite_path() -> str:
    ensure_data_dir()
    return SQLITE_PATH
