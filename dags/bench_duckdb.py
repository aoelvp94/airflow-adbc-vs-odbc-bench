"""
Benchmark: file-backed DuckDB, traditional Python API vs ADBC.

No extra compose service. Database file lives under include/data/.
Traditional path uses the duckdb module. ADBC path uses AdbcHook
(driver adbc_driver_duckdb, bundled with the duckdb wheel) with a
DBAPI fallback to adbc_driver_duckdb.dbapi.connect.
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
import pyarrow as pa
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from utils.benchmark_utils import (
    BenchmarkTimer,
    generate_sample_data,
    save_benchmark_results,
)
from utils.local_engines import duckdb_path

INSERT_SQL = "INSERT INTO bench_insert (name, value, created_at, data) VALUES (?, ?, ?, ?)"
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS bench_insert (
    name VARCHAR,
    value INTEGER,
    created_at TIMESTAMP,
    data VARCHAR
)
"""


def _row_count() -> int:
    return int(Variable.get("BENCH_ROW_COUNT", default_var=100000))


def _open_adbc():
    """Prefer AdbcHook; fall back to the DuckDB-bundled ADBC DBAPI."""
    try:
        hook = AdbcHook(adbc_conn_id="duckdb_adbc")
        return hook.get_conn()
    except Exception as exc:
        print(f"AdbcHook(duckdb_adbc) unavailable ({exc}); using adbc_driver_duckdb.dbapi")
        import adbc_driver_duckdb.dbapi

        return adbc_driver_duckdb.dbapi.connect(duckdb_path())


def setup_duckdb(**context):
    import duckdb

    path = duckdb_path()
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    try:
        con.execute(CREATE_SQL)
    finally:
        con.close()
    print(f"DuckDB file ready: {path}")


def bench_insert_traditional(**context):
    import duckdb

    row_count = _row_count()
    data = generate_sample_data(row_count)
    con = duckdb.connect(duckdb_path())
    try:
        con.execute("DELETE FROM bench_insert")
        with BenchmarkTimer() as timer:
            con.executemany(INSERT_SQL, data)
        duration = timer.get_duration()
    finally:
        con.close()
    context["task_instance"].xcom_push(key="insert_traditional", value=duration)
    print(f"DuckDB traditional insert: {duration:.3f}s")
    return duration


def bench_insert_adbc(**context):
    row_count = _row_count()
    data = generate_sample_data(row_count)
    arrow_table = pa.table(
        {
            "name": [row[0] for row in data],
            "value": [row[1] for row in data],
            "created_at": [row[2] for row in data],
            "data": [row[3] for row in data],
        }
    )
    conn = _open_adbc()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM bench_insert")
        with BenchmarkTimer() as timer:
            with conn.cursor() as cursor:
                cursor.adbc_ingest("bench_insert", arrow_table, mode="append")
        duration = timer.get_duration()
        if hasattr(conn, "commit"):
            conn.commit()
    finally:
        conn.close()
    context["task_instance"].xcom_push(key="insert_adbc", value=duration)
    print(f"DuckDB ADBC insert: {duration:.3f}s")
    return duration


def bench_select_traditional(**context):
    import duckdb

    row_count = _row_count()
    con = duckdb.connect(duckdb_path())
    try:
        with BenchmarkTimer() as timer:
            rows = con.execute(f"SELECT * FROM bench_insert LIMIT {row_count}").fetchall()
        duration = timer.get_duration()
        fetched = len(rows)
    finally:
        con.close()
    context["task_instance"].xcom_push(key="select_traditional", value=duration)
    context["task_instance"].xcom_push(key="select_traditional_rows", value=fetched)
    print(f"DuckDB traditional select: {duration:.3f}s ({fetched} rows)")
    return duration


def bench_select_adbc(**context):
    row_count = _row_count()
    conn = _open_adbc()
    try:
        with BenchmarkTimer() as timer:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM bench_insert LIMIT {row_count}")
                table = cursor.fetch_arrow_table()
        duration = timer.get_duration()
        fetched = len(table)
    finally:
        conn.close()
    context["task_instance"].xcom_push(key="select_adbc", value=duration)
    context["task_instance"].xcom_push(key="select_adbc_rows", value=fetched)
    print(f"DuckDB ADBC select: {duration:.3f}s ({fetched} rows)")
    return duration


def save_results(**context):
    ti = context["task_instance"]
    row_count = _row_count()
    save_benchmark_results(
        benchmark_name="bench_duckdb_insert",
        traditional_time=ti.xcom_pull(task_ids="insert_traditional", key="insert_traditional"),
        traditional_rows=row_count,
        adbc_time=ti.xcom_pull(task_ids="insert_adbc", key="insert_adbc"),
        adbc_rows=row_count,
        metadata={
            "engine": "duckdb",
            "layer": "hook",
            "traditional": "duckdb Python API executemany",
            "adbc": "AdbcHook / adbc_driver_duckdb Arrow ingest",
            "path": duckdb_path(),
        },
    )
    save_benchmark_results(
        benchmark_name="bench_duckdb_select",
        traditional_time=ti.xcom_pull(task_ids="select_traditional", key="select_traditional"),
        traditional_rows=ti.xcom_pull(task_ids="select_traditional", key="select_traditional_rows"),
        adbc_time=ti.xcom_pull(task_ids="select_adbc", key="select_adbc"),
        adbc_rows=ti.xcom_pull(task_ids="select_adbc", key="select_adbc_rows"),
        metadata={
            "engine": "duckdb",
            "layer": "hook",
            "traditional": "duckdb Python API fetchall",
            "adbc": "AdbcHook / fetch_arrow_table",
            "path": duckdb_path(),
        },
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "bench_duckdb",
    default_args=default_args,
    description="Benchmark: file-backed DuckDB, duckdb API vs ADBC",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "duckdb"],
) as dag:
    setup = PythonOperator(task_id="setup_duckdb", python_callable=setup_duckdb)
    insert_traditional = PythonOperator(
        task_id="insert_traditional", python_callable=bench_insert_traditional
    )
    insert_adbc = PythonOperator(task_id="insert_adbc", python_callable=bench_insert_adbc)
    select_traditional = PythonOperator(
        task_id="select_traditional", python_callable=bench_select_traditional
    )
    select_adbc = PythonOperator(task_id="select_adbc", python_callable=bench_select_adbc)
    save = PythonOperator(task_id="save_results", python_callable=save_results)

    # One file: serialize writes, then select.
    setup >> insert_traditional >> insert_adbc >> [select_traditional, select_adbc] >> save
