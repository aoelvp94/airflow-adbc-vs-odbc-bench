"""
Benchmark: file-backed SQLite, sqlite3 vs ADBC.

Optional local extra. No extra compose service. Traditional path is the
stdlib sqlite3 module. ADBC path uses AdbcHook (adbc_driver_sqlite)
with a DBAPI fallback.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from utils.benchmark_utils import (
    BenchmarkTimer,
    generate_sample_data,
    get_bench_row_count,
    sample_rows_to_arrow,
    save_benchmark_results,
)
from utils.local_engines import sqlite_path

INSERT_SQL = "INSERT INTO bench_insert (name, value, created_at, data) VALUES (?, ?, ?, ?)"
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS bench_insert (
    name TEXT,
    value INTEGER,
    created_at TEXT,
    data TEXT
)
"""


def _row_count() -> int:
    return get_bench_row_count()


def _open_adbc():
    try:
        hook = AdbcHook(adbc_conn_id="sqlite_adbc")
        return hook.get_conn()
    except Exception as exc:
        print(f"AdbcHook(sqlite_adbc) unavailable ({exc}); using adbc_driver_sqlite.dbapi")
        import adbc_driver_sqlite.dbapi

        return adbc_driver_sqlite.dbapi.connect(sqlite_path())


def setup_sqlite(**context):
    path = sqlite_path()
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    try:
        con.execute(CREATE_SQL)
        con.commit()
    finally:
        con.close()
    print(f"SQLite file ready: {path}")


def bench_insert_traditional(**context):
    row_count = _row_count()
    data = [(n, v, ts.isoformat(), d) for n, v, ts, d in generate_sample_data(row_count)]
    con = sqlite3.connect(sqlite_path())
    try:
        con.execute("DELETE FROM bench_insert")
        with BenchmarkTimer() as timer:
            con.executemany(INSERT_SQL, data)
            con.commit()
        duration = timer.get_duration()
    finally:
        con.close()
    context["task_instance"].xcom_push(key="insert_traditional", value=duration)
    print(f"SQLite traditional insert: {duration:.3f}s")
    return duration


def bench_insert_adbc(**context):
    row_count = _row_count()
    data = generate_sample_data(row_count)
    arrow_table = sample_rows_to_arrow(data, created_at_as_string=True)
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
    print(f"SQLite ADBC insert: {duration:.3f}s")
    return duration


def bench_select_traditional(**context):
    row_count = _row_count()
    con = sqlite3.connect(sqlite_path())
    try:
        with BenchmarkTimer() as timer:
            rows = con.execute(f"SELECT * FROM bench_insert LIMIT {row_count}").fetchall()
        duration = timer.get_duration()
        fetched = len(rows)
    finally:
        con.close()
    context["task_instance"].xcom_push(key="select_traditional", value=duration)
    context["task_instance"].xcom_push(key="select_traditional_rows", value=fetched)
    print(f"SQLite traditional select: {duration:.3f}s ({fetched} rows)")
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
    print(f"SQLite ADBC select: {duration:.3f}s ({fetched} rows)")
    return duration


def save_results(**context):
    ti = context["task_instance"]
    row_count = _row_count()
    save_benchmark_results(
        benchmark_name="bench_sqlite_insert",
        traditional_time=ti.xcom_pull(task_ids="insert_traditional", key="insert_traditional"),
        traditional_rows=row_count,
        adbc_time=ti.xcom_pull(task_ids="insert_adbc", key="insert_adbc"),
        adbc_rows=row_count,
        metadata={
            "engine": "sqlite",
            "layer": "hook",
            "traditional": "sqlite3 executemany",
            "adbc": "AdbcHook / adbc_driver_sqlite Arrow ingest",
            "path": sqlite_path(),
        },
    )
    save_benchmark_results(
        benchmark_name="bench_sqlite_select",
        traditional_time=ti.xcom_pull(task_ids="select_traditional", key="select_traditional"),
        traditional_rows=ti.xcom_pull(task_ids="select_traditional", key="select_traditional_rows"),
        adbc_time=ti.xcom_pull(task_ids="select_adbc", key="select_adbc"),
        adbc_rows=ti.xcom_pull(task_ids="select_adbc", key="select_adbc_rows"),
        metadata={
            "engine": "sqlite",
            "layer": "hook",
            "traditional": "sqlite3 fetchall",
            "adbc": "AdbcHook / fetch_arrow_table",
            "path": sqlite_path(),
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
    "bench_sqlite",
    default_args=default_args,
    description="Benchmark: file-backed SQLite, sqlite3 vs ADBC",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "sqlite"],
) as dag:
    setup = PythonOperator(task_id="setup_sqlite", python_callable=setup_sqlite)
    insert_traditional = PythonOperator(
        task_id="insert_traditional", python_callable=bench_insert_traditional
    )
    insert_adbc = PythonOperator(task_id="insert_adbc", python_callable=bench_insert_adbc)
    select_traditional = PythonOperator(
        task_id="select_traditional", python_callable=bench_select_traditional
    )
    select_adbc = PythonOperator(task_id="select_adbc", python_callable=bench_select_adbc)
    save = PythonOperator(task_id="save_results", python_callable=save_results)

    # One file: serialize writes and selects to avoid lock fights at large N.
    setup >> insert_traditional >> insert_adbc >> select_traditional >> select_adbc >> save
