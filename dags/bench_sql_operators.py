"""
Benchmark: operator-level SQL execute and GenericTransfer.

Compares the same Airflow operators on a traditional Postgres connection
(PostgresHook via conn type postgres) and an ADBC connection (AdbcHook via
conn type adbc). This is distinct from the hook-level DAGs, which call
PostgresHook / AdbcHook inside PythonOperator callables.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
except ImportError:
    from airflow.operators.generic_transfer import GenericTransfer

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from utils.benchmark_utils import (
    DEFAULT_ROW_COUNT,
    get_bench_row_count,
    product_rows_to_arrow,
    save_benchmark_results,
)

OP_TABLES = (
    "bench_op_sql_traditional",
    "bench_op_sql_adbc",
    "bench_op_xfer_traditional",
    "bench_op_xfer_adbc",
)


def _row_count() -> int:
    return get_bench_row_count()


def setup_operator_tables(**context):
    """Create dest tables and load bench_source. Not timed."""
    row_count = _row_count()
    hook = PostgresHook(postgres_conn_id="postgres_traditional")
    hook.run("TRUNCATE TABLE bench_source;")
    for table in OP_TABLES:
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price NUMERIC(10, 2),
                quantity INTEGER,
                created_at TIMESTAMP
            )
            """
        )
        hook.run(f"TRUNCATE TABLE {table};")

    # Arrow ingest for setup only; timed paths are the operators below.
    arrow_table = product_rows_to_arrow(row_count)
    adbc_hook = AdbcHook(adbc_conn_id="postgres_adbc")
    with adbc_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.adbc_ingest(
                table_name="bench_source",
                data=arrow_table,
                mode="append",
            )
        conn.commit()
    print(f"Operator bench source ready: {row_count} rows")


def save_operator_results(**context):
    """Persist TaskInstance.duration for operator tasks (seconds)."""
    dag_run = context["dag_run"]
    row_count = _row_count()

    def _duration(task_id: str) -> float:
        ti = dag_run.get_task_instance(task_id)
        if ti is None or ti.duration is None:
            raise RuntimeError(f"No duration recorded for task {task_id}")
        return float(ti.duration)

    save_benchmark_results(
        benchmark_name="bench_sql_execute",
        traditional_time=_duration("sql_execute_traditional"),
        traditional_rows=row_count,
        adbc_time=_duration("sql_execute_adbc"),
        adbc_rows=row_count,
        metadata={
            "layer": "operator",
            "operator": "SQLExecuteQueryOperator",
            "traditional_conn": "postgres_traditional",
            "adbc_conn": "postgres_adbc",
            "note": "Server-side INSERT...SELECT; hook overhead is connection + execute only.",
        },
    )
    save_benchmark_results(
        benchmark_name="bench_generic_transfer",
        traditional_time=_duration("generic_transfer_traditional"),
        traditional_rows=row_count,
        adbc_time=_duration("generic_transfer_adbc"),
        adbc_rows=row_count,
        metadata={
            "layer": "operator",
            "operator": "GenericTransfer",
            "traditional_conn": "postgres_traditional",
            "adbc_conn": "postgres_adbc",
            "note": "common.sql GenericTransfer: source get_records + dest insert_rows.",
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
    "bench_sql_operators",
    default_args=default_args,
    description="Benchmark: SQLExecuteQueryOperator and GenericTransfer, Postgres vs ADBC",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "operator"],
) as dag:
    setup = PythonOperator(
        task_id="setup_operator_tables",
        python_callable=setup_operator_tables,
    )

    limit_tpl = f"{{{{ var.value.get('BENCH_ROW_COUNT', {DEFAULT_ROW_COUNT}) }}}}"
    insert_select_sql = (
        "INSERT INTO {table} (product_name, category, price, quantity, created_at) "
        "SELECT product_name, category, price, quantity, created_at "
        f"FROM bench_source LIMIT {limit_tpl}"
    )
    extract_sql = (
        "SELECT product_name, category, price, quantity, created_at "
        f"FROM bench_source LIMIT {limit_tpl}"
    )

    sql_traditional = SQLExecuteQueryOperator(
        task_id="sql_execute_traditional",
        conn_id="postgres_traditional",
        sql=insert_select_sql.format(table="bench_op_sql_traditional"),
    )
    sql_adbc = SQLExecuteQueryOperator(
        task_id="sql_execute_adbc",
        conn_id="postgres_adbc",
        sql=insert_select_sql.format(table="bench_op_sql_adbc"),
    )

    xfer_traditional = GenericTransfer(
        task_id="generic_transfer_traditional",
        source_conn_id="postgres_traditional",
        destination_conn_id="postgres_traditional",
        destination_table="bench_op_xfer_traditional",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_op_xfer_traditional",
    )
    xfer_adbc = GenericTransfer(
        task_id="generic_transfer_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_op_xfer_adbc",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_op_xfer_adbc",
    )

    save = PythonOperator(
        task_id="save_results",
        python_callable=save_operator_results,
    )

    setup >> [sql_traditional, sql_adbc, xfer_traditional, xfer_adbc] >> save
