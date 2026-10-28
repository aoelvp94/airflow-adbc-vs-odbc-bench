"""
Benchmark: operator-level SQL execute, stock GenericTransfer, and AdbcArrowTransfer.

Compares the same Airflow operators on a traditional Postgres connection
(PostgresHook via conn type postgres) and an ADBC connection (AdbcHook via
conn type adbc). Stock GenericTransfer always uses insert_rows on the dest.
AdbcArrowTransfer reads the source (get_records or fetch_arrow_table) and
writes with cursor.adbc_ingest.

Hook-level ETL (Python callables) lives in bench_transfer and is kept as a
reference for the same Arrow ingest API without an operator wrapper.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
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
from operators.adbc_arrow_transfer import AdbcArrowTransfer
from utils.benchmark_utils import (
    DEFAULT_ROW_COUNT,
    duration_entry,
    get_bench_row_count,
    product_rows_to_arrow,
    read_operator_duration,
    record_operator_duration,
    save_benchmark_results,
    write_benchmark_json,
)

OP_TABLES = (
    "bench_op_sql_traditional",
    "bench_op_sql_adbc",
    "bench_op_xfer_traditional",
    "bench_op_xfer_adbc",
    "bench_op_xfer_arrow_from_pg",
    "bench_op_xfer_arrow_from_adbc",
)

# Stock GenericTransfer on ADBC dest still cannot bind NUMERIC via insert_rows.
FLOAT8_DEST_TABLES = frozenset({"bench_op_xfer_adbc"})


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
    # AdbcHook.insert_rows cannot bind Python str/float into NUMERIC; stock
    # GenericTransfer ADBC dest uses float8 and a CAST in the extract SQL.
    # AdbcArrowTransfer dest tables keep NUMERIC; ingest uses decimal128.
    for table in FLOAT8_DEST_TABLES:
        hook.run(
            f"ALTER TABLE {table} ALTER COLUMN price TYPE DOUBLE PRECISION "
            f"USING price::float8"
        )
    print(f"Operator bench source ready: {row_count} rows")


def save_operator_results(**context):
    """Persist TaskInstance.duration for operator tasks (seconds)."""
    row_count = _row_count()

    def _duration(task_id: str) -> float:
        # Airflow 3 tasks cannot ORM-query sibling TaskInstances.
        return read_operator_duration(task_id)

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
    gt_trad = _duration("generic_transfer_traditional")
    gt_adbc = _duration("generic_transfer_adbc")
    save_benchmark_results(
        benchmark_name="bench_generic_transfer",
        traditional_time=gt_trad,
        traditional_rows=row_count,
        adbc_time=gt_adbc,
        adbc_rows=row_count,
        metadata={
            "layer": "operator",
            "operator": "GenericTransfer",
            "traditional_conn": "postgres_traditional",
            "adbc_conn": "postgres_adbc",
            "note": "Stock common.sql GenericTransfer: source get_records + dest insert_rows.",
        },
    )
    arrow_from_pg = _duration("arrow_transfer_from_traditional")
    arrow_from_adbc = _duration("arrow_transfer_from_adbc")
    write_benchmark_json(
        "bench_arrow_transfer",
        {
            "row_count": row_count,
            "stock_generic_transfer_traditional": duration_entry(gt_trad, row_count),
            "stock_generic_transfer_adbc_insert_rows": duration_entry(gt_adbc, row_count),
            "adbc_arrow_transfer_from_traditional": duration_entry(arrow_from_pg, row_count),
            "adbc_arrow_transfer_from_adbc": duration_entry(arrow_from_adbc, row_count),
            "speedup_vs_stock_adbc_insert_rows": round(gt_adbc / arrow_from_adbc, 2)
            if arrow_from_adbc > 0
            else 0,
            "metadata": {
                "layer": "operator",
                "note": (
                    "Stock GenericTransfer uses dest insert_rows. "
                    "AdbcArrowTransfer reads source (get_records or fetch_arrow_table) "
                    "then cursor.adbc_ingest on postgres_adbc."
                ),
            },
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
    description="Benchmark: SQLExecuteQueryOperator, GenericTransfer, AdbcArrowTransfer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "operator"],
) as dag:
    setup = PythonOperator(
        task_id="setup_operator_tables",
        python_callable=setup_operator_tables,
    )

    # Concrete LIMIT: str.format() used for {table} would turn Jinja {{ }} into { }.
    row_limit = int(os.environ.get("BENCH_ROW_COUNT", DEFAULT_ROW_COUNT))
    insert_select_sql = (
        "INSERT INTO {table} (product_name, category, price, quantity, created_at) "
        "SELECT product_name, category, price, quantity, created_at "
        f"FROM bench_source LIMIT {row_limit}"
    )
    extract_sql = (
        "SELECT product_name, category, price, quantity, created_at "
        f"FROM bench_source LIMIT {row_limit}"
    )
    extract_sql_adbc = (
        "SELECT product_name, category, price::float8 AS price, quantity, created_at "
        f"FROM bench_source LIMIT {row_limit}"
    )
    insert_fields = {
        "target_fields": [
            "product_name",
            "category",
            "price",
            "quantity",
            "created_at",
        ]
    }

    sql_traditional = SQLExecuteQueryOperator(
        task_id="sql_execute_traditional",
        conn_id="postgres_traditional",
        sql=insert_select_sql.format(table="bench_op_sql_traditional"),
        on_success_callback=record_operator_duration,
    )
    sql_adbc = SQLExecuteQueryOperator(
        task_id="sql_execute_adbc",
        conn_id="postgres_adbc",
        sql=insert_select_sql.format(table="bench_op_sql_adbc"),
        on_success_callback=record_operator_duration,
    )

    xfer_traditional = GenericTransfer(
        task_id="generic_transfer_traditional",
        source_conn_id="postgres_traditional",
        destination_conn_id="postgres_traditional",
        destination_table="bench_op_xfer_traditional",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_op_xfer_traditional",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )
    xfer_adbc = GenericTransfer(
        task_id="generic_transfer_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_op_xfer_adbc",
        sql=extract_sql_adbc,
        preoperator="TRUNCATE TABLE bench_op_xfer_adbc",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )

    arrow_from_traditional = AdbcArrowTransfer(
        task_id="arrow_transfer_from_traditional",
        source_conn_id="postgres_traditional",
        destination_conn_id="postgres_adbc",
        destination_table="bench_op_xfer_arrow_from_pg",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_op_xfer_arrow_from_pg",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )
    arrow_from_adbc = AdbcArrowTransfer(
        task_id="arrow_transfer_from_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_op_xfer_arrow_from_adbc",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_op_xfer_arrow_from_adbc",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )

    save = PythonOperator(
        task_id="save_results",
        python_callable=save_operator_results,
    )

    setup >> [
        sql_traditional,
        sql_adbc,
        xfer_traditional,
        xfer_adbc,
        arrow_from_traditional,
        arrow_from_adbc,
    ] >> save
