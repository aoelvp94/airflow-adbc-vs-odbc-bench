"""
Benchmark: AIP Phase-2 capability GenericTransfer vs stock row GenericTransfer.

Local stubs only (include/protocols, include/adapters, include/operators).
Does not modify apache/airflow.

Matrix (same extract SQL / same N, serialized on LocalExecutor):
- Stock GenericTransfer Adbc -> Adbc (get_records + insert_rows; baseline)
- CapabilityGenericTransfer Adbc -> Adbc (RecordBatch E2E when both ends advertise)
- AdbcArrowTransfer Adbc -> Adbc (existing dest-only adbc_ingest; continuity)

Post-load (not timed): COUNT == N and payload digest vs bench_source.
Stock GT ADBC dest stores price as float8 (insert_rows cannot bind NUMERIC);
compared with CAST(price AS numeric(10, 2)).
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
except ImportError:
    from airflow.operators.generic_transfer import GenericTransfer

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from operators.adbc_arrow_transfer import AdbcArrowTransfer
from operators.capability_generic_transfer import CapabilityGenericTransfer
from utils.benchmark_utils import (
    DEFAULT_ROW_COUNT,
    duration_entry,
    get_bench_row_count,
    product_rows_to_arrow,
    read_operator_duration,
    record_operator_duration,
    write_benchmark_json,
)
from utils.verify_load import verify_copy_vs_source

CAP_TABLES = (
    "bench_cap_gt_adbc",
    "bench_cap_columnar",
    "bench_cap_arrow",
)
FLOAT8_DEST_TABLES = frozenset({"bench_cap_gt_adbc"})

INSERT_COLUMNS = [
    "product_name",
    "category",
    "price",
    "quantity",
    "created_at",
]


def _row_count() -> int:
    return get_bench_row_count()


def setup_capability_tables(**context):
    """Create dest tables and load bench_source. Not timed."""
    row_count = _row_count()
    hook = PostgresHook(postgres_conn_id="postgres_traditional")
    hook.run("TRUNCATE TABLE bench_source;")
    create_sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price NUMERIC(10, 2),
                quantity INTEGER,
                created_at TIMESTAMP
            )
            """
    for table in CAP_TABLES:
        hook.run(create_sql.format(table=table))
        hook.run(f"TRUNCATE TABLE {table};")

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

    for table in FLOAT8_DEST_TABLES:
        hook.run(
            f"ALTER TABLE {table} ALTER COLUMN price TYPE DOUBLE PRECISION "
            f"USING price::float8"
        )
    print(f"Capability transfer bench source ready: {row_count} rows")


def verify_capability_loads(**context):
    """COUNT dest rows; all three copy paths must match bench_source. Not timed."""
    expected = _row_count()
    hook = PostgresHook(postgres_conn_id="postgres_traditional")
    verify_copy_vs_source(
        hook,
        "bench_source",
        CAP_TABLES,
        expected,
        include_price=True,
    )
    print(
        "FLOAT8 dests "
        f"{sorted(FLOAT8_DEST_TABLES)}: "
        "price compared as CAST(price AS numeric(10, 2)) vs bench_source. "
        "Faster is not allowed to mean fewer or different rows."
    )


def save_capability_results(**context):
    """Persist TaskInstance durations for the Phase-2 comparison."""
    row_count = _row_count()
    gt = read_operator_duration("stock_generic_transfer_adbc")
    columnar = read_operator_duration("capability_transfer_adbc")
    arrow = read_operator_duration("adbc_arrow_transfer_adbc")
    write_benchmark_json(
        "bench_capability_transfer",
        {
            "row_count": row_count,
            "stock_generic_transfer_adbc_row_path": duration_entry(gt, row_count),
            "capability_generic_transfer_adbc_columnar": duration_entry(
                columnar, row_count
            ),
            "adbc_arrow_transfer_dest_ingest": duration_entry(arrow, row_count),
            "speedup_columnar_vs_stock_gt": round(gt / columnar, 2) if columnar else 0,
            "metadata": {
                "layer": "operator",
                "aip": "phase-2-local-stub",
                "note": (
                    "Stock GenericTransfer dest-writes with insert_rows even when "
                    "conn type is adbc. CapabilityGenericTransfer streams "
                    "RecordBatches when both wrapped AdbcHooks advertise "
                    "RecordBatchReadable + RecordBatchWritable. AdbcArrowTransfer "
                    "is dest-only adbc_ingest (kept for continuity)."
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
    "bench_capability_transfer",
    default_args=default_args,
    description=(
        "AIP Phase-2 demo: stock GenericTransfer row path vs both-ends "
        "RecordBatch CapabilityGenericTransfer vs AdbcArrowTransfer"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "transfer", "aip"],
) as dag:
    setup = PythonOperator(
        task_id="setup_capability_tables",
        python_callable=setup_capability_tables,
    )

    row_limit = int(os.environ.get("BENCH_ROW_COUNT", DEFAULT_ROW_COUNT))
    extract_sql = (
        "SELECT product_name, category, price, quantity, created_at "
        f"FROM bench_source LIMIT {row_limit}"
    )
    extract_sql_float8 = (
        "SELECT product_name, category, price::float8 AS price, quantity, created_at "
        f"FROM bench_source LIMIT {row_limit}"
    )
    insert_fields = {"target_fields": INSERT_COLUMNS}

    stock_gt = GenericTransfer(
        task_id="stock_generic_transfer_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_cap_gt_adbc",
        sql=extract_sql_float8,
        preoperator="TRUNCATE TABLE bench_cap_gt_adbc",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )
    capability = CapabilityGenericTransfer(
        task_id="capability_transfer_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_cap_columnar",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_cap_columnar",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )
    arrow = AdbcArrowTransfer(
        task_id="adbc_arrow_transfer_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_cap_arrow",
        sql=extract_sql,
        preoperator="TRUNCATE TABLE bench_cap_arrow",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )

    verify = PythonOperator(
        task_id="verify_capability_loads",
        python_callable=verify_capability_loads,
    )
    save = PythonOperator(
        task_id="save_results",
        python_callable=save_capability_results,
    )

    # Serialize timed tasks so LocalExecutor does not overlap 1e6-row jobs.
    timed = [stock_gt, capability, arrow]
    setup >> timed[0]
    for left, right in zip(timed, timed[1:]):
        left >> right
    timed[-1] >> verify >> save
