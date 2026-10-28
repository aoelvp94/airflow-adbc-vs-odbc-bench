"""
Benchmark: stock common.sql SQL + transfer operators vs ADBC/Arrow vs ODBC.

Same N and same INSERT...SELECT / extract SQL on compose PostgreSQL 18.

Measured (stock operators that exist in apache-airflow-providers-common-sql 2.1.1):
- SQLExecuteQueryOperator  (conn: postgres / odbc / adbc)
- SQLInsertRowsOperator    (conn: postgres / odbc / adbc) — hook.insert_rows wrapper
- SQLBulkLoadOperator      (conn: postgres only; AdbcHook/OdbcHook.bulk_load is NotImplementedError)
- GenericTransfer          (src+dest: postgres / odbc / adbc) — dest insert_rows
- AdbcArrowTransfer        (custom GenericTransfer dest using adbc_ingest)

Not measured (exist but are not SQL/transfer workloads):
- SQLCheckOperator, SQLColumnCheckOperator, SQLTableCheckOperator,
  SQLValueCheckOperator, SQLIntervalCheckOperator, SQLThresholdCheckOperator
  (data-quality assertions)
- BranchSQLOperator (control flow)
- AnalyticsOperator (DataFusion over files/object storage, not Postgres↔Postgres)
- @task.sql (decorator over SQLExecuteQueryOperator; same execute path)
- PostgresToPostgresOperator / PostgresOperator: not in postgres provider 7.0.2
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.common.sql.operators.sql import (
    SQLBulkLoadOperator,
    SQLExecuteQueryOperator,
    SQLInsertRowsOperator,
)
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
    BenchmarkTimer,
    duration_entry,
    generate_product_data,
    get_bench_row_count,
    product_rows_to_arrow,
    read_operator_duration,
    record_operator_duration,
    save_benchmark_results,
    skipped_entry,
    write_benchmark_json,
    write_task_duration,
)

BULK_TSV = "/opt/airflow/include/data/bench_source.tsv"

OP_TABLES = (
    "bench_op_sql_traditional",
    "bench_op_sql_odbc",
    "bench_op_sql_adbc",
    "bench_op_insert_traditional",
    "bench_op_insert_odbc",
    "bench_op_insert_adbc",
    "bench_op_xfer_traditional",
    "bench_op_xfer_odbc",
    "bench_op_xfer_adbc",
    "bench_op_xfer_arrow_from_pg",
    "bench_op_xfer_arrow_from_adbc",
)

# Tables without SERIAL so SQLBulkLoadOperator's COPY table FROM STDIN matches TSV.
BULK_TABLES = ("bench_op_bulk_traditional",)

# AdbcHook.insert_rows / OdbcHook.insert_rows cannot bind Python float into NUMERIC.
FLOAT8_DEST_TABLES = frozenset(
    {
        "bench_op_xfer_adbc",
        "bench_op_xfer_odbc",
        "bench_op_insert_adbc",
        "bench_op_insert_odbc",
    }
)

INSERT_COLUMNS = [
    "product_name",
    "category",
    "price",
    "quantity",
    "created_at",
]


def _row_count() -> int:
    return get_bench_row_count()


def setup_operator_tables(**context):
    """Create dest tables, load bench_source, write TSV for SQLBulkLoadOperator. Not timed."""
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
    for table in OP_TABLES:
        hook.run(create_sql.format(table=table))
        hook.run(f"TRUNCATE TABLE {table};")
    for table in BULK_TABLES:
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                product_name VARCHAR(100),
                category VARCHAR(50),
                price NUMERIC(10, 2),
                quantity INTEGER,
                created_at TIMESTAMP
            )
            """
        )
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

    os.makedirs(os.path.dirname(BULK_TSV), exist_ok=True)
    hook.copy_expert(
        "COPY (SELECT product_name, category, price, quantity, created_at "
        "FROM bench_source) TO STDOUT",
        BULK_TSV,
    )

    for table in FLOAT8_DEST_TABLES:
        hook.run(
            f"ALTER TABLE {table} ALTER COLUMN price TYPE DOUBLE PRECISION "
            f"USING price::float8"
        )
    print(f"Operator bench source ready: {row_count} rows; TSV {BULK_TSV}")


def run_sql_insert_rows(conn_id: str, table: str, duration_task_id: str, **context):
    """Run stock SQLInsertRowsOperator.execute on runtime-generated rows.

    Rows are built before the timer so DAG parse does not materialize N=1e6.
    Duration is execute() only (preoperator TRUNCATE + insert_rows).
    """
    rows = generate_product_data(_row_count())
    op = SQLInsertRowsOperator(
        task_id=f"{duration_task_id}_inner",
        table_name=table,
        conn_id=conn_id,
        columns=INSERT_COLUMNS,
        rows=rows,
        preoperator=f"TRUNCATE TABLE {table}",
    )
    with BenchmarkTimer() as timer:
        op.execute(context)
    duration = timer.get_duration()
    write_task_duration(duration_task_id, duration)
    print(f"SQLInsertRowsOperator {conn_id} -> {table}: {duration:.3f}s")
    return duration


def save_operator_results(**context):
    """Persist TaskInstance / execute() durations for the operator matrix."""
    row_count = _row_count()

    def _duration(task_id: str) -> float:
        return read_operator_duration(task_id)

    def _entry(task_id: str):
        return duration_entry(_duration(task_id), row_count)

    sql_pg = _duration("sql_execute_traditional")
    sql_odbc = _duration("sql_execute_odbc")
    sql_adbc = _duration("sql_execute_adbc")
    save_benchmark_results(
        benchmark_name="bench_sql_execute",
        traditional_time=sql_pg,
        traditional_rows=row_count,
        adbc_time=sql_adbc,
        adbc_rows=row_count,
        metadata={
            "layer": "operator",
            "operator": "SQLExecuteQueryOperator",
            "traditional_conn": "postgres_traditional",
            "adbc_conn": "postgres_adbc",
            "odbc_conn": "postgres_odbc",
            "odbc_seconds": sql_odbc,
            "note": "Server-side INSERT...SELECT; hook overhead is connection + execute only.",
        },
    )

    ins_pg = _duration("sql_insert_rows_traditional")
    ins_odbc = _duration("sql_insert_rows_odbc")
    ins_adbc = _duration("sql_insert_rows_adbc")
    save_benchmark_results(
        benchmark_name="bench_sql_insert_rows",
        traditional_time=ins_pg,
        traditional_rows=row_count,
        adbc_time=ins_adbc,
        adbc_rows=row_count,
        metadata={
            "layer": "operator",
            "operator": "SQLInsertRowsOperator",
            "traditional_conn": "postgres_traditional",
            "adbc_conn": "postgres_adbc",
            "odbc_conn": "postgres_odbc",
            "odbc_seconds": ins_odbc,
            "note": "Stock operator wrapping hook.insert_rows from Python tuples.",
        },
    )

    bulk_pg = _duration("sql_bulk_load_traditional")
    bulk_skip = skipped_entry(
        "hook.bulk_load raises NotImplementedError (only PostgresHook implements COPY)"
    )
    write_benchmark_json(
        "bench_sql_bulk_load",
        {
            "row_count": row_count,
            "postgres": duration_entry(bulk_pg, row_count),
            "odbc": bulk_skip,
            "adbc": bulk_skip,
            "metadata": {
                "layer": "operator",
                "operator": "SQLBulkLoadOperator",
                "note": "PostgresHook.bulk_load is COPY FROM STDIN. AdbcHook and OdbcHook inherit DbApiHook.bulk_load = NotImplementedError.",
            },
        },
    )

    gt_trad = _duration("generic_transfer_traditional")
    gt_odbc = _duration("generic_transfer_odbc")
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
            "odbc_conn": "postgres_odbc",
            "odbc_seconds": gt_odbc,
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
            "stock_generic_transfer_odbc": duration_entry(gt_odbc, row_count),
            "stock_generic_transfer_adbc_insert_rows": duration_entry(gt_adbc, row_count),
            "adbc_arrow_transfer_from_traditional": duration_entry(arrow_from_pg, row_count),
            "adbc_arrow_transfer_from_adbc": duration_entry(arrow_from_adbc, row_count),
            "speedup_vs_stock_adbc_insert_rows": round(gt_adbc / arrow_from_adbc, 2)
            if arrow_from_adbc > 0
            else 0,
            "metadata": {
                "layer": "operator",
                "note": (
                    "Stock GenericTransfer uses dest insert_rows on postgres, odbc, or adbc. "
                    "AdbcArrowTransfer dest-writes with cursor.adbc_ingest."
                ),
            },
        },
    )

    write_benchmark_json(
        "bench_operator_matrix",
        {
            "row_count": row_count,
            "sql_execute_query_operator": {
                "postgres": _entry("sql_execute_traditional"),
                "odbc": _entry("sql_execute_odbc"),
                "adbc": _entry("sql_execute_adbc"),
            },
            "sql_insert_rows_operator": {
                "postgres": _entry("sql_insert_rows_traditional"),
                "odbc": _entry("sql_insert_rows_odbc"),
                "adbc": _entry("sql_insert_rows_adbc"),
            },
            "sql_bulk_load_operator": {
                "postgres": duration_entry(bulk_pg, row_count),
                "odbc": bulk_skip,
                "adbc": bulk_skip,
            },
            "generic_transfer": {
                "postgres": duration_entry(gt_trad, row_count),
                "odbc": duration_entry(gt_odbc, row_count),
                "adbc": duration_entry(gt_adbc, row_count),
            },
            "adbc_arrow_transfer": {
                "from_postgres": duration_entry(arrow_from_pg, row_count),
                "from_adbc": duration_entry(arrow_from_adbc, row_count),
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
    description=(
        "Stock common.sql SQL+transfer operators vs ODBC vs ADBC/Arrow "
        "(SQLExecuteQuery, SQLInsertRows, SQLBulkLoad, GenericTransfer, AdbcArrowTransfer)"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "odbc", "operator"],
) as dag:
    setup = PythonOperator(
        task_id="setup_operator_tables",
        python_callable=setup_operator_tables,
    )

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
    extract_sql_float8 = (
        "SELECT product_name, category, price::float8 AS price, quantity, created_at "
        f"FROM bench_source LIMIT {row_limit}"
    )
    insert_fields = {"target_fields": INSERT_COLUMNS}

    sql_traditional = SQLExecuteQueryOperator(
        task_id="sql_execute_traditional",
        conn_id="postgres_traditional",
        sql=insert_select_sql.format(table="bench_op_sql_traditional"),
        on_success_callback=record_operator_duration,
    )
    sql_odbc = SQLExecuteQueryOperator(
        task_id="sql_execute_odbc",
        conn_id="postgres_odbc",
        sql=insert_select_sql.format(table="bench_op_sql_odbc"),
        on_success_callback=record_operator_duration,
    )
    sql_adbc = SQLExecuteQueryOperator(
        task_id="sql_execute_adbc",
        conn_id="postgres_adbc",
        sql=insert_select_sql.format(table="bench_op_sql_adbc"),
        on_success_callback=record_operator_duration,
    )

    insert_traditional = PythonOperator(
        task_id="sql_insert_rows_traditional",
        python_callable=run_sql_insert_rows,
        op_kwargs={
            "conn_id": "postgres_traditional",
            "table": "bench_op_insert_traditional",
            "duration_task_id": "sql_insert_rows_traditional",
        },
    )
    insert_odbc = PythonOperator(
        task_id="sql_insert_rows_odbc",
        python_callable=run_sql_insert_rows,
        op_kwargs={
            "conn_id": "postgres_odbc",
            "table": "bench_op_insert_odbc",
            "duration_task_id": "sql_insert_rows_odbc",
        },
    )
    insert_adbc = PythonOperator(
        task_id="sql_insert_rows_adbc",
        python_callable=run_sql_insert_rows,
        op_kwargs={
            "conn_id": "postgres_adbc",
            "table": "bench_op_insert_adbc",
            "duration_task_id": "sql_insert_rows_adbc",
        },
    )

    bulk_traditional = SQLBulkLoadOperator(
        task_id="sql_bulk_load_traditional",
        conn_id="postgres_traditional",
        table="bench_op_bulk_traditional",
        tmp_file=BULK_TSV,
        preoperator="TRUNCATE TABLE bench_op_bulk_traditional",
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
    xfer_odbc = GenericTransfer(
        task_id="generic_transfer_odbc",
        source_conn_id="postgres_odbc",
        destination_conn_id="postgres_odbc",
        destination_table="bench_op_xfer_odbc",
        sql=extract_sql_float8,
        preoperator="TRUNCATE TABLE bench_op_xfer_odbc",
        insert_args=insert_fields,
        on_success_callback=record_operator_duration,
    )
    xfer_adbc = GenericTransfer(
        task_id="generic_transfer_adbc",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_op_xfer_adbc",
        sql=extract_sql_float8,
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

    # Serialize timed tasks so LocalExecutor does not overlap 1e6-row jobs.
    timed = [
        sql_traditional,
        sql_odbc,
        sql_adbc,
        insert_traditional,
        insert_odbc,
        insert_adbc,
        bulk_traditional,
        xfer_traditional,
        xfer_odbc,
        xfer_adbc,
        arrow_from_traditional,
        arrow_from_adbc,
    ]
    setup >> timed[0]
    for left, right in zip(timed, timed[1:]):
        left >> right
    timed[-1] >> save
