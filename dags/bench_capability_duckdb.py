"""
Benchmark: AIP Phase-2 multi-engine RecordBatch transfer (DuckDB ↔ Postgres).

Local stubs only (include/protocols, include/adapters, include/operators).
Does not modify apache/airflow.

Timed paths (same extract SQL / same N, serialized):
- DuckDB -> DuckDB via DuckDBArrowCapabilityAdapter (same-file table copy;
  one connection because DuckDB takes an exclusive file lock)
- DuckDB -> Postgres Adbc (native DuckDB Arrow adapter -> Adbc ingest)
- Postgres Adbc -> DuckDB (Adbc RecordBatches -> native DuckDB from_arrow)

Post-load (not timed): COUNT == N and payload digest. Postgres dest is
compared to bench_source. DuckDB dests are compared to the DuckDB source
table (same Arrow load as Postgres during setup).

DuckDB row-path (executemany of 1e6 tuples) is not timed here; see
bench_duckdb traditional insert and the skipped JSON entry.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from adapters.adbc import wrap_sql_hook
from adapters.duckdb import DuckDBArrowCapabilityAdapter
from operators.capability_generic_transfer import transfer_with_capabilities
from utils.benchmark_utils import (
    BenchmarkTimer,
    duration_entry,
    get_bench_row_count,
    product_rows_to_arrow,
    read_operator_duration,
    skipped_entry,
    write_benchmark_json,
    write_task_duration,
)
from utils.local_engines import duckdb_cap_path
from utils.verify_load import LoadVerifyError, sql_ident, verify_copy_vs_source

PG_DEST_TABLE = "bench_cap_ddb_to_pg"
DDB_SRC_TABLE = "bench_source"
DDB_TO_DDB_TABLE = "bench_cap_ddb_to_ddb"
PG_TO_DDB_TABLE = "bench_cap_pg_to_ddb"

INSERT_COLUMNS = [
    "product_name",
    "category",
    "price",
    "quantity",
    "created_at",
]

CREATE_PG_DEST = f"""
CREATE TABLE IF NOT EXISTS {PG_DEST_TABLE} (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
)
"""

CREATE_DDB_PAYLOAD = """
CREATE TABLE IF NOT EXISTS {table} (
    product_name VARCHAR,
    category VARCHAR,
    price DECIMAL(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
)
"""


class _PrintLog:
    """Minimal logger so transfer_with_capabilities can print the path taken."""

    def info(self, msg, *args):
        print(msg % args if args else msg)


def _row_count() -> int:
    return get_bench_row_count()


def _extract_sql(table: str, row_count: int) -> str:
    ident = sql_ident(table)
    return (
        "SELECT product_name, category, price, quantity, created_at "
        f"FROM {ident} LIMIT {int(row_count)}"
    )


def _duckdb_digest(con, table: str) -> dict:
    """COUNT + SUM(quantity) + ordered md5, evaluated inside DuckDB."""
    ident = sql_ident(table)
    row = con.execute(
        "SELECT COUNT(*)::BIGINT AS n, "
        "COALESCE(SUM(quantity), 0)::BIGINT AS sum_quantity, "
        "md5(COALESCE(string_agg(row_md5, '' ORDER BY product_name), '')) AS digest "
        "FROM ("
        "SELECT product_name, quantity, md5(CONCAT_WS(CHR(31), "
        "product_name, category, "
        "CAST(CAST(price AS DECIMAL(10, 2)) AS VARCHAR), "
        "CAST(quantity AS VARCHAR), "
        "COALESCE(strftime(created_at, '%Y-%m-%d %H:%M:%S.%f'), '')"
        ")) AS row_md5 "
        f"FROM {ident}"
        ") AS hashed"
    ).fetchone()
    return {
        "table": table,
        "n": int(row[0]),
        "sum_quantity": int(row[1]),
        "digest": row[2],
    }


def _assert_duckdb_copy(con, source_table: str, dest_table: str, expected: int) -> None:
    src = _duckdb_digest(con, source_table)
    dest = _duckdb_digest(con, dest_table)
    if src["n"] != expected:
        raise LoadVerifyError(
            f"DuckDB {source_table} COUNT(*)={src['n']}, expected {expected}"
        )
    if dest["n"] != expected:
        raise LoadVerifyError(
            f"DuckDB {dest_table} COUNT(*)={dest['n']}, expected {expected}"
        )
    if (
        src["sum_quantity"] != dest["sum_quantity"]
        or src["digest"] != dest["digest"]
    ):
        raise LoadVerifyError(
            f"DuckDB payload mismatch {source_table} vs {dest_table}: "
            f"{src} != {dest}"
        )
    print(
        f"verify DuckDB payload {source_table} == {dest_table} "
        f"(n={src['n']}, sum_quantity={src['sum_quantity']}, digest={src['digest']})"
    )


def setup_capability_duckdb(**context):
    """Load the same Arrow table into Postgres and DuckDB sources. Not timed."""
    import duckdb

    row_count = _row_count()
    path = duckdb_cap_path()
    if os.path.exists(path):
        os.remove(path)
    wal = path + ".wal"
    if os.path.exists(wal):
        os.remove(wal)

    hook = PostgresHook(postgres_conn_id="postgres_traditional")
    hook.run(CREATE_PG_DEST)
    hook.run("TRUNCATE TABLE bench_source;")
    hook.run(f"TRUNCATE TABLE {PG_DEST_TABLE};")

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

    con = duckdb.connect(path)
    try:
        for table in (DDB_SRC_TABLE, DDB_TO_DDB_TABLE, PG_TO_DDB_TABLE):
            con.execute(CREATE_DDB_PAYLOAD.format(table=table))
            con.execute(f"DELETE FROM {sql_ident(table)}")
        con.from_arrow(arrow_table).insert_into(DDB_SRC_TABLE)
        n = con.execute(f"SELECT COUNT(*) FROM {sql_ident(DDB_SRC_TABLE)}").fetchone()[0]
        print(f"DuckDB capability source ready: {n} rows at {path}")
    finally:
        con.close()
    print(f"Capability DuckDB bench source ready: {row_count} rows")


def run_duckdb_to_duckdb(**context):
    """Columnar DuckDB -> DuckDB (shared connection; exclusive file lock)."""
    import duckdb

    row_count = _row_count()
    con = duckdb.connect(duckdb_cap_path())
    try:
        adapter = DuckDBArrowCapabilityAdapter(connection=con)
        with BenchmarkTimer() as timer:
            path = transfer_with_capabilities(
                adapter,
                adapter,
                sql=_extract_sql(DDB_SRC_TABLE, row_count),
                destination_table=DDB_TO_DDB_TABLE,
                insert_args={"target_fields": INSERT_COLUMNS},
                preoperator=f"DELETE FROM {DDB_TO_DDB_TABLE}",
                log=_PrintLog(),
            )
        duration = timer.get_duration()
    finally:
        con.close()
    write_task_duration("capability_duckdb_to_duckdb", duration)
    print(f"DuckDB -> DuckDB columnar ({path}): {duration:.3f}s")
    return duration


def run_duckdb_to_postgres(**context):
    """Columnar DuckDB native adapter -> Postgres Adbc ingest."""
    row_count = _row_count()
    source = DuckDBArrowCapabilityAdapter(path=duckdb_cap_path())
    dest = wrap_sql_hook(AdbcHook(adbc_conn_id="postgres_adbc"))
    with BenchmarkTimer() as timer:
        path = transfer_with_capabilities(
            source,
            dest,
            sql=_extract_sql(DDB_SRC_TABLE, row_count),
            destination_table=PG_DEST_TABLE,
            insert_args={"target_fields": INSERT_COLUMNS},
            preoperator=f"TRUNCATE TABLE {PG_DEST_TABLE}",
            log=_PrintLog(),
        )
    duration = timer.get_duration()
    write_task_duration("capability_duckdb_to_postgres", duration)
    print(f"DuckDB -> Postgres Adbc columnar ({path}): {duration:.3f}s")
    return duration


def run_postgres_to_duckdb(**context):
    """Columnar Postgres Adbc -> DuckDB native adapter."""
    row_count = _row_count()
    source = wrap_sql_hook(AdbcHook(adbc_conn_id="postgres_adbc"))
    dest = DuckDBArrowCapabilityAdapter(path=duckdb_cap_path())
    with BenchmarkTimer() as timer:
        path = transfer_with_capabilities(
            source,
            dest,
            sql=_extract_sql("bench_source", row_count),
            destination_table=PG_TO_DDB_TABLE,
            insert_args={"target_fields": INSERT_COLUMNS},
            preoperator=f"DELETE FROM {PG_TO_DDB_TABLE}",
            log=_PrintLog(),
        )
    duration = timer.get_duration()
    write_task_duration("capability_postgres_to_duckdb", duration)
    print(f"Postgres Adbc -> DuckDB columnar ({path}): {duration:.3f}s")
    return duration


def verify_capability_duckdb(**context):
    """COUNT dest rows; Postgres dest matches bench_source; DuckDB dests match source."""
    import duckdb

    expected = _row_count()
    hook = PostgresHook(postgres_conn_id="postgres_traditional")
    verify_copy_vs_source(
        hook,
        "bench_source",
        (PG_DEST_TABLE,),
        expected,
        include_price=True,
    )
    con = duckdb.connect(duckdb_cap_path())
    try:
        _assert_duckdb_copy(con, DDB_SRC_TABLE, DDB_TO_DDB_TABLE, expected)
        _assert_duckdb_copy(con, DDB_SRC_TABLE, PG_TO_DDB_TABLE, expected)
    finally:
        con.close()
    print(
        "Faster is not allowed to mean fewer or different rows "
        "(DuckDB dests vs DuckDB source; Postgres dest vs bench_source)."
    )


def save_capability_duckdb_results(**context):
    """Persist BenchmarkTimer durations for the multi-engine comparison."""
    row_count = _row_count()
    ddb_ddb = read_operator_duration("capability_duckdb_to_duckdb")
    ddb_pg = read_operator_duration("capability_duckdb_to_postgres")
    pg_ddb = read_operator_duration("capability_postgres_to_duckdb")
    write_benchmark_json(
        "bench_capability_duckdb",
        {
            "row_count": row_count,
            "capability_duckdb_to_duckdb": duration_entry(ddb_ddb, row_count),
            "capability_duckdb_to_postgres": duration_entry(ddb_pg, row_count),
            "capability_postgres_to_duckdb": duration_entry(pg_ddb, row_count),
            "stock_row_path_duckdb_insert_rows": skipped_entry(
                "DuckDB executemany of 1e6 Python tuples is not a Phase-2 "
                "baseline (see bench_duckdb traditional insert, ~10+ min)."
            ),
            "metadata": {
                "layer": "operator",
                "aip": "phase-2-local-stub-multi-engine",
                "duckdb_file": duckdb_cap_path(),
                "note": (
                    "DuckDBArrowCapabilityAdapter advertises the same "
                    "RecordBatchReadable/Writable protocols as the Adbc wrapper. "
                    "DuckDB -> DuckDB uses one connection (exclusive file lock). "
                    "DuckDB <-> Postgres streams RecordBatches without a Python "
                    "row materialization. Not mixed into the Postgres Adbc "
                    "both-ends 11.35x claim."
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
    "bench_capability_duckdb",
    default_args=default_args,
    description=(
        "AIP Phase-2 multi-engine demo: DuckDB <-> DuckDB and "
        "DuckDB <-> Postgres RecordBatch CapabilityGenericTransfer helper"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "adbc", "duckdb", "transfer", "aip"],
) as dag:
    setup = PythonOperator(
        task_id="setup_capability_duckdb",
        python_callable=setup_capability_duckdb,
    )
    ddb_to_ddb = PythonOperator(
        task_id="capability_duckdb_to_duckdb",
        python_callable=run_duckdb_to_duckdb,
    )
    ddb_to_pg = PythonOperator(
        task_id="capability_duckdb_to_postgres",
        python_callable=run_duckdb_to_postgres,
    )
    pg_to_ddb = PythonOperator(
        task_id="capability_postgres_to_duckdb",
        python_callable=run_postgres_to_duckdb,
    )
    verify = PythonOperator(
        task_id="verify_capability_duckdb",
        python_callable=verify_capability_duckdb,
    )
    save = PythonOperator(
        task_id="save_results",
        python_callable=save_capability_duckdb_results,
    )

    # Serialize timed tasks so LocalExecutor does not overlap 1e6-row jobs,
    # and so the DuckDB file is not opened by two writers at once.
    setup >> ddb_to_ddb >> ddb_to_pg >> pg_to_ddb >> verify >> save
