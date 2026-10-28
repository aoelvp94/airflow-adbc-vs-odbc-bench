"""
Benchmark: Data Transfer Comparison

Compares traditional PostgresHook vs AdbcHook for extract-transform-load patterns.

Also runs ``AdbcArrowTransfer`` (operator, Arrow ingest, no transform) so the
suite can compare hook ETL vs stock GenericTransfer vs custom Arrow ingest.
Stock GenericTransfer lives in ``bench_sql_operators``.
"""

from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
import pyarrow as pa
import pyarrow.compute as pc
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'include'))
from operators.adbc_arrow_transfer import AdbcArrowTransfer
from utils.benchmark_utils import (
    DEFAULT_ROW_COUNT,
    BenchmarkTimer,
    arrow_numeric_to_float64,
    duration_entry,
    get_bench_batch_size,
    get_bench_row_count,
    product_rows_to_arrow,
    read_operator_duration,
    record_operator_duration,
    save_benchmark_results,
    write_benchmark_json,
)
from utils.verify_load import assert_payload_equal, verify_counts


def populate_source_data(**context):
    """Populate source table for transfer benchmark."""
    row_count = get_bench_row_count()
    
    print(f"🔧 Populating source data: {row_count:,} rows...")
    
    hook = PostgresHook(postgres_conn_id='postgres_traditional')
    
    # Truncate tables
    hook.run("TRUNCATE TABLE bench_source;")
    hook.run("TRUNCATE TABLE bench_dest_traditional;")
    hook.run("TRUNCATE TABLE bench_dest_adbc;")
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS bench_dest_arrow (
            id SERIAL PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price NUMERIC(10, 2),
            quantity INTEGER,
            created_at TIMESTAMP
        )
        """
    )
    hook.run("TRUNCATE TABLE bench_dest_arrow;")
    
    adbc_hook = AdbcHook(adbc_conn_id='postgres_adbc')
    arrow_table = product_rows_to_arrow(row_count)
    
    with adbc_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.adbc_ingest(
                table_name='bench_source',
                data=arrow_table,
                mode='append'
            )
        conn.commit()
    
    print(f"✅ Source data ready: {row_count:,} rows")


def bench_transfer_traditional(**context):
    """Transfer data using traditional PostgresHook (extract + transform + load)."""
    row_count = get_bench_row_count()
    
    print(f"📊 Traditional Transfer: {row_count:,} rows")
    
    hook = PostgresHook(postgres_conn_id='postgres_traditional')
    
    with BenchmarkTimer() as timer:
        # Extract
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM bench_source LIMIT {row_count}")
        rows = cursor.fetchall()
        
        print(f"  Extracted {len(rows):,} rows")
        
        # Transform (simple transformation: apply 10% discount to prices)
        transformed_rows = []
        for row in rows:
            # row: (id, product_name, category, price, quantity, created_at)
            transformed_rows.append((
                row[1],  # product_name
                row[2],  # category
                round(float(row[3]) * 0.9, 2),  # price with discount
                row[4],  # quantity
                row[5]   # created_at
            ))
        
        print(f"  Transformed {len(transformed_rows):,} rows")
        
        # Load (batch insert)
        batch_size = get_bench_batch_size()
        for i in range(0, len(transformed_rows), batch_size):
            batch = transformed_rows[i:i + batch_size]
            hook.insert_rows(
                table='bench_dest_traditional',
                rows=batch,
                target_fields=['product_name', 'category', 'price', 'quantity', 'created_at'],
                commit_every=batch_size
            )
        
        cursor.close()
        conn.close()
    
    duration = timer.get_duration()
    rows_per_sec = row_count / duration if duration > 0 else 0
    
    print(f"⏱️  Traditional transfer completed: {duration:.3f}s ({rows_per_sec:.0f} rows/s)")
    
    context['task_instance'].xcom_push(key='traditional_duration', value=duration)
    context['task_instance'].xcom_push(key='traditional_rows', value=row_count)
    
    return duration


def bench_transfer_adbc(**context):
    """Transfer data using ADBC (Arrow-native extract + transform + load)."""
    row_count = get_bench_row_count()
    
    print(f"📊 ADBC Transfer: {row_count:,} rows")
    
    hook = AdbcHook(adbc_conn_id='postgres_adbc')
    
    with BenchmarkTimer() as timer:
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Extract as Arrow table
                cursor.execute(f"SELECT * FROM bench_source LIMIT {row_count}")
                arrow_table = cursor.fetch_arrow_table()
                
                print(f"  Extracted {len(arrow_table):,} rows as Arrow table")
                
                # NUMERIC arrives as opaque string; float64 then decimal128 for COPY.
                discounted_prices = pc.round(
                    pc.multiply(arrow_numeric_to_float64(arrow_table["price"]), 0.9),
                    2,
                ).cast(pa.decimal128(10, 2))
                
                transformed_table = pa.table({
                    'product_name': arrow_table['product_name'],
                    'category': arrow_table['category'],
                    'price': discounted_prices,
                    'quantity': arrow_table['quantity'],
                    'created_at': arrow_table['created_at']
                })
                
                print(f"  Transformed {len(transformed_table):,} rows using Arrow compute")
                
                cursor.adbc_ingest(
                    table_name='bench_dest_adbc',
                    data=transformed_table,
                    mode='append'
                )
            conn.commit()
    
    duration = timer.get_duration()
    rows_per_sec = row_count / duration if duration > 0 else 0
    
    print(f"⏱️  ADBC transfer completed: {duration:.3f}s ({rows_per_sec:.0f} rows/s)")
    
    context['task_instance'].xcom_push(key='adbc_duration', value=duration)
    context['task_instance'].xcom_push(key='adbc_rows', value=row_count)
    
    return duration


def verify_transfer_loads(**context):
    """COUNT dest rows; Arrow copy vs source. Hook ETL is COUNT-only. Not timed."""
    expected = get_bench_row_count()
    hook = PostgresHook(postgres_conn_id="postgres_traditional")
    verify_counts(
        hook,
        (
            "bench_source",
            "bench_dest_traditional",
            "bench_dest_adbc",
            "bench_dest_arrow",
        ),
        expected,
    )
    assert_payload_equal(hook, "bench_source", "bench_dest_arrow")
    print(
        "hook ETL dests bench_dest_traditional / bench_dest_adbc: COUNT only. "
        "Both apply a 10% price discount by design; content is not compared to "
        "bench_source, bench_dest_arrow, or GenericTransfer in bench_sql_operators."
    )


def save_results(**context):
    """Save benchmark results to file."""
    ti = context['task_instance']
    
    traditional_duration = ti.xcom_pull(task_ids='transfer_traditional', key='traditional_duration')
    traditional_rows = ti.xcom_pull(task_ids='transfer_traditional', key='traditional_rows')
    adbc_duration = ti.xcom_pull(task_ids='transfer_adbc', key='adbc_duration')
    adbc_rows = ti.xcom_pull(task_ids='transfer_adbc', key='adbc_rows')
    
    results = save_benchmark_results(
        benchmark_name="bench_transfer",
        traditional_time=traditional_duration,
        traditional_rows=traditional_rows,
        adbc_time=adbc_duration,
        adbc_rows=adbc_rows,
        metadata={
            "method": "extract_transform_load",
            "transformation": "apply_10_percent_discount",
            "database": "PostgreSQL 18",
        }
    )
    try:
        arrow_op = read_operator_duration("transfer_arrow_operator")
        write_benchmark_json(
            "bench_transfer_arrow_operator",
            {
                "row_count": traditional_rows,
                "hook_etl_traditional": duration_entry(traditional_duration, traditional_rows),
                "hook_etl_adbc": duration_entry(adbc_duration, adbc_rows),
                "adbc_arrow_transfer": duration_entry(arrow_op, traditional_rows),
                "metadata": {
                    "hook_etl": "PythonOperator + 10% price discount",
                    "adbc_arrow_transfer": "AdbcArrowTransfer, no transform, dest adbc_ingest",
                },
            },
        )
    except RuntimeError as exc:
        print(f"AdbcArrowTransfer duration not recorded: {exc}")
    return results


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'bench_transfer',
    default_args=default_args,
    description='Benchmark: Data Transfer (ETL) plus AdbcArrowTransfer operator',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['benchmark', 'adbc', 'transfer', 'etl'],
) as dag:
    
    populate = PythonOperator(
        task_id='populate_source_data',
        python_callable=populate_source_data,
    )
    
    traditional = PythonOperator(
        task_id='transfer_traditional',
        python_callable=bench_transfer_traditional,
    )
    
    adbc = PythonOperator(
        task_id='transfer_adbc',
        python_callable=bench_transfer_adbc,
    )

    row_limit = int(os.environ.get("BENCH_ROW_COUNT", DEFAULT_ROW_COUNT))
    arrow_op = AdbcArrowTransfer(
        task_id="transfer_arrow_operator",
        source_conn_id="postgres_adbc",
        destination_conn_id="postgres_adbc",
        destination_table="bench_dest_arrow",
        sql=(
            "SELECT product_name, category, price, quantity, created_at "
            f"FROM bench_source LIMIT {row_limit}"
        ),
        preoperator="TRUNCATE TABLE bench_dest_arrow",
        insert_args={
            "target_fields": [
                "product_name",
                "category",
                "price",
                "quantity",
                "created_at",
            ]
        },
        on_success_callback=record_operator_duration,
    )
    
    verify = PythonOperator(
        task_id='verify_transfer_loads',
        python_callable=verify_transfer_loads,
    )
    save = PythonOperator(
        task_id='save_results',
        python_callable=save_results,
    )
    
    populate >> [traditional, adbc, arrow_op] >> verify >> save
