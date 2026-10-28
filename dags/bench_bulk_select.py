"""
Benchmark: Bulk Select Comparison

Compares traditional PostgresHook vs AdbcHook for fetching large result sets.
"""

from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'include'))
from utils.benchmark_utils import (
    BenchmarkTimer,
    get_bench_row_count,
    product_rows_to_arrow,
    save_benchmark_results,
)


def populate_test_data(**context):
    """Populate a table with test data for SELECT benchmarks."""
    row_count = get_bench_row_count()
    
    print(f"🔧 Populating test data: {row_count:,} rows...")
    
    hook = PostgresHook(postgres_conn_id='postgres_traditional')
    
    # Truncate and populate
    hook.run("TRUNCATE TABLE bench_source;")
    
    from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

    arrow_table = product_rows_to_arrow(row_count)
    adbc_hook = AdbcHook(adbc_conn_id='postgres_adbc')
    with adbc_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.adbc_ingest(
                table_name='bench_source',
                data=arrow_table,
                mode='append'
            )
        conn.commit()
    
    print(f"✅ Test data ready: {row_count:,} rows")


def bench_select_traditional(**context):
    """Fetch rows using traditional PostgresHook."""
    row_count = get_bench_row_count()
    
    print(f"📊 Traditional SELECT: fetching {row_count:,} rows")
    
    hook = PostgresHook(postgres_conn_id='postgres_traditional')
    
    with BenchmarkTimer() as timer:
        # Traditional row-based fetch
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM bench_source LIMIT {row_count}")
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        fetched_count = len(rows)
    
    duration = timer.get_duration()
    rows_per_sec = fetched_count / duration if duration > 0 else 0
    
    print(f"⏱️  Traditional completed: {duration:.3f}s ({rows_per_sec:.0f} rows/s)")
    print(f"   Fetched {fetched_count:,} rows as list of tuples")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='traditional_duration', value=duration)
    context['task_instance'].xcom_push(key='traditional_rows', value=fetched_count)
    
    return duration


def bench_select_adbc(**context):
    """Fetch rows using ADBC (Arrow table)."""
    row_count = get_bench_row_count()
    
    print(f"📊 ADBC SELECT: fetching {row_count:,} rows as Arrow table")
    
    hook = AdbcHook(adbc_conn_id='postgres_adbc')
    
    with BenchmarkTimer() as timer:
        # ADBC Arrow-based fetch
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM bench_source LIMIT {row_count}")
                arrow_table = cursor.fetch_arrow_table()
        
        fetched_count = len(arrow_table)
    
    duration = timer.get_duration()
    rows_per_sec = fetched_count / duration if duration > 0 else 0
    
    print(f"⏱️  ADBC completed: {duration:.3f}s ({rows_per_sec:.0f} rows/s)")
    print(f"   Fetched {fetched_count:,} rows as Arrow table")
    print(f"   Arrow table schema: {arrow_table.schema}")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='adbc_duration', value=duration)
    context['task_instance'].xcom_push(key='adbc_rows', value=fetched_count)
    
    return duration


def save_results(**context):
    """Save benchmark results to file."""
    ti = context['task_instance']
    
    traditional_duration = ti.xcom_pull(task_ids='select_traditional', key='traditional_duration')
    traditional_rows = ti.xcom_pull(task_ids='select_traditional', key='traditional_rows')
    adbc_duration = ti.xcom_pull(task_ids='select_adbc', key='adbc_duration')
    adbc_rows = ti.xcom_pull(task_ids='select_adbc', key='adbc_rows')
    
    results = save_benchmark_results(
        benchmark_name="bench_bulk_select",
        traditional_time=traditional_duration,
        traditional_rows=traditional_rows,
        adbc_time=adbc_duration,
        adbc_rows=adbc_rows,
        metadata={
            "method": "bulk_select",
            "database": "PostgreSQL 15",
            "format_traditional": "list_of_tuples",
            "format_adbc": "arrow_table"
        }
    )
    
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
    'bench_bulk_select',
    default_args=default_args,
    description='Benchmark: Bulk Select - Traditional vs ADBC',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['benchmark', 'adbc', 'select'],
) as dag:
    
    populate = PythonOperator(
        task_id='populate_test_data',
        python_callable=populate_test_data,
    )
    
    traditional = PythonOperator(
        task_id='select_traditional',
        python_callable=bench_select_traditional,
    )
    
    adbc = PythonOperator(
        task_id='select_adbc',
        python_callable=bench_select_adbc,
    )
    
    save = PythonOperator(
        task_id='save_results',
        python_callable=save_results,
    )
    
    populate >> [traditional, adbc] >> save
