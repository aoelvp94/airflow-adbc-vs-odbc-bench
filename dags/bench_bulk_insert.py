"""
Benchmark: Bulk Insert Comparison

Compares traditional PostgresHook vs AdbcHook for inserting large batches of data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
import pyarrow as pa
import sys
import os

# Add include path for utilities
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'include'))
from utils.benchmark_utils import (
    BenchmarkTimer,
    generate_sample_data,
    get_bench_batch_size,
    get_bench_row_count,
    save_benchmark_results,
)


def setup_tables(**context):
    """Create/truncate benchmark tables."""
    print("🔧 Setting up benchmark tables...")
    
    hook = PostgresHook(postgres_conn_id='postgres_traditional')
    
    # Truncate existing tables
    hook.run("TRUNCATE TABLE bench_insert_traditional;")
    hook.run("TRUNCATE TABLE bench_insert_adbc;")
    
    print("✅ Tables ready")


def bench_insert_traditional(**context):
    """Insert rows using traditional PostgresHook."""
    row_count = get_bench_row_count()
    batch_size = get_bench_batch_size()
    
    print(f"📊 Traditional Insert: {row_count:,} rows (batch size: {batch_size:,})")
    
    hook = PostgresHook(postgres_conn_id='postgres_traditional')
    data = generate_sample_data(row_count)
    
    with BenchmarkTimer() as timer:
        # Insert in batches using traditional method
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            # Traditional row-by-row insert
            hook.insert_rows(
                table='bench_insert_traditional',
                rows=batch,
                target_fields=['name', 'value', 'created_at', 'data'],
                commit_every=batch_size
            )
            
            if (i + batch_size) % 10000 == 0:
                print(f"  Progress: {i + batch_size:,}/{row_count:,}")
    
    duration = timer.get_duration()
    rows_per_sec = row_count / duration if duration > 0 else 0
    
    print(f"⏱️  Traditional completed: {duration:.3f}s ({rows_per_sec:.0f} rows/s)")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='traditional_duration', value=duration)
    context['task_instance'].xcom_push(key='traditional_rows', value=row_count)
    
    return duration


def bench_insert_adbc(**context):
    """Insert rows using ADBC with Arrow batches."""
    row_count = get_bench_row_count()
    
    print(f"📊 ADBC Insert: {row_count:,} rows (Arrow batches)")
    
    # Generate data
    data = generate_sample_data(row_count)
    
    # Convert to Arrow table
    names = [row[0] for row in data]
    values = [row[1] for row in data]
    timestamps = [row[2] for row in data]
    texts = [row[3] for row in data]
    
    arrow_table = pa.table({
        'name': names,
        'value': values,
        'created_at': timestamps,
        'data': texts
    })
    
    print(f"  Arrow table created: {len(arrow_table):,} rows")
    
    # Use ADBC hook
    hook = AdbcHook(adbc_conn_id='postgres_adbc')
    
    with BenchmarkTimer() as timer:
        # Insert using ADBC (direct Arrow transfer)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.adbc_ingest(
                    table_name='bench_insert_adbc',
                    data=arrow_table,
                    mode='append'
                )
    
    duration = timer.get_duration()
    rows_per_sec = row_count / duration if duration > 0 else 0
    
    print(f"⏱️  ADBC completed: {duration:.3f}s ({rows_per_sec:.0f} rows/s)")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='adbc_duration', value=duration)
    context['task_instance'].xcom_push(key='adbc_rows', value=row_count)
    
    return duration


def save_results(**context):
    """Save benchmark results to file."""
    ti = context['task_instance']
    
    traditional_duration = ti.xcom_pull(task_ids='insert_traditional', key='traditional_duration')
    traditional_rows = ti.xcom_pull(task_ids='insert_traditional', key='traditional_rows')
    adbc_duration = ti.xcom_pull(task_ids='insert_adbc', key='adbc_duration')
    adbc_rows = ti.xcom_pull(task_ids='insert_adbc', key='adbc_rows')
    
    batch_size = get_bench_batch_size()
    
    results = save_benchmark_results(
        benchmark_name="bench_bulk_insert",
        traditional_time=traditional_duration,
        traditional_rows=traditional_rows,
        adbc_time=adbc_duration,
        adbc_rows=adbc_rows,
        metadata={
            "method": "bulk_insert",
            "batch_size_traditional": batch_size,
            "database": "PostgreSQL 15"
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
    'bench_bulk_insert',
    default_args=default_args,
    description='Benchmark: Bulk Insert - Traditional vs ADBC',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['benchmark', 'adbc', 'insert'],
) as dag:
    
    setup = PythonOperator(
        task_id='setup_tables',
        python_callable=setup_tables,
    )
    
    traditional = PythonOperator(
        task_id='insert_traditional',
        python_callable=bench_insert_traditional,
    )
    
    adbc = PythonOperator(
        task_id='insert_adbc',
        python_callable=bench_insert_adbc,
    )
    
    save = PythonOperator(
        task_id='save_results',
        python_callable=save_results,
    )
    
    setup >> [traditional, adbc] >> save
