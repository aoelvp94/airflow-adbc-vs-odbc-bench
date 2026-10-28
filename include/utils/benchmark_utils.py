"""Shared utilities for benchmark DAGs."""

import json
import os
import time
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

# Single volume knob for every DAG (insert, select, transfer, operators, DuckDB, SQLite).
# 1_000_000 is 10x the original insert/select default (100k) and 20x the original
# transfer/operator default (50k). Override with Airflow Variable or env BENCH_ROW_COUNT.
DEFAULT_ROW_COUNT = 1_000_000
DEFAULT_BATCH_SIZE = 1_000


class BenchmarkTimer:
    """Context manager for timing operations."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
    
    def get_duration(self) -> float:
        """Get duration in seconds."""
        return self.duration if self.duration else 0.0


def save_benchmark_results(
    benchmark_name: str,
    traditional_time: float,
    traditional_rows: int,
    adbc_time: float,
    adbc_rows: int,
    metadata: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Save benchmark results to JSON file.
    
    Args:
        benchmark_name: Name of the benchmark
        traditional_time: Duration for traditional method (seconds)
        traditional_rows: Number of rows processed (traditional)
        adbc_time: Duration for ADBC method (seconds)
        adbc_rows: Number of rows processed (ADBC)
        metadata: Additional metadata to include
    
    Returns:
        Dictionary of benchmark results
    """
    results = {
        "benchmark": benchmark_name,
        "timestamp": datetime.now().isoformat(),
        "traditional": {
            "duration_seconds": round(traditional_time, 3),
            "rows_processed": traditional_rows,
            "rows_per_second": round(traditional_rows / traditional_time, 2) if traditional_time > 0 else 0
        },
        "adbc": {
            "duration_seconds": round(adbc_time, 3),
            "rows_processed": adbc_rows,
            "rows_per_second": round(adbc_rows / adbc_time, 2) if adbc_time > 0 else 0
        },
        "speedup": round(traditional_time / adbc_time, 2) if adbc_time > 0 else 0,
        "metadata": {
            **(metadata or {}),
            "row_count": traditional_rows,
        },
    }
    
    # Save to file
    results_dir = Path("/opt/airflow/include/results")
    results_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = results_dir / f"{benchmark_name}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"✅ Results saved to {output_file}")
    print(json.dumps(results, indent=2))
    
    return results


def get_bench_row_count() -> int:
    """Row count for all benchmark DAGs.

    Resolution: Airflow Variable BENCH_ROW_COUNT, else env BENCH_ROW_COUNT,
    else DEFAULT_ROW_COUNT. Transfer and operator DAGs use this same value.
    """
    env_default = os.environ.get("BENCH_ROW_COUNT", str(DEFAULT_ROW_COUNT))
    from airflow.models import Variable

    return int(Variable.get("BENCH_ROW_COUNT", default_var=env_default))


def get_bench_batch_size() -> int:
    """Traditional Postgres insert batch size (BENCH_BATCH_SIZE)."""
    env_default = os.environ.get("BENCH_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))
    from airflow.models import Variable

    return int(Variable.get("BENCH_BATCH_SIZE", default_var=env_default))


def sample_rows_to_arrow(data, *, created_at_as_string: bool = False):
    """Arrow table for (name, value, created_at, data). value is int32 for PG INTEGER."""
    import pyarrow as pa

    ts_type = pa.string() if created_at_as_string else pa.timestamp("us")
    timestamps = [row[2].isoformat() if created_at_as_string else row[2] for row in data]
    return pa.table(
        {
            "name": pa.array([row[0] for row in data], type=pa.string()),
            "value": pa.array([row[1] for row in data], type=pa.int32()),
            "created_at": pa.array(timestamps, type=ts_type),
            "data": pa.array([row[3] for row in data], type=pa.string()),
        }
    )


def product_rows_to_arrow(num_rows: int):
    """Arrow table matching bench_source (quantity int32, price float64, timestamp us)."""
    import random
    from datetime import datetime

    import pyarrow as pa

    categories = ["Electronics", "Clothing", "Food", "Books", "Toys"]
    now = datetime.now()
    return pa.table(
        {
            "product_name": pa.array(
                [f"Product_{i}" for i in range(num_rows)], type=pa.string()
            ),
            "category": pa.array(
                [random.choice(categories) for _ in range(num_rows)], type=pa.string()
            ),
            "price": pa.array(
                [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
                type=pa.float64(),
            ),
            "quantity": pa.array(
                [random.randint(1, 100) for _ in range(num_rows)], type=pa.int32()
            ),
            "created_at": pa.array([now] * num_rows, type=pa.timestamp("us")),
        }
    )


def generate_product_data(num_rows: int) -> list:
    """Generate (product_name, category, price, quantity, created_at) rows."""
    import random
    from datetime import datetime

    categories = ["Electronics", "Clothing", "Food", "Books", "Toys"]
    now = datetime.now()
    return [
        (
            f"Product_{i}",
            random.choice(categories),
            round(random.uniform(10.0, 1000.0), 2),
            random.randint(1, 100),
            now,
        )
        for i in range(num_rows)
    ]


def generate_sample_data(num_rows: int) -> list:
    """
    Generate sample data for benchmarks.
    
    Args:
        num_rows: Number of rows to generate
    
    Returns:
        List of tuples with sample data
    """
    import random
    from datetime import datetime, timedelta
    
    data = []
    base_date = datetime.now()
    
    for i in range(num_rows):
        data.append((
            f"Record_{i}",
            random.randint(1, 1000),
            base_date - timedelta(seconds=i),
            f"Sample data string for row {i} with some padding to simulate real data"
        ))
    
    return data


def log_benchmark_progress(operation: str, current: int, total: int):
    """Log benchmark progress."""
    if current % 10000 == 0 or current == total:
        percent = (current / total * 100) if total > 0 else 0
        print(f"  {operation}: {current:,}/{total:,} ({percent:.1f}%)")
