"""Shared utilities for benchmark DAGs."""

import json
import time
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


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
        "metadata": metadata or {}
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
