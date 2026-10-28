#!/bin/bash
set -e

CONTAINER_ID=$(docker compose ps -q airflow-webserver)

if [ -z "$CONTAINER_ID" ]; then
    echo "❌ Airflow webserver container not found. Run 'docker compose up -d' first."
    exit 1
fi

echo "🚀 Starting ADBC vs Traditional Hooks Benchmark"
echo "================================================"
echo ""

# Function to trigger DAG and wait
trigger_and_wait() {
    local dag_id=$1
    local dag_name=$2
    
    echo "▶️  Running $dag_name..."
    
    # Trigger DAG
    run_id=$(docker exec $CONTAINER_ID airflow dags trigger $dag_id --output json | jq -r '.run_id // empty')
    
    if [ -z "$run_id" ]; then
        echo "❌ Failed to trigger $dag_id"
        return 1
    fi
    
    echo "   Run ID: $run_id"
    echo "   Waiting for completion..."
    
    # Wait for completion
    max_wait=300  # 5 minutes
    elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        state=$(docker exec $CONTAINER_ID airflow dags state $dag_id $run_id 2>/dev/null || echo "running")
        
        if [ "$state" = "success" ]; then
            echo "   ✅ Completed successfully"
            return 0
        elif [ "$state" = "failed" ]; then
            echo "   ❌ Failed"
            return 1
        fi
        
        sleep 5
        elapsed=$((elapsed + 5))
        printf "."
    done
    
    echo ""
    echo "   ⚠️  Timeout waiting for completion"
    return 1
}

# Unpause DAGs
echo "Unpausing DAGs..."
docker exec $CONTAINER_ID airflow dags unpause bench_bulk_insert
docker exec $CONTAINER_ID airflow dags unpause bench_bulk_select
docker exec $CONTAINER_ID airflow dags unpause bench_transfer
echo ""

# Run benchmarks
trigger_and_wait "bench_bulk_insert" "Bulk Insert Benchmark"
echo ""

trigger_and_wait "bench_bulk_select" "Bulk Select Benchmark"
echo ""

trigger_and_wait "bench_transfer" "Transfer Benchmark"
echo ""

# Display results
echo "================================================"
echo "📊 BENCHMARK RESULTS"
echo "================================================"
echo ""

if [ -f "include/results/bench_bulk_insert.json" ]; then
    echo "--- Bulk Insert (100,000 rows) ---"
    cat include/results/bench_bulk_insert.json | jq -r '
        "Traditional (PostgresHook):",
        "  Time: \(.traditional.duration_seconds)s",
        "  Rows/sec: \(.traditional.rows_per_second)",
        "",
        "ADBC (Arrow batches):",
        "  Time: \(.adbc.duration_seconds)s", 
        "  Rows/sec: \(.adbc.rows_per_second)",
        "",
        "Speedup: \(.speedup)x faster"
    '
    echo ""
fi

if [ -f "include/results/bench_bulk_select.json" ]; then
    echo "--- Bulk Select (100,000 rows) ---"
    cat include/results/bench_bulk_select.json | jq -r '
        "Traditional (row fetch):",
        "  Time: \(.traditional.duration_seconds)s",
        "  Rows/sec: \(.traditional.rows_per_second)",
        "",
        "ADBC (Arrow table):",
        "  Time: \(.adbc.duration_seconds)s",
        "  Rows/sec: \(.adbc.rows_per_second)",
        "",
        "Speedup: \(.speedup)x faster"
    '
    echo ""
fi

if [ -f "include/results/bench_transfer.json" ]; then
    echo "--- Transfer (50,000 rows) ---"
    cat include/results/bench_transfer.json | jq -r '
        "Traditional (row-by-row):",
        "  Time: \(.traditional.duration_seconds)s",
        "  Rows/sec: \(.traditional.rows_per_second)",
        "",
        "ADBC (Arrow native):",
        "  Time: \(.adbc.duration_seconds)s",
        "  Rows/sec: \(.adbc.rows_per_second)",
        "",
        "Speedup: \(.speedup)x faster"
    '
    echo ""
fi

echo "================================================"
echo "✅ Benchmark complete!"
echo ""
echo "Results saved in: include/results/"
echo "View in Airflow UI: http://localhost:8080"
