#!/bin/bash
set -e

CONTAINER_ID=$(docker compose ps -q airflow-apiserver)
if [ -z "$CONTAINER_ID" ]; then
    CONTAINER_ID=$(docker compose ps -q airflow-webserver)
fi

if [ -z "$CONTAINER_ID" ]; then
    echo "Airflow api-server container not found. Run 'docker compose up -d' first."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$REPO_ROOT/.env"
    set +a
fi

echo "Starting ADBC vs Traditional Hooks Benchmark"
echo "================================================"
echo "BENCH_ROW_COUNT=${BENCH_ROW_COUNT:-1000000} (from .env / compose default 1000000)"
echo "Reproduce: cp .env.example .env && docker compose up -d --build && ./scripts/wait_for_airflow.sh && ./scripts/setup_connections.sh && ./scripts/run_bench.sh"
echo ""

# Harder default N (1e6) can take well over 5 minutes per DAG on a laptop/VM.
MAX_WAIT="${BENCH_DAG_TIMEOUT_SEC:-1800}"

# Quiet CLI: Airflow prints plugin INFO / urllib3 warnings on stdout.
airflow_cli() {
    docker exec \
        -e PYTHONWARNINGS=ignore \
        -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR \
        "$CONTAINER_ID" \
        airflow "$@"
}

parse_run_id() {
    # AF 2.11 --output json is a one-element array with dag_run_id.
    # AF 3 may emit an object with run_id / dag_run_id.
    jq -r '
        (if type == "array" then .[0] else . end) as $r
        | ($r.dag_run_id // $r.run_id // empty)
    '
}

# Function to trigger DAG and wait
trigger_and_wait() {
    local dag_id=$1
    local dag_name=$2
    
    echo "Running $dag_name..."
    
    # Trigger DAG
    local trigger_json
    trigger_json=$(airflow_cli dags trigger "$dag_id" --output json)
    run_id=$(printf '%s\n' "$trigger_json" | grep -E '^[\[{]' | parse_run_id)
    
    if [ -z "$run_id" ]; then
        echo "Failed to trigger $dag_id"
        printf '%s\n' "$trigger_json" | tail -20
        return 1
    fi
    
    echo "   Run ID: $run_id"
    echo "   Waiting for completion (timeout ${MAX_WAIT}s)..."
    
    elapsed=0
    while [ $elapsed -lt "$MAX_WAIT" ]; do
        # Airflow 3: `dags state DAG_ID RUN_ID`. AF 2.11 took execution_date and
        # `list-runs -d`; AF 3 list-runs wants a positional dag_id.
        state=$(
            airflow_cli dags state "$dag_id" "$run_id" 2>/dev/null \
                | grep -E '^(queued|running|success|failed|up_for_retry|up_for_reschedule|restarting)$' \
                | tail -1
        )
        if [ -z "$state" ]; then
            state=$(
                airflow_cli dags list-runs -o json "$dag_id" 2>/dev/null \
                    | grep -E '^[\[{]' \
                    | jq -r --arg id "$run_id" '
                        (if type == "array" then . else [.] end)[]
                        | select((.run_id // .dag_run_id) == $id)
                        | .state
                      ' \
                    | head -1
            )
        fi
        state=${state:-running}
        
        if [ "$state" = "success" ]; then
            echo "   Completed successfully"
            return 0
        elif [ "$state" = "failed" ]; then
            echo "   Failed"
            return 1
        fi
        
        sleep 5
        elapsed=$((elapsed + 5))
        printf "."
    done
    
    echo ""
    echo "   Timeout waiting for completion after ${MAX_WAIT}s"
    return 1
}

# Unpause DAGs
echo "Unpausing DAGs..."
airflow_cli dags unpause bench_bulk_insert
airflow_cli dags unpause bench_bulk_select
airflow_cli dags unpause bench_transfer
airflow_cli dags unpause bench_sql_operators
airflow_cli dags unpause bench_duckdb
airflow_cli dags unpause bench_sqlite
echo ""

failures=0

# Run benchmarks; continue so later DAGs still produce JSON if one fails.
trigger_and_wait "bench_bulk_insert" "Bulk Insert Benchmark (hook, Postgres)" || failures=$((failures + 1))
echo ""

trigger_and_wait "bench_bulk_select" "Bulk Select Benchmark (hook, Postgres)" || failures=$((failures + 1))
echo ""

trigger_and_wait "bench_transfer" "Transfer Benchmark (hook ETL, Postgres)" || failures=$((failures + 1))
echo ""

trigger_and_wait "bench_sql_operators" "SQL operators (SQLExecuteQueryOperator + GenericTransfer + AdbcArrowTransfer)" || failures=$((failures + 1))
echo ""

trigger_and_wait "bench_duckdb" "DuckDB file-backed (duckdb API vs ADBC)" || failures=$((failures + 1))
echo ""

trigger_and_wait "bench_sqlite" "SQLite file-backed (sqlite3 vs ADBC)" || failures=$((failures + 1))
echo ""

# Display results
echo "================================================"
echo "BENCHMARK RESULTS"
echo "================================================"
echo ""

print_pair() {
    local file=$1
    local title=$2
    if [ -f "$file" ]; then
        local rows
        rows=$(jq -r '.traditional.rows_processed // .adbc.rows_processed // "?"' "$file")
        echo "--- $title ($rows rows) ---"
        jq -r '
            "Traditional: \(.traditional.duration_seconds)s  \(.traditional.rows_per_second) rows/s",
            "ADBC:        \(.adbc.duration_seconds)s  \(.adbc.rows_per_second) rows/s",
            "Speedup:     \(.speedup)x"
        ' "$file"
        echo ""
    fi
}

echo "### Hook-level (PythonOperator; not an Airflow transfer operator)"
print_pair "include/results/bench_bulk_insert.json" "Bulk Insert (hook, Postgres)"
print_pair "include/results/bench_bulk_select.json" "Bulk Select (hook, Postgres)"
print_pair "include/results/bench_transfer.json" "Hook ETL (Python callable + 10% discount; not 1:1 vs GenericTransfer)"

echo "### Operator-level: SQLExecuteQueryOperator"
print_pair "include/results/bench_sql_execute.json" "SQLExecuteQueryOperator (Postgres)"

echo "### Operator-level: stock GenericTransfer vs AdbcArrowTransfer"
print_pair "include/results/bench_generic_transfer.json" "Stock GenericTransfer (dest insert_rows)"

if [ -f include/results/bench_arrow_transfer.json ]; then
    echo "--- Stock GenericTransfer vs AdbcArrowTransfer (same operator interface) ---"
    jq -r '
        "Stock GenericTransfer traditional: \(.stock_generic_transfer_traditional.duration_seconds)s  \(.stock_generic_transfer_traditional.rows_per_second) rows/s",
        "Stock GenericTransfer ADBC dest:   \(.stock_generic_transfer_adbc_insert_rows.duration_seconds)s  \(.stock_generic_transfer_adbc_insert_rows.rows_per_second) rows/s",
        "AdbcArrowTransfer traditional src: \(.adbc_arrow_transfer_from_traditional.duration_seconds)s  \(.adbc_arrow_transfer_from_traditional.rows_per_second) rows/s",
        "AdbcArrowTransfer ADBC source:     \(.adbc_arrow_transfer_from_adbc.duration_seconds)s  \(.adbc_arrow_transfer_from_adbc.rows_per_second) rows/s",
        "Speedup vs stock ADBC insert_rows: \(.speedup_vs_stock_adbc_insert_rows)x"
    ' include/results/bench_arrow_transfer.json
    echo ""
fi

if [ -f include/results/bench_transfer_arrow_operator.json ]; then
    echo "### Non-comparable footnotes (different workload / DAG context; do not rank vs stock GenericTransfer)"
    jq -r '
        "Hook ETL ADBC (Python callable, extract+compute+ingest): \(.hook_etl_adbc.duration_seconds)s  \(.hook_etl_adbc.rows_per_second) rows/s",
        "AdbcArrowTransfer in bench_transfer (operator copy, no transform): \(.adbc_arrow_transfer.duration_seconds)s  \(.adbc_arrow_transfer.rows_per_second) rows/s"
    ' include/results/bench_transfer_arrow_operator.json
    echo ""
fi

echo "### File engines"
print_pair "include/results/bench_duckdb_insert.json" "DuckDB insert (file)"
print_pair "include/results/bench_duckdb_select.json" "DuckDB select (file)"
print_pair "include/results/bench_sqlite_insert.json" "SQLite insert (file)"
print_pair "include/results/bench_sqlite_select.json" "SQLite select (file)"

echo "================================================"
if [ "$failures" -gt 0 ]; then
    echo "Benchmark finished with $failures failed DAG(s)."
else
    echo "Benchmark complete."
fi
echo ""
echo "Results saved in: include/results/"
echo "View in Airflow UI: http://localhost:8080"

if [ "$failures" -gt 0 ]; then
    exit 1
fi
