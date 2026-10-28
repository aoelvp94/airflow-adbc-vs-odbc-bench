#!/bin/bash
set -e

echo "Setting up Airflow connections..."

# Host-side defaults; Airflow Variables are the runtime knob the DAGs read.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$REPO_ROOT/.env"
    set +a
fi

BENCH_ROW_COUNT="${BENCH_ROW_COUNT:-1000000}"
BENCH_BATCH_SIZE="${BENCH_BATCH_SIZE:-1000}"

# Wait for webserver to be available
until curl -s http://localhost:8080/health | grep -q "healthy"; do
    echo "Waiting for Airflow webserver..."
    sleep 2
done

# Get container ID
CONTAINER_ID=$(docker compose ps -q airflow-webserver)

if [ -z "$CONTAINER_ID" ]; then
    echo "Could not find airflow-webserver container"
    exit 1
fi

# Traditional PostgreSQL (PostgresHook / SQLExecuteQueryOperator / GenericTransfer)
echo "Creating 'postgres_traditional' connection..."
docker exec "$CONTAINER_ID" airflow connections delete postgres_traditional 2>/dev/null || true
docker exec "$CONTAINER_ID" airflow connections add postgres_traditional \
    --conn-type postgres \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login benchuser \
    --conn-password benchpass \
    --conn-schema benchdb

# ADBC PostgreSQL. conn-type adbc so SQL operators resolve AdbcHook.
# db_kwargs.uri is the libpq URL. Putting driver/dialect in extra otherwise
# leaks them as URI query params and libpq rejects the connect
# (invalid URI query parameter: "driver").
echo "Creating 'postgres_adbc' connection..."
docker exec "$CONTAINER_ID" airflow connections delete postgres_adbc 2>/dev/null || true
docker exec "$CONTAINER_ID" airflow connections add postgres_adbc \
    --conn-type adbc \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login benchuser \
    --conn-password benchpass \
    --conn-schema benchdb \
    --conn-extra '{"driver": "adbc_driver_postgresql", "dialect": "postgresql", "db_kwargs": {"uri": "postgresql://benchuser:benchpass@postgres:5432/benchdb"}}'

# File-backed DuckDB (AdbcHook). Host uses '::' so AdbcHook passes the URI through.
echo "Creating 'duckdb_adbc' connection..."
docker exec "$CONTAINER_ID" airflow connections delete duckdb_adbc 2>/dev/null || true
docker exec "$CONTAINER_ID" airflow connections add duckdb_adbc \
    --conn-type adbc \
    --conn-host 'file::/opt/airflow/include/data/bench.duckdb' \
    --conn-extra '{"driver": "adbc_driver_duckdb", "entrypoint": "duckdb_adbc_init", "dialect": "duckdb"}'

# File-backed SQLite (AdbcHook)
echo "Creating 'sqlite_adbc' connection..."
docker exec "$CONTAINER_ID" airflow connections delete sqlite_adbc 2>/dev/null || true
docker exec "$CONTAINER_ID" airflow connections add sqlite_adbc \
    --conn-type adbc \
    --conn-host 'file:/opt/airflow/include/data/bench.sqlite' \
    --conn-extra '{"driver": "adbc_driver_sqlite", "dialect": "sqlite"}'

echo "Setting benchmark variables..."
docker exec "$CONTAINER_ID" airflow variables set BENCH_ROW_COUNT "$BENCH_ROW_COUNT"
docker exec "$CONTAINER_ID" airflow variables set BENCH_BATCH_SIZE "$BENCH_BATCH_SIZE"

echo "Connections and variables configured."
echo ""
echo "Connections created:"
echo "  - postgres_traditional: PostgresHook (psycopg2), conn type postgres"
echo "  - postgres_adbc: AdbcHook, conn type adbc"
echo "  - duckdb_adbc: AdbcHook -> file-backed DuckDB"
echo "  - sqlite_adbc: AdbcHook -> file-backed SQLite"
echo ""
echo "Variables set:"
echo "  - BENCH_ROW_COUNT: $BENCH_ROW_COUNT"
echo "  - BENCH_BATCH_SIZE: $BENCH_BATCH_SIZE"
