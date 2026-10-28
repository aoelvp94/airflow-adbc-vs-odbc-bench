# Airflow ADBC vs Traditional Hooks Benchmark

Local Airflow 2.11+ demo of `AdbcHook` as an **opt-in** beside `OdbcHook` / `DbApiHook` (here, `PostgresHook`), not a replacement. Choose the path from workload and driver availability.

Hook-level DAGs call hooks from `PythonOperator`. Operator-level DAG `bench_sql_operators` runs the same SQL through `SQLExecuteQueryOperator` and `GenericTransfer` on both connection types.

## Engine matrix

| Engine | Traditional path | ADBC path | Offline | Default runner |
| --- | --- | --- | --- | --- |
| PostgreSQL 15 (compose) | `PostgresHook`; `SQLExecuteQueryOperator`; `GenericTransfer` | `AdbcHook`; same operators, conn type `adbc` | Yes | Yes |
| DuckDB (file in `include/data/`) | `duckdb` Python API | `AdbcHook` + bundled `adbc_driver_duckdb` | Yes | Yes |
| SQLite (file in `include/data/`) | stdlib `sqlite3` | `AdbcHook` + `adbc_driver_sqlite` | Yes | Yes |
| MSSQL / Oracle / Teradata | `OdbcHook` | `AdbcHook` only if a driver exists; MSSQL ADBC is experimental as of 2026 | No (ODBC driver or cloud) | No |
| Snowflake / BigQuery / Databricks | Vendor hooks | Vendor ADBC drivers | No (accounts / heavy deps) | No |

Snowflake, BigQuery, and Databricks are out of scope. This repo does not require cloud credentials.

## Layer split

| Layer | DAGs | What is timed |
| --- | --- | --- |
| Hook | `bench_bulk_insert`, `bench_bulk_select`, `bench_transfer` | `PostgresHook` vs `AdbcHook` inside Python callables (insert_rows / fetchall vs Arrow ingest / `fetch_arrow_table`; hand-rolled ETL) |
| Hook (local file) | `bench_duckdb`, `bench_sqlite` | Native Python API vs ADBC on a file in the compose volume |
| Operator | `bench_sql_operators` | Airflow `SQLExecuteQueryOperator` (INSERT...SELECT) and `common.sql` `GenericTransfer` (`get_records` + `insert_rows`) on `postgres_traditional` vs `postgres_adbc`. There is no `PostgresToPostgresOperator` in current providers; `GenericTransfer` is the portable transfer operator. |

## Stack

- Apache Airflow 2.11.2, LocalExecutor
- PostgreSQL 15 (SQL-server baseline)
- File-backed DuckDB and SQLite under `include/data/` (no extra service)
- Providers: `apache-airflow-providers-postgres`, `apache-airflow-providers-apache-arrow` 0.1.1, `apache-airflow-providers-common-sql`
- Drivers: `psycopg2-binary`, `adbc-driver-postgresql`, `adbc-driver-sqlite`, `duckdb` (exports `adbc_driver_duckdb`)
- Docker Compose

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 4GB RAM available for containers

### Run the Benchmark

```bash
# 1. Clone the repository
git clone https://github.com/aoelvp94/airflow-adbc-vs-odbc-bench.git
cd airflow-adbc-vs-odbc-bench

# 2. Copy environment template
cp .env.example .env

# 3. Start the stack (first run takes ~2 minutes)
docker compose up -d

# 4. Wait for Airflow to initialize
./scripts/wait_for_airflow.sh

# 5. Create connections
./scripts/setup_connections.sh

# 6. Run the benchmark
./scripts/run_bench.sh
```

### View Results

- Airflow UI: http://localhost:8080 (user: `airflow`, password: `airflow`)
- JSON: `include/results/*.json` and the table printed by `./scripts/run_bench.sh`

## Sample timings (illustrative)

Figures below are samples, not measurements from this checkout. Run `./scripts/run_bench.sh` for local numbers. Operator, DuckDB, and SQLite rows appear only after that run.

| Workload | Traditional | ADBC | Speedup |
| --- | --- | --- | --- |
| Bulk insert (100,000 rows, hook, Postgres) | 12.3 s / 8,130 rows/s (`PostgresHook`) | 2.1 s / 47,619 rows/s (Arrow batches) | 5.9x |
| Bulk select (100,000 rows, hook, Postgres) | 8.7 s / 11,494 rows/s (`cursor.fetchall`) | 1.4 s / 71,429 rows/s (Arrow table) | 6.2x |
| Transfer (50,000 rows extract + load, hook, Postgres) | 9.8 s / 5,102 rows/s (row-by-row) | 1.9 s / 26,316 rows/s (Arrow native) | 5.2x |
| `SQLExecuteQueryOperator` / `GenericTransfer` | (run locally) | (run locally) | (run locally) |
| DuckDB insert/select (file) | (run locally) | (run locally) | (run locally) |
| SQLite insert/select (file) | (run locally) | (run locally) | (run locally) |

## DAGs

### Hook-level (Postgres)

`bench_bulk_insert` — default `BENCH_ROW_COUNT` (1,000,000) rows:

- Traditional: `PostgresHook.insert_rows()`
- ADBC: `AdbcHook` Arrow ingest

`bench_bulk_select` — large result sets:

- Traditional: `cursor.fetchall()` (list of tuples)
- ADBC: `AdbcHook` Arrow Table

`bench_transfer` — extract-transform-load in Python:

- Traditional: read rows, process, insert rows
- ADBC: Arrow table, Arrow compute, Arrow write

### Operator-level (Postgres)

`bench_sql_operators`:

- `SQLExecuteQueryOperator` on `postgres_traditional` and `postgres_adbc` (`INSERT ... SELECT`)
- `GenericTransfer` on the same pair (hook-mediated extract/load)
- Durations are Airflow `TaskInstance.duration` for those operator tasks

### Hook-level (local files)

`bench_duckdb` — file `DUCKDB_PATH` (default `/opt/airflow/include/data/bench.duckdb`):

- Traditional: `duckdb.connect(...).executemany` / `fetchall`
- ADBC: `AdbcHook(adbc_conn_id='duckdb_adbc')` (fallback: `adbc_driver_duckdb.dbapi`)

`bench_sqlite` — file `SQLITE_PATH` (default `/opt/airflow/include/data/bench.sqlite`):

- Traditional: `sqlite3.executemany` / `fetchall`
- ADBC: `AdbcHook(adbc_conn_id='sqlite_adbc')` (fallback: `adbc_driver_sqlite.dbapi`)

## Configuration

Set in `.env` or Airflow Variables. One volume knob drives every DAG:

| Variable | Default | Role |
| --- | --- | --- |
| `BENCH_ROW_COUNT` | `1000000` | insert, select, transfer, operator SQL, DuckDB, and SQLite row count |
| `BENCH_BATCH_SIZE` | `1000` | traditional Postgres insert batch size |
| `DUCKDB_PATH` | `/opt/airflow/include/data/bench.duckdb` | DuckDB file |
| `SQLITE_PATH` | `/opt/airflow/include/data/bench.sqlite` | SQLite file |

`setup_connections.sh` writes `BENCH_ROW_COUNT` / `BENCH_BATCH_SIZE` into Airflow Variables from `.env` (or the defaults above). `include/utils/benchmark_utils.py` defines `DEFAULT_ROW_COUNT`; DAGs call `get_bench_row_count()`. JSON under `include/results/` is gitignored and regenerated by `./scripts/run_bench.sh`.

## Workload fit

ADBC is the better fit when:

- The job is a bulk transfer or a large result set
- The pipeline is Arrow-native (PyArrow, pandas, Parquet)
- The warehouse accepts Arrow (Snowflake, BigQuery) and a driver is installed

`OdbcHook` / `DbApiHook` remain the default when:

- The work is single-row CRUD or transactional
- The database is ODBC-only
- You need a mature vendor driver, stored procedures, or dialect-specific SQL

## Caveats

1. **Driver maturity.** ADBC drivers are newer. PostgreSQL, SQLite, and DuckDB are usable here; MSSQL is experimental as of 2026.
2. **Server-side Arrow.** Throughput depends on database Arrow support. Flight SQL servers do best. `SQLExecuteQueryOperator` `INSERT...SELECT` is mostly server-side, so hook differences are smaller than in the extract/load DAGs.
3. **Workload size.** Queries under ~1000 rows show little difference.
4. **Memory.** Columnar buffers use more RAM than row fetch.
5. **DuckDB / SQLite locking.** Those DAGs serialize writes on one file; they do not start an extra container.

## ODBC

Same split for MSSQL and other ODBC targets. See [ODBC_REFERENCE.md](ODBC_REFERENCE.md). This path is not in `./scripts/run_bench.sh`.

```python
# Traditional ODBC (row-based)
from airflow.providers.odbc.hooks.odbc import OdbcHook
hook = OdbcHook(odbc_conn_id='my_mssql')

# ADBC (Arrow-based, when a driver is installed)
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
hook = AdbcHook(adbc_conn_id='my_mssql_adbc')
```

## Project Structure

```
.
├── dags/
│   ├── bench_bulk_insert.py       # Hook insert (Postgres)
│   ├── bench_bulk_select.py       # Hook select (Postgres)
│   ├── bench_transfer.py          # Hook ETL (Postgres)
│   ├── bench_sql_operators.py     # SQLExecuteQueryOperator + GenericTransfer
│   ├── bench_duckdb.py            # File-backed DuckDB
│   └── bench_sqlite.py            # File-backed SQLite
├── include/
│   ├── data/                      # DuckDB / SQLite files
│   ├── results/                   # JSON outputs
│   └── utils/
├── scripts/
│   ├── run_bench.sh
│   ├── setup_connections.sh
│   └── wait_for_airflow.sh
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## References

- [Airflow Apache Arrow Provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/index.html)
- [ADBC Documentation](https://arrow.apache.org/adbc/)
- [AdbcHook API Reference](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/_api/airflow/providers/apache/arrow/hooks/adbc/index.html)
- [SQL operators (common.sql)](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html)

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Contributing

This is a personal demonstration repository. For Airflow improvements, contribute to [apache/airflow](https://github.com/apache/airflow).

## Author

@aoelvp94
