# Airflow ADBC vs Traditional Hooks Benchmark

Local Airflow 3.3.2 demo of `AdbcHook` as an **opt-in** beside `OdbcHook` / `DbApiHook` (here, `PostgresHook`), not a replacement. Choose the path from workload and driver availability.

Hook-level DAGs call hooks from `PythonOperator`. Operator-level DAG `bench_sql_operators` runs the same SQL through `SQLExecuteQueryOperator`, stock `GenericTransfer` (`insert_rows`), and custom `AdbcArrowTransfer` (`adbc_ingest`).

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
| Operator | `bench_sql_operators` | Airflow `SQLExecuteQueryOperator` (INSERT...SELECT); stock `common.sql` `GenericTransfer` (`get_records` + `insert_rows`); custom `AdbcArrowTransfer` (Arrow extract when source is ADBC, else row extract, then dest `adbc_ingest`). There is no `PostgresToPostgresOperator` in current providers. |

## Stack

- Apache Airflow 3.3.2 (`apache/airflow:3.3.2-python3.12`), LocalExecutor, api-server + dag-processor
- PostgreSQL 15 (SQL-server baseline)
- File-backed DuckDB and SQLite under `include/data/` (no extra service)
- Providers: `apache-airflow-providers-postgres` 7.0.2, `apache-airflow-providers-apache-arrow` 0.1.1, `apache-airflow-providers-common-sql` 2.1.1, `apache-airflow-providers-standard` 1.19.0
- Drivers: `psycopg2-binary`, `adbc-driver-postgresql`, `adbc-driver-sqlite`, `duckdb` (exports `adbc_driver_duckdb`)
- Docker Compose

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 4GB RAM available for containers

### Reproduce the N=1,000,000 timings

Volume knob is **`BENCH_ROW_COUNT`**. `.env.example` sets it to `1000000`. `docker-compose.yml` injects `${BENCH_ROW_COUNT:-1000000}` into every Airflow service. `./scripts/setup_connections.sh` copies that value into Airflow Variable `BENCH_ROW_COUNT` (hook DAGs read it at runtime). Operator SQL `LIMIT` is taken from the same container env at DAG parse. `./scripts/run_bench.sh` prints `BENCH_ROW_COUNT` and triggers all six DAGs.

```bash
git clone https://github.com/aoelvp94/airflow-adbc-vs-odbc-bench.git
cd airflow-adbc-vs-odbc-bench
cp .env.example .env
grep BENCH_ROW_COUNT .env    # must be 1000000 to match the sample table

# Wipe the postgres volume if this tree previously ran Airflow 2
docker compose down -v
docker compose up -d --build
./scripts/wait_for_airflow.sh
./scripts/setup_connections.sh
./scripts/run_bench.sh
```

JSON under `include/results/` is gitignored and overwritten by `run_bench.sh`. UI: http://localhost:8080 (user `airflow` / `airflow`).

Operator unit tests (no Postgres): `PYTHONPATH=include pytest tests/ -q`.

## Sample timings (measured)

Single run on 2026-10-28, docker compose on a Cursor cloud VM: 4 vCPU Intel Xeon, 16 GiB RAM, Linux 6.12 x86_64, Airflow 3.3.2 LocalExecutor (`apache/airflow:3.3.2-python3.12`), PostgreSQL 15. **N = `BENCH_ROW_COUNT` = 1,000,000**. Durations are seconds from `include/results/*.json`. Operator rows are Airflow `TaskInstance.duration`. Speedup is traditional / ADBC unless noted.

### Stock GenericTransfer vs AdbcArrowTransfer

Same Airflow transfer-operator interface (`source_conn_id`, `destination_conn_id`, SQL, dest table). Stock `GenericTransfer` always dest-writes with `insert_rows`, even when the connection type is `adbc`. Custom `AdbcArrowTransfer` (`include/operators/adbc_arrow_transfer.py`) dest-writes with `cursor.adbc_ingest`.

| Path | Time | Dest write |
| --- | --- | --- |
| Stock `GenericTransfer` traditional | 19.629 s / 50,944 rows/s | `PostgresHook.insert_rows` |
| Stock `GenericTransfer` ADBC dest | 30.429 s / 32,864 rows/s | `AdbcHook.insert_rows` (row-based; dest `price` is float8) |
| `AdbcArrowTransfer` traditional source | 8.077 s / 123,807 rows/s | `adbc_ingest` into NUMERIC |
| `AdbcArrowTransfer` ADBC source | 7.984 s / 125,253 rows/s | `adbc_ingest` into NUMERIC (**3.81x** vs stock ADBC `insert_rows`) |

### Other operator / hook-level results

| Workload | Traditional | ADBC | Speedup |
| --- | --- | --- | --- |
| Bulk insert (hook, Postgres) | 17.735 s / 56,386 rows/s (`insert_rows`) | 1.757 s / 569,012 rows/s (Arrow ingest) | 10.09x |
| Bulk select (hook, Postgres) | 1.102 s / 907,085 rows/s (`fetchall`) | 0.457 s / 2,189,295 rows/s (Arrow table) | 2.41x |
| `SQLExecuteQueryOperator` INSERT...SELECT | 6.674 s / 149,825 rows/s | 7.524 s / 132,917 rows/s | 0.89x |
| DuckDB insert (file) | 528.324 s / 1,893 rows/s (`executemany`) | 0.713 s / 1,401,873 rows/s (Arrow ingest) | 740.64x |
| DuckDB select (file) | 0.760 s / 1,315,384 rows/s (`fetchall`) | 0.202 s / 4,948,879 rows/s (Arrow table) | 3.76x |
| SQLite insert (file) | 1.092 s / 916,018 rows/s (`executemany`) | 0.769 s / 1,300,842 rows/s (Arrow ingest) | 1.42x |
| SQLite select (file) | 0.928 s / 1,077,518 rows/s (`fetchall`) | 0.366 s / 2,730,549 rows/s (Arrow table) | 2.53x |

`SQLExecuteQueryOperator` is server-side `INSERT...SELECT`, so hook choice barely moves the number. DuckDB traditional insert is `executemany` of 1e6 Python tuples vs one Arrow ingest. Re-run locally: hardware and cache state will move the digits.

### Hook ETL (different workload; not 1:1 vs transfer operators)

`bench_transfer` times a **Python callable** (extract + 10% price discount + load), not `GenericTransfer` / `AdbcArrowTransfer`. Do not rank Hook ETL ADBC (~2.3 s) against the operator table above.

| Path | Time | What it is |
| --- | --- | --- |
| Hook ETL traditional | 19.708 s | `PythonOperator` + `PostgresHook.insert_rows` + 10% discount |
| Hook ETL ADBC | 2.337 s | `PythonOperator` + Arrow compute + `adbc_ingest` |
| `AdbcArrowTransfer` in `bench_transfer` | 3.680 s | operator copy, no transform, measured in that DAG’s run |

Footnotes only: different code path (callable vs operator), extra transform on the hook ETL tasks, and a different parallel-task mix than `bench_sql_operators`.

## DAGs

### Hook-level (Postgres)

`bench_bulk_insert` — default `BENCH_ROW_COUNT` (1,000,000) rows:

- Traditional: `PostgresHook.insert_rows()`
- ADBC: `AdbcHook` Arrow ingest

`bench_bulk_select` — large result sets:

- Traditional: `cursor.fetchall()` (list of tuples)
- ADBC: `AdbcHook` Arrow Table

`bench_transfer` — **hook ETL** in Python (not an Airflow transfer operator):

- Traditional: read rows, 10% discount, `insert_rows`
- ADBC: Arrow table, Arrow compute, Arrow write
- Also runs `AdbcArrowTransfer` as a side task (copy, no transform). That duration is a footnote, not a peer of stock `GenericTransfer`.

### Operator-level (Postgres)

`bench_sql_operators`:

- `SQLExecuteQueryOperator` on `postgres_traditional` and `postgres_adbc` (`INSERT ... SELECT`)
- Stock `GenericTransfer` on the same pair (`get_records` + dest `insert_rows`)
- Custom `AdbcArrowTransfer` from traditional source and from ADBC source; dest is always `adbc_ingest` into NUMERIC
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

`setup_connections.sh` writes `BENCH_ROW_COUNT` / `BENCH_BATCH_SIZE` into Airflow Variables from `.env` (or the defaults above). `include/utils/benchmark_utils.py` defines `DEFAULT_ROW_COUNT`; hook DAGs call `get_bench_row_count()` at runtime. Operator `LIMIT` uses container env `BENCH_ROW_COUNT` at DAG parse (compose default `1000000`). JSON under `include/results/` is gitignored and regenerated by `./scripts/run_bench.sh`.

## Compatibility

Tested stack (personal demo; not a guarantee for every 3.x patch):

```text
[x] Apache Airflow 3.3.2 (docker apache/airflow:3.3.2-python3.12; target AF >= 3.3.2)
[x] Python 3.12
[x] apache-airflow-providers-apache-arrow == 0.1.1  (AdbcHook; conn type adbc)
[x] apache-airflow-providers-postgres == 7.0.2     (PostgresHook; conn type postgres)
[x] apache-airflow-providers-common-sql == 2.1.1   (GenericTransfer, SQLExecuteQueryOperator)
[x] apache-airflow-providers-standard == 1.19.0    (PythonOperator)
[x] constraints-3.3.2 / constraints-3.12.txt
[x] AdbcHook.get_conn() + cursor.adbc_ingest / fetch_arrow_table
[x] PostgresHook.insert_rows / get_records / run
```

Arrow provider 0.1.1 is not in the 3.3.2 constraints file; the Dockerfile still pins it and verifies `from airflow.providers.apache.arrow.hooks.adbc import AdbcHook`. DAGs use `schedule=` and `airflow.sdk` (`DAG`, `Variable`). Not tested on Airflow 2.x or 3.0–3.2.

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
│   ├── bench_transfer.py          # Hook ETL (Python callable; not GenericTransfer)
│   ├── bench_sql_operators.py     # SQLExecuteQueryOperator + GenericTransfer + AdbcArrowTransfer
│   ├── bench_duckdb.py            # File-backed DuckDB
│   └── bench_sqlite.py            # File-backed SQLite
├── include/
│   ├── data/                      # DuckDB / SQLite files
│   ├── operators/                 # AdbcArrowTransfer (GenericTransfer + adbc_ingest)
│   ├── results/                   # JSON outputs
│   └── utils/
├── tests/                         # pytest: adbc_ingest mock (no Postgres)
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
