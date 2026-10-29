# Airflow ADBC vs Traditional Hooks Benchmark

Local Airflow 3.3.2 demo of `AdbcHook` as an **opt-in** beside `OdbcHook` / `DbApiHook` (here, `PostgresHook`), not a replacement. Choose the path from workload and driver availability.

Hook-level DAGs call hooks from `PythonOperator`. Operator-level DAG `bench_sql_operators` runs the same SQL / same N through every stock `common.sql` SQL and transfer operator that applies to Postgres↔Postgres, plus ODBC (`OdbcHook`) and custom `AdbcArrowTransfer`.

## TL;DR

Stock `GenericTransfer` / `SQLInsertRowsOperator` still dest-write with `insert_rows` even when `conn_type=adbc` (no `adbc_ingest`).

N=1e6 from [Sample timings](#sample-timings-measured):

| Path | Time |
| --- | --- |
| Stock `GenericTransfer` libpq | ~14.6 s |
| Stock `GenericTransfer` ADBC | ~31.2 s |
| Stock `GenericTransfer` ODBC | ~65.2 s |
| `AdbcArrowTransfer` | ~3.4–4.0 s (~9× vs stock ADBC GT) |
| `SQLExecuteQueryOperator` `INSERT…SELECT` | ~2.5 s (tied; server-side) |

Post-load verify: `COUNT(*)` + ordered md5 on copy paths matched `bench_source` (see [Post-load verification](#post-load-verification)). `SQLInsertRowsOperator` / hook ETL are COUNT-only by design.

Personal demo; not upstream yet.

## Protocol matrix

ODBC/JDBC are row-oriented client protocols. ADBC is Arrow (columnar). This demo holds the **database** fixed (compose PostgreSQL 18) and varies the **protocol**.

| Protocol | Client | Airflow hook | Data shape | Default stack |
| --- | --- | --- | --- | --- |
| libpq (native) | `psycopg2` | `PostgresHook` (conn type `postgres`) | Row | Yes |
| ODBC | unixODBC + psqlODBC + `pyodbc` | `OdbcHook` (conn type `odbc`, `postgres_odbc`) | Row | Yes (same Postgres; no MSSQL container, no cloud creds) |
| JDBC | — | not in this demo | Row | No |
| ADBC | `adbc-driver-postgresql` | `AdbcHook` (conn type `adbc`) | Arrow | Yes |

MSSQL/Oracle via ODBC still needs a vendor driver and is not in `./scripts/run_bench.sh`. See [ODBC_REFERENCE.md](ODBC_REFERENCE.md). JDBC is out of scope (no JDBC provider in this image).

## Engine matrix

| Engine | Traditional path | ADBC path | Offline | Default runner |
| --- | --- | --- | --- | --- |
| PostgreSQL 18 (compose `postgres:18`) | `PostgresHook`; stock SQL/transfer operators | `AdbcHook`; same stock operators, conn type `adbc` | Yes | Yes |
| PostgreSQL 18 over ODBC | `OdbcHook` + psqlODBC; same stock operators, conn type `odbc` | same `AdbcHook` (ADBC is not ODBC) | Yes | Yes |
| DuckDB 2.0.0 (file in `include/data/`) | `duckdb` Python API | `AdbcHook` + bundled `adbc_driver_duckdb` | Yes | Yes |
| SQLite (file in `include/data/`) | stdlib `sqlite3` | `AdbcHook` + `adbc_driver_sqlite` | Yes | Yes |
| MSSQL / Oracle / Teradata | `OdbcHook` + vendor driver | `AdbcHook` only if a driver exists; MSSQL ADBC is experimental as of 2026 | No (extra driver or cloud) | No |
| Snowflake / BigQuery / Databricks | Vendor hooks | Vendor ADBC drivers | No (accounts / heavy deps) | No |

Snowflake, BigQuery, and Databricks are out of scope. This repo does not require cloud credentials.

## Stock operator coverage (`common.sql` 2.1.1)

Goal: stock operator + traditional hook vs stock operator + ADBC hook vs Arrow-aware operator, same SQL / same N where applicable. **No invented operators.**

| Operator | PostgresHook | OdbcHook | AdbcHook | Arrow-aware | In `run_bench.sh` |
| --- | --- | --- | --- | --- | --- |
| `SQLExecuteQueryOperator` | Yes | Yes | Yes | n/a (server SQL) | Yes |
| `SQLInsertRowsOperator` | Yes | Yes | Yes | no stock Arrow insert-from-Python operator | Yes |
| `SQLBulkLoadOperator` | Yes (`COPY FROM STDIN`) | **N/A** (`OdbcHook.bulk_load` is `NotImplementedError`) | **N/A** (`AdbcHook.bulk_load` is `NotImplementedError`) | n/a | Yes (Postgres only) |
| `GenericTransfer` | Yes (`get_records` + `insert_rows`) | Yes | Yes (still dest `insert_rows`) | custom `AdbcArrowTransfer` (`adbc_ingest`) | Yes |
| `@task.sql` | same execute path as `SQLExecuteQueryOperator` | same | same | n/a | No (duplicate) |
| `SQLCheckOperator` / `SQLColumnCheckOperator` / `SQLTableCheckOperator` / `SQLValueCheckOperator` / `SQLIntervalCheckOperator` / `SQLThresholdCheckOperator` | data-quality assertions, not bulk SQL/transfer | — | — | — | No |
| `BranchSQLOperator` | control flow, not transfer | — | — | — | No |
| `AnalyticsOperator` | DataFusion over files/object storage, not Postgres↔Postgres | — | — | — | No |
| `PostgresOperator` / `PostgresToPostgresOperator` | **do not exist** in `apache-airflow-providers-postgres` 7.0.2 | — | — | — | No |

## Layer split

| Layer | DAGs | What is timed |
| --- | --- | --- |
| Hook | `bench_bulk_insert`, `bench_bulk_select`, `bench_transfer` | `PostgresHook` vs `AdbcHook` inside Python callables (insert_rows / fetchall vs Arrow ingest / `fetch_arrow_table`; hand-rolled ETL) |
| Hook (local file) | `bench_duckdb`, `bench_sqlite` | Native Python API vs ADBC on a file in the compose volume |
| Operator | `bench_sql_operators` | Stock `SQLExecuteQueryOperator`, `SQLInsertRowsOperator`, `SQLBulkLoadOperator`, `GenericTransfer` on postgres / odbc / adbc connections; custom `AdbcArrowTransfer` dest `adbc_ingest`. Timed tasks run **serially** on LocalExecutor. Untimed `verify_operator_loads` runs after the matrix and is not in `BenchmarkTimer` / `on_success_callback` durations. |

## Stack

- Apache Airflow 3.3.2 (`apache/airflow:3.3.2-python3.12`), LocalExecutor, api-server + dag-processor
- PostgreSQL 18 (compose `postgres:18`; 18.6 at time of writing — not 19 RC)
- File-backed DuckDB 2.0.0 LTS and SQLite under `include/data/` (no extra service)
- Providers: `apache-airflow-providers-postgres` 7.0.2, `apache-airflow-providers-apache-arrow` 0.1.1, `apache-airflow-providers-common-sql` 2.1.1, `apache-airflow-providers-standard` 1.19.0, `apache-airflow-providers-odbc` 4.13.0
- Drivers: `psycopg2-binary`, unixODBC + psqlODBC (`pyodbc`), `adbc-driver-manager` / `adbc-driver-postgresql` / `adbc-driver-sqlite` 1.12.0 (constraints-3.3.2), `duckdb==2.0.0` (exports `adbc_driver_duckdb`; not in AF constraints)
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

# Wipe the postgres volume if this tree previously ran Airflow 2 or PostgreSQL 15
# (PG 15 and 18 data dirs are incompatible). Also removes old DuckDB 1.x files.
docker compose down -v
docker compose up -d --build
./scripts/wait_for_airflow.sh
./scripts/setup_connections.sh
./scripts/run_bench.sh
```

JSON under `include/results/` is gitignored and overwritten by `run_bench.sh`. UI: http://localhost:8080 (user `airflow` / `airflow`).

Operator unit tests (no Postgres): `PYTHONPATH=include pytest tests/ -q`.

## Sample timings (measured)

Single run on 2026-10-28, docker compose on a Cursor cloud VM: 4 vCPU Intel Xeon, 16 GiB RAM, Linux 6.12 x86_64, Airflow 3.3.2 LocalExecutor (`apache/airflow:3.3.2-python3.12`), PostgreSQL 18.6 (`postgres:18`), DuckDB 2.0.0, unixODBC + psqlODBC. **N = `BENCH_ROW_COUNT` = 1,000,000**. Durations are seconds from `include/results/*.json`. Operator rows are Airflow `TaskInstance.duration` (or `SQLInsertRowsOperator.execute()` wall time). Timed operator tasks ran serially. Speedup is traditional / ADBC unless noted.

### Stock operator matrix (same SQL / same N)

| Operator | PostgresHook (libpq) | OdbcHook (psqlODBC) | AdbcHook | Arrow-aware |
| --- | --- | --- | --- | --- |
| `SQLExecuteQueryOperator` | 2.532 s / 394,909 rows/s | 2.487 s / 402,129 rows/s | 2.459 s / 406,703 rows/s | n/a (server `INSERT...SELECT`) |
| `SQLInsertRowsOperator` | 11.781 s / 84,883 rows/s | 59.047 s / 16,936 rows/s | 28.453 s / 35,146 rows/s | none (stock) |
| `SQLBulkLoadOperator` | 1.611 s / 620,801 rows/s (`COPY`) | N/A (`bulk_load` NotImplementedError) | N/A (`bulk_load` NotImplementedError) | n/a |
| `GenericTransfer` | 14.584 s / 68,570 rows/s | 65.187 s / 15,341 rows/s | 31.209 s / 32,042 rows/s | `AdbcArrowTransfer` below |

Server-side `SQLExecuteQueryOperator` barely moves with hook choice. Row `insert_rows` (SQLInsertRows / GenericTransfer) is slowest on ODBC, then ADBC, then libpq. Native `COPY` is the fastest stock load on Postgres.

### Stock GenericTransfer vs AdbcArrowTransfer

Same Airflow transfer-operator interface (`source_conn_id`, `destination_conn_id`, SQL, dest table). Stock `GenericTransfer` always dest-writes with `insert_rows`, even when the connection type is `adbc` or `odbc`. Custom `AdbcArrowTransfer` (`include/operators/adbc_arrow_transfer.py`) dest-writes with `cursor.adbc_ingest`.

| Path | Time | Dest write |
| --- | --- | --- |
| Stock `GenericTransfer` traditional | 14.584 s / 68,570 rows/s | `PostgresHook.insert_rows` |
| Stock `GenericTransfer` ODBC dest | 65.187 s / 15,341 rows/s | `OdbcHook.insert_rows` (row-based; dest `price` is float8) |
| Stock `GenericTransfer` ADBC dest | 31.209 s / 32,042 rows/s | `AdbcHook.insert_rows` (row-based; dest `price` is float8) |
| `AdbcArrowTransfer` traditional source | 4.035 s / 247,846 rows/s | `adbc_ingest` into NUMERIC |
| `AdbcArrowTransfer` ADBC source | 3.371 s / 296,629 rows/s | `adbc_ingest` into NUMERIC (**9.26x** vs stock ADBC `insert_rows`) |

### Other hook-level / file-engine results

| Workload | Traditional | ADBC | Speedup |
| --- | --- | --- | --- |
| Bulk insert (hook, Postgres) | 18.005 s / 55,539 rows/s (`insert_rows`) | 2.082 s / 480,207 rows/s (Arrow ingest) | 8.65x |
| Bulk select (hook, Postgres) | 0.960 s / 1,041,853 rows/s (`fetchall`) | 0.426 s / 2,345,880 rows/s (Arrow table) | 2.25x |
| DuckDB insert (file) | 622.421 s / 1,607 rows/s (`executemany`) | 0.658 s / 1,519,781 rows/s (Arrow ingest) | 945.94x |
| DuckDB select (file) | 0.691 s / 1,448,040 rows/s (`fetchall`) | 0.178 s / 5,623,198 rows/s (Arrow table) | 3.88x |
| SQLite insert (file) | 1.078 s / 927,566 rows/s (`executemany`) | 0.813 s / 1,229,461 rows/s (Arrow ingest) | 1.33x |
| SQLite select (file) | 0.893 s / 1,120,081 rows/s (`fetchall`) | 0.354 s / 2,822,360 rows/s (Arrow table) | 2.52x |

`SQLExecuteQueryOperator` is server-side `INSERT...SELECT`, so hook choice barely moves the number. DuckDB traditional insert is `executemany` of 1e6 Python tuples vs one Arrow ingest. Re-run locally: hardware and cache state will move the digits.

### Hook ETL (different workload; not 1:1 vs transfer operators)

`bench_transfer` times a **Python callable** (extract + 10% price discount + load), not `GenericTransfer` / `AdbcArrowTransfer`. Do not rank Hook ETL ADBC (~2.7 s) against the operator table above.

| Path | Time | What it is |
| --- | --- | --- |
| Hook ETL traditional | 21.006 s | `PythonOperator` + `PostgresHook.insert_rows` + 10% discount |
| Hook ETL ADBC | 2.722 s | `PythonOperator` + Arrow compute + `adbc_ingest` |
| `AdbcArrowTransfer` in `bench_transfer` | 3.726 s | operator copy, no transform, measured in that DAG’s run |

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

### Operator-level (Postgres libpq / ODBC / ADBC)

`bench_sql_operators` (timed tasks serialized):

- `SQLExecuteQueryOperator` on `postgres_traditional`, `postgres_odbc`, `postgres_adbc` (`INSERT ... SELECT`)
- `SQLInsertRowsOperator` on the same three connections (Python tuples → `insert_rows`)
- `SQLBulkLoadOperator` on `postgres_traditional` only (`COPY FROM STDIN` from a TSV of `bench_source`)
- Stock `GenericTransfer` on each protocol pair (`get_records` + dest `insert_rows`)
- Custom `AdbcArrowTransfer` from traditional source and from ADBC source; dest is always `adbc_ingest` into NUMERIC
- Durations are Airflow `TaskInstance.duration`, except `SQLInsertRowsOperator` which records `execute()` wall time after row generation
- After timed tasks, untimed `verify_operator_loads` checks dest tables (see **Post-load verification**)

`bench_transfer` also has untimed `verify_transfer_loads` after the hook ETL / Arrow copy tasks.

### Post-load verification

Task success is not enough: dest tables are queried in PostgreSQL (`COUNT(*)`, an ordered md5 checksum of payload columns, and `EXCEPT` both ways on checksum mismatch). Rows are not loaded into Python. A mismatch **fails the DAG task** (`LoadVerifyError`).

**Verified (must match `BENCH_ROW_COUNT` / `get_bench_row_count()`):**

- Every operator dest in `bench_sql_operators`, plus `bench_source`
- Copy paths that read the same extract SQL with no transform — `SQLExecuteQueryOperator` (postgres/odbc/adbc), stock `GenericTransfer` postgres, both `AdbcArrowTransfer` dests, `SQLBulkLoadOperator` — payload equals `bench_source` (`product_name`, `category`, `quantity`, `created_at`, `price`)
- Stock `GenericTransfer` ODBC/ADBC dests store `price` as float8 (`FLOAT8_DEST_TABLES`). They are still compared to `bench_source` with `CAST(price AS numeric(10, 2))` plus the other payload columns
- `bench_transfer`: `COUNT(*)` on `bench_dest_traditional`, `bench_dest_adbc`, `bench_dest_arrow`; Arrow dest payload equals `bench_source` (copy, no transform)

**Intentionally not equal (COUNT only, documented):**

- `SQLInsertRowsOperator` dests regenerate rows via `generate_product_data` (unseeded `random` + `datetime.now()`), not `bench_source`. COUNT == N only; content is not compared to transfer paths or across postgres/odbc/adbc
- Hook ETL in `bench_transfer` applies a **10% price discount**. Do not expect equality with `GenericTransfer` / Arrow copy; COUNT == N only vs those paths

**If verify fails:** the DAG run is failed (and `./scripts/run_bench.sh` treats that DAG as failed). Timings are not an apples-to-apples claim until dest rows match. The verify task log names the table, `COUNT(*)` vs expected N, checksum fields, and `EXCEPT` row counts in each direction (truncated load, `LIMIT` vs Variable mismatch, type coercion, …). Verification time is **not** folded into operator durations.

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
[x] apache-airflow-providers-common-sql == 2.1.1   (SQLExecuteQuery, SQLInsertRows, SQLBulkLoad, GenericTransfer)
[x] apache-airflow-providers-standard == 1.19.0    (PythonOperator)
[x] apache-airflow-providers-odbc == 4.13.0        (OdbcHook; conn type odbc)
[x] constraints-3.3.2 / constraints-3.12.txt
[x] PostgreSQL 18 (compose postgres:18; 18.6 at time of writing — not 19 RC)
[x] duckdb == 2.0.0 (exports adbc_driver_duckdb; not in AF constraints)
[x] adbc-driver-manager / adbc-driver-postgresql / adbc-driver-sqlite == 1.12.0 (constraints-3.3.2)
[x] unixODBC + psqlODBC (Driver=PostgreSQL Unicode) via postgres_odbc
[x] AdbcHook.get_conn() + cursor.adbc_ingest / fetch_arrow_table
[x] PostgresHook.insert_rows / get_records / run / bulk_load (COPY)
[x] OdbcHook.insert_rows / get_records / run
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

Default stack uses **PostgreSQL via unixODBC + psqlODBC** (`postgres_odbc`) so ODBC vs libpq vs ADBC is the same engine, offline, no extra container. `./scripts/run_bench.sh` includes those operator tasks.

MSSQL/Oracle still need a vendor ODBC driver and are not in the default runner. See [ODBC_REFERENCE.md](ODBC_REFERENCE.md).

```python
# ODBC (row-based) — demo conn postgres_odbc, or mssql_odbc with a vendor driver
from airflow.providers.odbc.hooks.odbc import OdbcHook
hook = OdbcHook(odbc_conn_id='postgres_odbc')

# ADBC (Arrow-based)
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
hook = AdbcHook(adbc_conn_id='postgres_adbc')
```

## Project Structure

```
.
├── dags/
│   ├── bench_bulk_insert.py       # Hook insert (Postgres)
│   ├── bench_bulk_select.py       # Hook select (Postgres)
│   ├── bench_transfer.py          # Hook ETL (Python callable; not GenericTransfer)
│   ├── bench_sql_operators.py     # Stock SQL+transfer ops × postgres/odbc/adbc + AdbcArrowTransfer
│   ├── bench_duckdb.py            # File-backed DuckDB
│   └── bench_sqlite.py            # File-backed SQLite
├── include/
│   ├── data/                      # DuckDB / SQLite files
│   ├── operators/                 # AdbcArrowTransfer (GenericTransfer + adbc_ingest)
│   ├── results/                   # JSON outputs
│   └── utils/                     # timers, generators, post-load verify_load
├── tests/                         # pytest: operators + verify helpers (no Postgres)
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
