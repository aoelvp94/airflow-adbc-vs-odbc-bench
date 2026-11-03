# AIP Phase-2 evidence

Measured on this repo’s compose stack. Local demo stubs only — not apache/airflow.

Three claims are kept in **separate tables** on purpose:

1. **Postgres Adbc both-ends RecordBatch** vs stock GenericTransfer row path (**11.35×**). This is the Phase-2 capability-switch claim.
2. **Stock ODBC / operator matrix** (libpq vs ODBC vs ADBC vs Arrow). Stock GenericTransfer ODBC is a row protocol path; it is **not** folded into the 11.35× figure.
3. **Multi-engine Phase-2** (DuckDB ↔ DuckDB, DuckDB ↔ Postgres) using the same RecordBatch protocols.

## Hardware / VM

| Item | Value |
| --- | --- |
| Host | Cursor cloud VM, Linux 6.12.94+ x86_64 |
| CPU | 4 vCPU, Intel Xeon (1 thread per core) |
| Memory | 16 GiB |
| Date | 2026-11-03 |

## Software

| Item | Value |
| --- | --- |
| N (`BENCH_ROW_COUNT`) | **1,000,000** |
| Airflow | 3.3.2 (`apache/airflow:3.3.2-python3.12`), LocalExecutor |
| PostgreSQL | 18.6 (`postgres:18`) |
| Arrow provider / AdbcHook | `apache-airflow-providers-apache-arrow` 0.1.1 |
| common.sql | 2.1.1 (`GenericTransfer`) |
| ADBC PostgreSQL driver | 1.12.0 |
| PyArrow | 25.0.1 |
| DuckDB | 2.0.0 (file-backed; used in section 3) |
| ODBC | unixODBC + psqlODBC (`PostgreSQL Unicode`) via `postgres_odbc` |

Timed operator tasks ran **serially** on LocalExecutor. Durations are Airflow `TaskInstance.duration` (or `BenchmarkTimer` / `SQLInsertRowsOperator.execute()` wall time). JSON under `include/results/` is gitignored; numbers are copied here.

---

## 1. Phase-2 both-ends RecordBatch (Postgres Adbc ↔ Adbc)

**Do not mix ODBC GenericTransfer times into this table or into the 11.35× speedup.**

DAG: `bench_capability_transfer`, run `manual__2026-11-03T13:36:10.632167+00:00`. JSON: `include/results/bench_capability_transfer.json`.

| Path | Time | Rows/s | Dest write | Verify |
| --- | --- | --- | --- | --- |
| Stock `GenericTransfer` Adbc → Adbc (row path) | **31.395 s** | 31,852 | `AdbcHook.insert_rows` (price stored as float8) | PASS — `COUNT(*)=1000000`, payload digest matches `bench_source` (price CAST to numeric(10,2)) |
| `CapabilityGenericTransfer` Adbc → Adbc (both-ends RecordBatch) | **2.765 s** | 361,627 | `get_record_batches` → `adbc_ingest` | PASS — same COUNT, same digest as source |
| `AdbcArrowTransfer` Adbc → Adbc (dest-only ingest) | **3.259 s** | 306,880 | `fetch_arrow_table` → `adbc_ingest` | PASS — same COUNT, same digest as source |

Speedup, columnar vs stock GenericTransfer (this table only): **11.35×**.

Post-load digest (all three dests vs `bench_source`): `n=1000000`, `sum_quantity=50504219`, `digest=15e2f3f24e6a33282fc269968622d312`. Task log: `Columnar path: both ends advertise RecordBatch read/write (AdbcArrowCapabilityAdapter -> AdbcArrowCapabilityAdapter)`.

Stock `GenericTransfer` still dest-writes with `insert_rows` even when both connections are `adbc`, and that row path is the slow one (~31 s for 1e6 rows). When **both** hooks advertise RecordBatch read and write, a GenericTransfer-shaped helper can stream Arrow batches end-to-end (~2.8 s) and still land the same 1e6 rows and the same payload digest as `bench_source`. Dest-only `AdbcArrowTransfer` (`adbc_ingest` after a full Arrow table extract) is in the same ballpark (~3.3 s); it is a different claim (destination Arrow ingest, source may be row-based). Phase-2 is the both-ends capability switch.

---

## 2. Stock ODBC / operator matrix (N = 1,000,000)

DAG: `bench_sql_operators`, run `manual__2026-11-03T13:58:33.234715+00:00` (wall ~185 s including setup/verify). Same SQL / same N / same Postgres 18. JSON: `include/results/bench_operator_matrix.json` and `include/results/bench_arrow_transfer.json`.

This section is the protocol × stock-operator comparison. **Stock GenericTransfer ODBC (~60.5 s) is not a Phase-2 columnar number** and is not used in the 11.35× ratio above.

### SQL / load operators

| Operator | PostgresHook (libpq) | OdbcHook (psqlODBC) | AdbcHook | Arrow-aware |
| --- | --- | --- | --- | --- |
| `SQLExecuteQueryOperator` | 2.602 s / 384,314 rows/s | 2.637 s / 379,278 rows/s | 2.905 s / 344,222 rows/s | n/a (server `INSERT...SELECT`) |
| `SQLInsertRowsOperator` | 13.734 s / 72,810 rows/s | 58.057 s / 17,225 rows/s | 26.656 s / 37,514 rows/s | none (stock) |
| `SQLBulkLoadOperator` | 1.689 s / 591,986 rows/s (`COPY`) | N/A (`bulk_load` NotImplementedError) | N/A (`bulk_load` NotImplementedError) | n/a |
| `GenericTransfer` | 14.320 s / 69,834 rows/s | 60.513 s / 16,525 rows/s | 28.443 s / 35,158 rows/s | see transfer table below |

Server-side `SQLExecuteQueryOperator` barely moves with hook choice. Row `insert_rows` (SQLInsertRows / GenericTransfer) is slowest on ODBC, then ADBC, then libpq. Native `COPY` is the fastest stock load on Postgres.

### Transfer operators (same extract SQL)

| Path | Time | Dest write |
| --- | --- | --- |
| Stock `GenericTransfer` traditional | 14.320 s / 69,834 rows/s | `PostgresHook.insert_rows` |
| Stock `GenericTransfer` ODBC dest | 60.513 s / 16,525 rows/s | `OdbcHook.insert_rows` (row-based; dest `price` is float8) |
| Stock `GenericTransfer` ADBC dest | 28.443 s / 35,158 rows/s | `AdbcHook.insert_rows` (row-based; dest `price` is float8) |
| `AdbcArrowTransfer` traditional source | 4.209 s / 237,606 rows/s | `adbc_ingest` into NUMERIC |
| `AdbcArrowTransfer` ADBC source | 3.409 s / 293,306 rows/s | `adbc_ingest` into NUMERIC (**8.34×** vs stock ADBC GT in **this** DAG run) |
| `CapabilityGenericTransfer` ADBC source+dest | 2.670 s / 374,553 rows/s | both-ends RecordBatch (**10.65×** vs stock ADBC GT in **this** DAG run) |

The 10.65× in this matrix is the same code path as section 1, measured in a longer serialized DAG after ODBC/libpq work on the same VM. The dedicated three-way run in section 1 is the number cited as **11.35×**. Do not average them with ODBC GT.

Post-load: `verify_operator_loads` PASS — copy-path dests match `bench_source` (`COUNT(*)=1000000` and payload digest); `SQLInsertRowsOperator` dests COUNT-only by design.

Nothing skipped in this matrix except `SQLBulkLoadOperator` on ODBC/ADBC (`NotImplementedError`, documented).

---

## 3. Multi-engine Phase-2 (DuckDB ↔ Postgres)

DAG: `bench_capability_duckdb`, run `manual__2026-11-03T14:03:30.012927+00:00`. File: `/opt/airflow/include/data/bench_cap.duckdb` (not the `bench_duckdb` file). JSON: `include/results/bench_capability_duckdb.json`.

Uses `transfer_with_capabilities` (the same helper as `CapabilityGenericTransfer`) with `DuckDBArrowCapabilityAdapter` and `AdbcArrowCapabilityAdapter`. Native DuckDB `to_arrow_reader` / `from_arrow`; Postgres dest uses `adbc_ingest`.

| Path | Time | Rows/s | Adapters | Verify |
| --- | --- | --- | --- | --- |
| DuckDB → DuckDB (columnar) | **0.428 s** | 2,335,855 | `DuckDBArrowCapabilityAdapter` → same (one connection; exclusive file lock) | PASS — DuckDB dest digest matches DuckDB source |
| DuckDB → Postgres Adbc (columnar) | **1.989 s** | 502,726 | native DuckDB Arrow → `AdbcArrowCapabilityAdapter` | PASS — Postgres `bench_cap_ddb_to_pg` digest matches `bench_source` |
| Postgres Adbc → DuckDB (columnar) | **0.925 s** | 1,081,540 | `AdbcArrowCapabilityAdapter` → native DuckDB Arrow | PASS — DuckDB dest digest matches DuckDB source |

Same Arrow table was loaded into Postgres `bench_source` and DuckDB `bench_source` during untimed setup, so both engines start from the same 1e6 rows.

Skipped: DuckDB row-path `executemany` of 1e6 Python tuples. That is not a Phase-2 baseline (`bench_duckdb` traditional insert was ~10+ minutes on this class of VM).

This section is **not** the 11.35× Postgres-Adbc both-ends claim. It shows the RecordBatch protocols work across engines, not only AdbcHook ↔ AdbcHook.

---

## Takeaway for the AIP

Stock `GenericTransfer` dest-writes with `insert_rows` on libpq, ODBC, and ADBC. At N=1e6 that row path is ~14 s (libpq), ~28–31 s (ADBC), ~60 s (ODBC). When **both** ends advertise RecordBatch read and write, the local helper streams Arrow batches (~2.7–2.8 s Adbc↔Adbc; ~0.4–2.0 s with DuckDB in the path) and still lands the same 1e6 rows. Faster is not fewer rows. Dest-only `AdbcArrowTransfer` remains a different claim.

## How to reproduce

```bash
git clone https://github.com/aoelvp94/airflow-adbc-vs-odbc-bench.git
cd airflow-adbc-vs-odbc-bench
git checkout cursor/aip-phase2-odbc-duckdb-d7e8   # or main once merged
cp .env.example .env
grep BENCH_ROW_COUNT .env    # 1000000

docker compose down -v
docker compose up -d --build
./scripts/wait_for_airflow.sh
./scripts/setup_connections.sh

# Section 1 (both-ends Postgres Adbc)
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags unpause bench_capability_transfer
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags trigger bench_capability_transfer

# Section 2 (full stock operator × postgres/odbc/adbc matrix)
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags unpause bench_sql_operators
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags trigger bench_sql_operators

# Section 3 (DuckDB multi-engine)
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags unpause bench_capability_duckdb
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags trigger bench_capability_duckdb
```

`./scripts/run_bench.sh` also triggers these DAGs (plus hook ETL / DuckDB insert-select / SQLite). Unit tests do not produce these timings: `PYTHONPATH=include pytest tests/ -q`.

### VM note (this run only)

On this cloud VM, Ubuntu’s **iptables-legacy FORWARD policy was DROP** while Docker programmed iptables-nft. Container-to-container TCP on the compose bridge timed out (`airflow db check` hung; api-server healthcheck failed). Host fix, not a DAG change:

```bash
sudo iptables-legacy -P FORWARD ACCEPT
sudo sysctl -w net.bridge.bridge-nf-call-iptables=0
docker compose restart airflow-apiserver airflow-dag-processor
docker compose up -d airflow-scheduler
```

Typical Docker Desktop / systemd Docker hosts do not need this. Do not treat it as an Airflow or ADBC issue.
