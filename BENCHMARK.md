# AIP Phase-2 capability transfer timings

Measured on this repo’s compose stack. Local demo stubs only — not apache/airflow.

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
| DuckDB (image; not used in this DAG) | 2.0.0 |

DAG: `bench_capability_transfer`, run `manual__2026-11-03T13:36:10.632167+00:00`. Timed tasks ran **serially**. Durations are Airflow `TaskInstance.duration` (same sidecar files as `record_operator_duration`). JSON: `include/results/bench_capability_transfer.json` (gitignored; numbers copied here).

## Results (N = 1,000,000)

| Path | Time | Rows/s | Dest write | Verify |
| --- | --- | --- | --- | --- |
| Stock `GenericTransfer` Adbc → Adbc (row path) | **31.395 s** | 31,852 | `AdbcHook.insert_rows` (price stored as float8) | PASS — `COUNT(*)=1000000`, payload digest matches `bench_source` (price CAST to numeric(10,2)) |
| `CapabilityGenericTransfer` Adbc → Adbc (both-ends RecordBatch) | **2.765 s** | 361,627 | `get_record_batches` → `adbc_ingest` | PASS — same COUNT, same digest as source |
| `AdbcArrowTransfer` Adbc → Adbc (dest-only ingest) | **3.259 s** | 306,880 | `fetch_arrow_table` → `adbc_ingest` | PASS — same COUNT, same digest as source |

Speedup, columnar vs stock GenericTransfer: **11.35×**.

Post-load digest (all three dests vs `bench_source`): `n=1000000`, `sum_quantity=50504219`, `digest=15e2f3f24e6a33282fc269968622d312`. Task log: `Columnar path: both ends advertise RecordBatch read/write (AdbcArrowCapabilityAdapter -> AdbcArrowCapabilityAdapter)`.

`bench_sql_operators` was **not** re-run here. That DAG serializes the full operator × postgres/odbc/adbc matrix (stock GenericTransfer ODBC alone is ~65 s at this N). The three-way comparison above is the AIP evidence.

## Takeaway for the AIP

Stock `GenericTransfer` still dest-writes with `insert_rows` even when both connections are `adbc`, and that row path is the slow one (~31 s for 1e6 rows). When **both** hooks advertise RecordBatch read and write, a GenericTransfer-shaped helper can stream Arrow batches end-to-end (~2.8 s) and still land the same 1e6 rows and the same payload digest as `bench_source`. Dest-only `AdbcArrowTransfer` (`adbc_ingest` after a full Arrow table extract) is in the same ballpark (~3.3 s); it is a different claim (destination Arrow ingest, source may be row-based). Phase-2 is the both-ends capability switch, and this run shows it is faster than the stock row path without dropping or changing rows.

## How to reproduce

```bash
git clone https://github.com/aoelvp94/airflow-adbc-vs-odbc-bench.git
cd airflow-adbc-vs-odbc-bench
git checkout cursor/aip-phase2-recordbatch-cc57   # or main once merged
cp .env.example .env
grep BENCH_ROW_COUNT .env    # 1000000

docker compose down -v
docker compose up -d --build
./scripts/wait_for_airflow.sh
./scripts/setup_connections.sh

docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags unpause bench_capability_transfer
docker exec "$(docker compose ps -q airflow-apiserver)" airflow dags trigger bench_capability_transfer
# wait until the DAG run succeeds, then:
cat include/results/bench_capability_transfer.json
```

`./scripts/run_bench.sh` also triggers this DAG (plus the rest of the matrix). Unit tests do not produce these timings: `PYTHONPATH=include pytest tests/ -q`.

### VM note (this run only)

On this cloud VM, Ubuntu’s **iptables-legacy FORWARD policy was DROP** while Docker programmed iptables-nft. Container-to-container TCP on the compose bridge timed out (`airflow db check` hung; api-server healthcheck failed). Host fix, not a DAG change:

```bash
sudo iptables-legacy -P FORWARD ACCEPT
sudo sysctl -w net.bridge.bridge-nf-call-iptables=0
docker compose restart airflow-apiserver airflow-dag-processor
docker compose up -d airflow-scheduler
```

Typical Docker Desktop / systemd Docker hosts do not need this. Do not treat it as an Airflow or ADBC issue.
