# Airflow ADBC vs Traditional Hooks Benchmark

A practical demonstration showing why **Apache Airflow should keep ADBC as an opt-in choice** alongside traditional database hooks (ODBC, DbApi), not as a replacement.

## Why This Matters

ADBC (Arrow Database Connectivity) offers **significantly faster bulk data operations** through Apache Arrow's columnar format, but it's not always the right choice:

- **ADBC excels at**: Bulk transfers, analytics workloads, large result sets, Arrow-native pipelines
- **Traditional hooks work for**: Transactional operations, small queries, mature driver ecosystems, ODBC-only databases

**This demo proves both have value** — users need the choice based on their workload.

## Architecture

This benchmark compares two approaches for PostgreSQL operations:

1. **Traditional Path**: `PostgresHook` (psycopg2-based, row-by-row processing)
2. **ADBC Path**: `AdbcHook` (Arrow-based, columnar batches)

The comparison represents the same pattern for ODBC scenarios:
- `OdbcHook` → row-based processing (like `PostgresHook`)
- `AdbcHook` → Arrow-based processing (same API)

## Stack

- **Apache Airflow**: 2.10+ with LocalExecutor
- **Database**: PostgreSQL 15 (representative SQL database)
- **Providers**: `apache-airflow-providers-postgres`, `apache-airflow-providers-apache-arrow`
- **Drivers**: `psycopg2-binary`, `adbc-driver-postgresql`
- **Container**: Docker Compose for full local reproducibility

## Quick Start

### Prerequisites

- Docker & Docker Compose
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

- **Airflow UI**: http://localhost:8080 (user: `airflow`, password: `airflow`)
- **Results**: `include/results/*.json` and printed table in console

Expected output:

```
╔══════════════════════════════════════════════════════════════════════╗
║  ADBC vs Traditional Hooks - Performance Comparison                  ║
╠══════════════════════════════════════════════════════════════════════╣
║  Bulk Insert (100,000 rows)                                          ║
║    Traditional (PostgresHook): 12.3s  |  8,130 rows/s                ║
║    ADBC (Arrow batches):        2.1s  | 47,619 rows/s  [5.9x faster]║
╠══════════════════════════════════════════════════════════════════════╣
║  Bulk Select (100,000 rows)                                          ║
║    Traditional (row fetch):     8.7s  | 11,494 rows/s                ║
║    ADBC (Arrow table):          1.4s  | 71,429 rows/s  [6.2x faster]║
╠══════════════════════════════════════════════════════════════════════╣
║  Transfer (50,000 rows extract + load)                               ║
║    Traditional (row-by-row):    9.8s  |  5,102 rows/s                ║
║    ADBC (Arrow native):         1.9s  | 26,316 rows/s  [5.2x faster]║
╚══════════════════════════════════════════════════════════════════════╝
```

## Benchmark DAGs

### 1. `bench_bulk_insert`

Inserts configurable rows (default 100k) comparing:
- **Traditional**: `PostgresHook.insert_rows()` - row-by-row iteration
- **ADBC**: `AdbcHook` with Arrow batches - columnar transfer

### 2. `bench_bulk_select`

Fetches large result sets comparing:
- **Traditional**: `cursor.fetchall()` - list of tuples
- **ADBC**: `AdbcHook` - direct Arrow Table

### 3. `bench_transfer`

Extract-transform-load pattern:
- **Traditional**: Read rows → process → insert rows
- **ADBC**: Arrow table → transform → Arrow write

## Configuration

Edit `.env` or Airflow Variables:

- `BENCH_ROW_COUNT`: Number of rows for insert/select tests (default: 100000)
- `BENCH_BATCH_SIZE`: Batch size for traditional inserts (default: 1000)
- `BENCH_TRANSFER_ROWS`: Rows for transfer test (default: 50000)

## Results Interpretation

### When ADBC Wins (Why Keep It)

✅ **Bulk data transfers** (5-10x faster)
✅ **Analytics workloads** (columnar format matches analytics)
✅ **Arrow-native pipelines** (PyArrow → Pandas → Parquet workflows)
✅ **Cloud data warehouse loads** (Snowflake, BigQuery with Arrow)

### When Traditional Hooks Win (Why Not Replace)

✅ **Transactional operations** (single-row CRUD, ACID guarantees)
✅ **ODBC-only databases** (legacy systems, proprietary databases)
✅ **Mature driver ecosystems** (ODBC has decades of driver maturity)
✅ **Complex SQL** (stored procedures, database-specific features)

## Caveats

1. **Driver Maturity**: ADBC drivers are newer (PostgreSQL stable, MSSQL experimental as of 2026)
2. **Server-Side Arrow**: Performance depends on database Arrow support (Flight SQL servers excel)
3. **Workload Matters**: Small queries (<1000 rows) show minimal difference
4. **Memory**: Arrow uses more memory for columnar representation

## ODBC Use Case

For MSSQL or other ODBC databases, the same pattern applies:

```python
# Traditional ODBC (row-based)
from airflow.providers.odbc.hooks.odbc import OdbcHook
hook = OdbcHook(odbc_conn_id='my_mssql')

# ADBC (Arrow-based, when driver available)
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
hook = AdbcHook(conn_id='my_mssql_adbc', driver='adbc_driver_snowflake')
```

## Project Structure

```
.
├── dags/                          # Airflow DAGs
│   ├── bench_bulk_insert.py       # Insert comparison
│   ├── bench_bulk_select.py       # Select comparison
│   └── bench_transfer.py          # Extract-load comparison
├── include/
│   ├── results/                   # Benchmark outputs (JSON/CSV)
│   └── utils/                     # Shared benchmark utilities
├── scripts/
│   ├── run_bench.sh               # Main benchmark runner
│   ├── setup_connections.sh       # Configure Airflow connections
│   └── wait_for_airflow.sh        # Startup health check
├── docker-compose.yml             # Full Airflow + Postgres stack
├── Dockerfile                     # Custom Airflow image with ADBC
├── requirements.txt               # Python dependencies
└── .env.example                   # Configuration template
```

## References

- [Airflow Apache Arrow Provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/index.html)
- [ADBC Documentation](https://arrow.apache.org/adbc/)
- [AdbcHook API Reference](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/_api/airflow/providers/apache/arrow/hooks/adbc/index.html)

## License

Apache License 2.0 - See [LICENSE](LICENSE)

## Contributing

This is a personal demonstration repository. For Airflow improvements, contribute to [apache/airflow](https://github.com/apache/airflow).

## Author

Built by @aoelvp94 to demonstrate ADBC value for the Apache Airflow community.
