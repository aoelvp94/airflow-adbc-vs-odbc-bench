# ODBC Reference Configuration

The default demo compares **protocols against the same PostgreSQL 18**:

| Connection | Protocol | Hook |
| --- | --- | --- |
| `postgres_traditional` | libpq | `PostgresHook` |
| `postgres_odbc` | ODBC (unixODBC + psqlODBC + pyodbc) | `OdbcHook` |
| `postgres_adbc` | ADBC | `AdbcHook` |

ODBC is row-oriented. ADBC is Arrow. JDBC is not in this image.

There is **no SQL Server container** and **no cloud credentials**. FreeTDS + MSSQL in compose is heavier and not required to exercise `OdbcHook` / `GenericTransfer` / `SQLExecuteQueryOperator` / `SQLInsertRowsOperator`. `SQLBulkLoadOperator` cannot run on `OdbcHook` (`bulk_load` is `NotImplementedError`).

`AIRFLOW__PROVIDERS_ODBC__ALLOW_DRIVER_IN_EXTRA=true` is set in compose so the connection extra may name `Driver=PostgreSQL Unicode`.

## Local demo connection (`postgres_odbc`)

Created by `./scripts/setup_connections.sh`:

```bash
airflow connections add postgres_odbc \
    --conn-type odbc \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login benchuser \
    --conn-password benchpass \
    --conn-schema benchdb \
    --conn-extra '{"Driver":"PostgreSQL Unicode","driver":"PostgreSQL Unicode","sqlalchemy_scheme":"postgresql+pyodbc","placeholder":"?"}'
```

`sqlalchemy_scheme` must be `postgresql+pyodbc`. `OdbcHook` defaults to `mssql+pyodbc`, which would generate the wrong INSERT dialect.

## ODBC Connection Example (MSSQL / vendor driver)

For production ODBC use cases (e.g., MSSQL, Oracle via ODBC) you install a vendor driver. That path is **not** in `./scripts/run_bench.sh`.

### Traditional ODBC Approach

```python
from airflow.providers.odbc.hooks.odbc import OdbcHook

# Using OdbcHook (row-based processing)
hook = OdbcHook(odbc_conn_id='mssql_odbc')

# Insert rows (traditional row-by-row)
hook.insert_rows(
    table='target_table',
    rows=data_rows,
    target_fields=['col1', 'col2', 'col3']
)

# Select rows (returns list of tuples)
conn = hook.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT * FROM source_table")
rows = cursor.fetchall()
```

### ADBC Approach (When Driver Available)

```python
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook
import pyarrow as pa

# Using AdbcHook (Arrow-based processing)
hook = AdbcHook(conn_id='mssql_adbc')

# Insert as Arrow table (columnar transfer)
arrow_table = pa.table({
    'col1': values1,
    'col2': values2,
    'col3': values3
})

with hook.get_conn() as conn:
    with conn.cursor() as cursor:
        cursor.adbc_ingest(
            table_name='target_table',
            data=arrow_table,
            mode='append'
        )

# Select as Arrow table
with hook.get_conn() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM source_table")
        arrow_table = cursor.fetch_arrow_table()
```

## Airflow Connection Configuration

### ODBC Connection (Traditional)

**Connection Type**: ODBC

**Configuration**:
- Host: `<server>`
- Port: `1433` (for MSSQL)
- Schema: `<database>`
- Login: `<username>`
- Password: `<password>`
- Extra:
  ```json
  {
    "driver": "ODBC Driver 18 for SQL Server",
    "TrustServerCertificate": "yes"
  }
  ```

**CLI Command**:
```bash
airflow connections add mssql_odbc \
    --conn-type odbc \
    --conn-host myserver.database.windows.net \
    --conn-port 1433 \
    --conn-login myuser \
    --conn-password mypassword \
    --conn-schema mydatabase \
    --conn-extra '{"driver": "ODBC Driver 18 for SQL Server"}'
```

### ADBC Connection

**Connection Type**: `adbc` (this demo) or Generic with an ADBC driver extra.

```bash
airflow connections add postgres_adbc \
    --conn-type adbc \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login benchuser \
    --conn-password benchpass \
    --conn-schema benchdb \
    --conn-extra '{"driver": "adbc_driver_postgresql", "db_kwargs": {"uri": "postgresql://benchuser:benchpass@postgres:5432/benchdb"}}'
```

## ADBC Driver Availability (2026)

### Production Ready
- **PostgreSQL**: `adbc-driver-postgresql` ✅ Stable
- **SQLite**: `adbc-driver-sqlite` ✅ Stable
- **Snowflake**: `adbc-driver-snowflake` ✅ Stable
- **Flight SQL**: `adbc-driver-flightsql` ✅ Stable

### Experimental
- **SQL Server**: Limited ADBC support (use Flight SQL proxy)
- **BigQuery**: Use Flight SQL connector
- **MySQL**: Use Flight SQL or wait for native driver

### Legacy ODBC Required
- **Oracle**: ODBC still recommended
- **Teradata**: ODBC only
- **DB2**: ODBC only
- **Proprietary databases**: ODBC only

## When to Use ODBC vs ADBC

| Factor | Use ODBC/Traditional | Use ADBC |
|--------|---------------------|----------|
| **Database** | ODBC-only databases | ADBC driver available |
| **Workload** | Transactional (OLTP) | Analytical (OLAP) |
| **Row count** | Small (<10k rows) | Large (>10k rows) |
| **Data format** | Row-oriented processing | Columnar processing |
| **Integration** | ODBC-dependent tools | Arrow/Parquet pipelines |
| **Driver maturity** | Mature, stable drivers | Newer drivers |

## Resources

- [ADBC Specification](https://arrow.apache.org/adbc/)
- [Airflow Arrow Provider Docs](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/)
- [ADBC Drivers](https://arrow.apache.org/adbc/current/driver/installation.html)
- [Airflow ODBC Provider](https://airflow.apache.org/docs/apache-airflow-providers-odbc/stable/)
- [ODBC Connection extra / sqlalchemy_scheme](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/connections.html)
