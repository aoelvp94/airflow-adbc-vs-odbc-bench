# ODBC Reference Configuration

This demo uses PostgreSQL with native drivers as the benchmark database. However, the same pattern applies to ODBC connections, particularly for databases like Microsoft SQL Server.

## ODBC Connection Example

For production ODBC use cases (e.g., MSSQL, Oracle via ODBC):

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

**Connection Type**: Generic

**Configuration**:
- Host: `<server>`
- Port: `1433`
- Schema: `<database>`
- Login: `<username>`
- Password: `<password>`
- Extra:
  ```json
  {
    "driver": "adbc_driver_flightsql",
    "adbc.connection.timeout": "30000"
  }
  ```

**CLI Command**:
```bash
airflow connections add mssql_adbc \
    --conn-type generic \
    --conn-host myserver.database.windows.net \
    --conn-port 1433 \
    --conn-login myuser \
    --conn-password mypassword \
    --conn-schema mydatabase \
    --conn-extra '{"driver": "adbc_driver_flightsql"}'
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

## Performance Expectations

Based on this benchmark and similar patterns:

- **Small transfers (<1k rows)**: Minimal difference
- **Medium transfers (1k-100k rows)**: 2-5x faster with ADBC
- **Large transfers (>100k rows)**: 5-10x faster with ADBC
- **Very large (>1M rows)**: 10-20x faster with ADBC

## Example: Migrating ODBC to ADBC

### Before (ODBC)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook

def extract_data():
    hook = OdbcHook(odbc_conn_id='mssql')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM large_table")
    rows = cursor.fetchall()
    # Process rows...
```

### After (ADBC - when available)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook

def extract_data():
    hook = AdbcHook(conn_id='mssql_adbc')
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM large_table")
            arrow_table = cursor.fetch_arrow_table()
    # Process Arrow table (much faster for large data)...
```

## Resources

- [ADBC Specification](https://arrow.apache.org/adbc/)
- [Airflow Arrow Provider Docs](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/)
- [ADBC Drivers](https://arrow.apache.org/adbc/current/driver/installation.html)
- [Airflow ODBC Provider](https://airflow.apache.org/docs/apache-airflow-providers-odbc/stable/)
