# Official Airflow 2.11 image (apache-airflow-providers-apache-arrow requires >=2.11.0)
FROM apache/airflow:2.11.2-python3.11

USER root

# unixODBC is only needed if you exercise the ODBC provider locally.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        unixodbc \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN python -c "import adbc_driver_postgresql; print('ADBC PostgreSQL driver installed')" \
    && python -c "import adbc_driver_sqlite; print('ADBC SQLite driver installed')" \
    && python -c "import duckdb, adbc_driver_duckdb; print('DuckDB + ADBC driver installed')" \
    && python -c "import pyarrow; print('PyArrow installed')" \
    && python -c "from airflow.providers.apache.arrow.hooks.adbc import AdbcHook; print('AdbcHook available')" \
    && python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('PostgresHook available')"
