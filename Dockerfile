# Official Airflow 3.3.2 image (apache-airflow-providers-apache-arrow requires >=2.11.0).
FROM apache/airflow:3.3.2-python3.12

USER root

# unixODBC is only needed if you exercise the ODBC provider locally.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        unixodbc \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# constraints-3.3.2 / Python 3.12. Arrow provider is not in that file; pip still
# installs it from requirements.txt at the pinned version.
ARG AIRFLOW_VERSION=3.3.2
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /requirements.txt \
        --constraint "${CONSTRAINT_URL}"

RUN python -c "import adbc_driver_postgresql; print('ADBC PostgreSQL driver installed')" \
    && python -c "import adbc_driver_sqlite; print('ADBC SQLite driver installed')" \
    && python -c "import duckdb, adbc_driver_duckdb; print('DuckDB + ADBC driver installed')" \
    && python -c "import pyarrow; print('PyArrow installed')" \
    && python -c "from airflow.providers.apache.arrow.hooks.adbc import AdbcHook; print('AdbcHook available')" \
    && python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('PostgresHook available')" \
    && python -c "from airflow.providers.standard.operators.python import PythonOperator; print('PythonOperator available')"
