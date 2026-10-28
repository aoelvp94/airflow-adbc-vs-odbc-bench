# Use official Airflow 2.10+ image
FROM apache/airflow:2.10.3-python3.11

USER root

# Install system dependencies for ODBC (for reference/documentation)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        unixodbc \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Airflow providers and ADBC drivers
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Verify installations
RUN python -c "import adbc_driver_postgresql; print('✓ ADBC PostgreSQL driver installed')" \
    && python -c "import pyarrow; print('✓ PyArrow installed')" \
    && python -c "from airflow.providers.apache.arrow.hooks.adbc import AdbcHook; print('✓ AdbcHook available')" \
    && python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('✓ PostgresHook available')"
