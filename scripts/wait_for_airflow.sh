#!/bin/bash
set -e

echo "Waiting for Airflow to be ready..."

max_attempts=120
attempt=0

while [ $attempt -lt $max_attempts ]; do
    # Airflow 3 api-server health. AF2 /health is kept as a fallback.
    if curl -sf http://localhost:8080/api/v2/monitor/health >/dev/null \
        || curl -sf http://localhost:8080/health >/dev/null; then
        echo "Airflow is ready!"
        exit 0
    fi
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts - waiting 5s..."
    sleep 5
done

echo "Airflow did not become ready in time"
exit 1
