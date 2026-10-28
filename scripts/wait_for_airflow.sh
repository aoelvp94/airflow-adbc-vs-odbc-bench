#!/bin/bash
set -e

echo "⏳ Waiting for Airflow to be ready..."

max_attempts=120
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -s http://localhost:8080/health | grep -q "healthy"; then
        echo "✅ Airflow is ready!"
        exit 0
    fi
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts - waiting 5s..."
    sleep 5
done

echo "❌ Airflow did not become ready in time"
exit 1
