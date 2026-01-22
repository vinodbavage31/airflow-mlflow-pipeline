#!/bin/bash

set -e

echo "Initializing Airflow database..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true
airflow db migrate
echo "Starting Airflow webserver..."
exec airflow api-server