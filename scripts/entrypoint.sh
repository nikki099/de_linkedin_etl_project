#!/bin/bash
set -e

echo "POSTGRES_DB: $POSTGRES_DB"
echo "POSTGRES_USER: $POSTGRES_USER"
echo "POSTGRES_PASSWORD: $POSTGRES_PASSWORD"

# Initialize airflow db
if ! airflow db upgrade; then
  echo "Database upgrade failed!"
  exit 1
fi

if ! airflow db init; then
  echo "Database initialization failed!"
  exit 1
fi

# Check if user exists
if ! airflow users list | grep -q "admin"; then
    # create admin user
    airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
else
    echo "Admin user already exists. Skipping user creation."
fi

# Set Airflow Connections
/app/airflow/scripts/init_connections.sh

# Start the corresponding Airflow process based on the parameter
if [ "$1" == "webserver" ]; then
  airflow webserver
elif [ "$1" == "scheduler" ]; then
  airflow scheduler
else
  exec "$@"
fi
