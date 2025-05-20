#!/usr/bin/env bash
set -x

if ! airflow connections get postgres_connection > /dev/null 2>&1; then
  echo "Add postgres connection"
  airflow connections add 'postgres_connection' \
                    --conn-type generic \
                    --conn-host "postgres" \
                    --conn-schema "$POSTGRES_DB" \
                    --conn-login "$POSTGRES_USER" \
                    --conn-password "$POSTGRES_PASSWORD" \
                    --conn-port "5432"
else
  echo "Postgres connection already exists. Skipping connection creation."
fi
