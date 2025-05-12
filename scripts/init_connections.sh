#!/usr/bin/env bash
set -x

# Always find the .env file in the parent directory
cd "$(dirname "$0")"/..
source .env
# echo "Add Snowflake connection"

# SNOWFLAKE_USER=$snowflake_user
# SNOWFLAKE_PASSWORD=$snowflake_password
# SNOWFLAKE_ACCOUNT="xy12345.west-europe.azure"
# SNOWFLAKE_WAREHOUSE="SNOWFLAKE_LEARNING_WH"
# SNOWFLAKE_DATABASE="linkedin_db"
# SNOWFLAKE_SCHEMA="linkedin_raw"


# airflow connections add 'snowflake_connection' \
#     --conn-type snowflake \
#     --conn-login "$SNOWFLAKE_USER" \
#     --conn-password "$SNOWFLAKE_PASSWORD" \
#     --conn-account "$SNOWFLAKE_ACCOUNT" \
#     --conn-warehouse "$SNOWFLAKE_WAREHOUSE" \
#     --conn-database "$SNOWFLAKE_DATABASE" \
#     --conn-schema "$SNOWFLAKE_SCHEMA"

set -x

echo "Add postgres connection"
airflow connections add 'postgres_connection' \
                    --conn-type postgres \
                    --conn-host "postgres" \
                    --conn-schema "$POSTGRES_DB" \
                    --conn-login "$POSTGRES_USER" \
                    --conn-password "$POSTGRES_PASSWORD" \
                    --conn-port "5432"
