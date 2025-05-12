#!/usr/bin/env bash
set -x

# 让脚本无论在哪个目录下运行都能找到上层.env
cd "$(dirname "$0")"/..
source .env
echo "Add connection"

SNOWFLAKE_USER=$snowflake_user
SNOWFLAKE_PASSWORD=$snowflake_password
SNOWFLAKE_ACCOUNT="xy12345.west-europe.azure"
SNOWFLAKE_WAREHOUSE="SNOWFLAKE_LEARNING_WH"
SNOWFLAKE_DATABASE="linkedin_db"
SNOWFLAKE_SCHEMA="linkedin_raw"

echo "Add Snowflake connection"
airflow connections add 'snowflake_connection' \
    --conn-type snowflake \
    --conn-login "$SNOWFLAKE_USER" \
    --conn-password "$SNOWFLAKE_PASSWORD" \
    --conn-account "$SNOWFLAKE_ACCOUNT" \
    --conn-warehouse "$SNOWFLAKE_WAREHOUSE" \
    --conn-database "$SNOWFLAKE_DATABASE" \
    --conn-schema "$SNOWFLAKE_SCHEMA"
