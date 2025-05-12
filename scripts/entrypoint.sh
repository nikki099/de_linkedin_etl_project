#!/usr/bin/env bash

set -e
set -x

airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow

scripts/init_connections.sh

airflow webserver
