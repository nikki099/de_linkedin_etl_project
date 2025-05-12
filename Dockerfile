FROM python:3.12.8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV POETRY_VERSION=2.1.1
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow


WORKDIR $AIRFLOW_HOME

#Install system dependencies
RUN apt-get update && apt-get install -y \
build-essential \
libpq-dev \
&& rm -rf /var/lib/apt/lists/*

#Install poetry
RUN pip3 install --upgrade --no-cache-dir pip \
    && pip install poetry==${POETRY_VERSION}


#COPY ALL REQUIRED FILES
COPY pyproject.toml poetry.lock ./
COPY scripts ./scripts
COPY src ./src
COPY dbt_linkedin_etl_project ./dbt_linkedin_etl_project
COPY credentials ./credentials
COPY dags ./dags



#RUN Connection and entrypoint scripts
RUN chmod +x scripts/entrypoint.sh scripts/init_connections.sh


#Use poetry to install main dependencies
RUN poetry install --only main


#Install additional dependencies (airflow, dbt, snowflake)
RUN poetry run pip install \
    apache-airflow==2.8.4 \
    apache-airflow-providers-snowflake \
    dbt-snowflake\
    snowflake-connector-python \
    snowflake-sqlalchemy \
    apache-airflow-providers-http \
    dbt-core
