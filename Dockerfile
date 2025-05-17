FROM python:3.11.12-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow
WORKDIR $AIRFLOW_HOME

#Install system dependencies
RUN apt-get update && apt-get install -y \
  build-essential \
  libpq-dev \
  && rm -rf /var/lib/apt/lists/*

#Install required modules
COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install psycopg2-binary

#COPY ALL REQUIRED FILES

COPY .dbt/ .dbt/
COPY scripts ./scripts
COPY src ./src
COPY dbt_linkedin_etl_project ./dbt_linkedin_etl_project
# COPY credentials ./credentials
COPY dags ./dags


#RUN Connection and entrypoint scripts
RUN chmod +x scripts/entrypoint.sh scripts/init_connections.sh
