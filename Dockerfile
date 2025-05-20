FROM python:3.11.12-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow
WORKDIR $AIRFLOW_HOME

#Install system dependencies including postgresql-client
RUN apt-get update && apt-get install -y \
  build-essential \
  libpq-dev \
  postgresql-client \
  && rm -rf /var/lib/apt/lists/*

#Install required python modules
COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

#COPY ALL REQUIRED FILES
COPY .dbt/ .dbt/
COPY scripts ./scripts
COPY src ./src
COPY dbt_linkedin_etl_project ./dbt_linkedin_etl_project
# COPY credentials ./credentials
COPY dags ./dags


#RUN Connection and entrypoint scripts
RUN chmod +x /app/airflow/scripts/entrypoint.sh /app/airflow/scripts/init_connections.sh
