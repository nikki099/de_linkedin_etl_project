FROM python:3.12.8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV POETRY_VERSION=2.1.1
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/AIRFLOW_HOME


WORKDIR $AIRFLOW_HOME

RUN apt-get update && apt-get install curl -y


#COPY ALL REQUIRED FILES
COPY pyproject.toml poetry.lock ./
COPY scripts ./scripts
COPY src ./src
COPY dbt_linkedin_etl_project ./dbt_linkedin_etl_project
COPY credentials ./credentials
#COPY AIRFLOW DAGs
COPY dags ./dags
RUN chmod +x scripts/entrypoint.sh
RUN chmod +x scripts/init_connections.sh


# #Install additional dependencies
# RUN pip install apache-airflow==2.9.1 \
#     apache-airflow-providers-snowflake \
#     dbt-snowflake\
#     snowflake-connector-python \
#     snowflake-sqlalchemy \
#     apache-airflow-providers-dbt-core \




RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main
