from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

local_tz=pendulum.timezone("Australia/Brisbane")


default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2025, 5, 13, 17, 0, tz=local_tz),
    "retries": 1,
}

with DAG(
    dag_id="linkedin_etl_dag",
    default_args=default_args,
    schedule="0 17 * * *", #5pm Brisbane time daily
    catchup=False,
    tags=["linkedin"],
) as dag:

    #step 1 run linkedin_api_raw_data_process.py to get api raw data and load into Snowflake

    extract_load = BashOperator(
        task_id="extract_load_api_data",
        bash_command=(
            "python src/de_linkedin_etl_project/linkedin_api_data_extraction/linkedin_api_raw_data_process.py"
        ),
        cwd="/app/airflow" #change to the working directory
    )

    #step 2 run all DBT models to transform the raw data and load into new tables in snowflake
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --profiles-dir /app/airflow/.dbt",
        cwd="/app/airflow/dbt_linkedin_etl_project", #change to the DBT project directory
        # env={"DBT_PROFILES_DIR": "/app/airflow/.dbt"} #specifically set the DBT profiles directory
    )

    extract_load >> run_dbt
