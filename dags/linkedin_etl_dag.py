import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum
import json

DBT_DIR=os.getenv("DBT_DIR")
