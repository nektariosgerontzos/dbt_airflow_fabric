from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Path to this DAG file
DAGS_DIR = os.path.dirname(__file__)
DBT_DIR = os.path.join(DAGS_DIR, "nyc_taxi_green")

DBT_PROJECTS = "/opt/airflow/git/dbt_airflow_fabric.git/dags/nyc_taxi_green"

default_args = {
    "owner": "airflow",
    "retries": 0,
}


with DAG(
    dag_id="dbt_simple_run",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_DIR} && dbt debug --connection"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run"
    )
    
    check_folder_items = BashOperator(
          task_id = "check_folder_items",
          bash_command = f"dbt debug"
     )

    check_folder_items >> dbt_debug >> dbt_run
