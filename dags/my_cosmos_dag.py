from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from pathlib import Path

# Path to this DAG file
DAGS_DIR = os.path.dirname(__file__)
DBT_DIR = os.path.join(DAGS_DIR, "nyc_taxi_green")

DBT_PROJECTS = "/opt/airflow/git/dbt_airflow_fabric.git/dags/nyc_taxi_green"

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dags" / "nyc_taxi_green"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

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
        bash_command=f"cd {DBT_ROOT_PATH} && DBT_PROFILES_DIR={DBT_ROOT_PATH} && dbt --version"
    )
    
    check_inside_noob = BashOperator(
         task_id = "check_inside_noob",
         bash_command = f"cd {DBT_ROOT_PATH} && ls -l"
    )
    
    check_inside_noob >> dbt_debug
