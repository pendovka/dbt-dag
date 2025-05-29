import os
from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping 
from airflow import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime as pendulum_datetime


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dbt",
        profile_args={"database":"dbt_db", "schema": "dbt_schema"},
    )
)   

dbt_dag = DbtDag(
    dag_id="dbt_dag",
    project_config=ProjectConfig(Path(__file__).with_name("data_pipeline")),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('AIRFLOW_HOME', '/opt/airflow')}/dbt_venv/bin/dbt"
    ),
    start_date=pendulum_datetime(2024, 5, 26),
    catchup=False,
)
