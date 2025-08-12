from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime, timedelta

CONNECTION_ID = "postgres_default"
SCHEMA_NAME = "refined"
DBT_PROJECT_PATH = "/usr/local/airflow/dags/astro_dbt"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 15),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="refined_models_run",
    default_args=default_args,
    description="refined models",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["astro", "refined"]
):

    profile_config = ProfileConfig(
        profile_name="etl_dbt",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=CONNECTION_ID,
            profile_args={"schema": SCHEMA_NAME},
        ),
    )

    execution_config = ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )
    
    start = EmptyOperator(task_id="start")

    dimensions_group = DbtTaskGroup(
        group_id="dimensions_models",
        project_config=ProjectConfig(
            DBT_PROJECT_PATH,
            models_relative_path="models"
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:models/refined/dimensions"]
        ),
        operator_args={
            "install_deps": True
        },
        default_args={
            "retries": 1,
            "retry_delay": timedelta(seconds=30)
        }
    )
    
    facts_group = DbtTaskGroup(
        group_id="facts_models",
        project_config=ProjectConfig(
            DBT_PROJECT_PATH,
            models_relative_path="models"
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:models/refined/facts"]
        ),
        operator_args={
            "install_deps": True
        },
        default_args={
            "retries": 1,
            "retry_delay": timedelta(seconds=30)
        }
    )
    
    aggregate_group = DbtTaskGroup(
        group_id="aggregate_models",
        project_config=ProjectConfig(
            DBT_PROJECT_PATH,
            models_relative_path="models"
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:models/refined/aggregate"]
        ),
        operator_args={
            "install_deps": True
        },
        default_args={
            "retries": 1,
            "retry_delay": timedelta(seconds=30)
        }
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> dimensions_group >> facts_group >> aggregate_group >> end