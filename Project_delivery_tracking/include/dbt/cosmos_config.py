from cosmos.config import ProfileConfig, ProjectConfig
from cosmos import ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping


project_root = "/usr/local/airflow"

DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name='time_logger',
    target_name='dev',
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_connection_id",
        profile_args={"schema": "analytics",
                      "threads": 4
        
        },
    ),
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=f"{project_root}/include/dbt/",
)

DBT_EXECUTABLE_PATH = ExecutionConfig(
    dbt_executable_path=f"{project_root}/dbt_venv/bin/dbt"
)

