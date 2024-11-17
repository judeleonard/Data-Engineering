# airflow operations
import datetime
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# import dbt dev tools to ensure data integrity and transformation
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_PROFILE_CONFIG, DBT_EXECUTABLE_PATH
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

# import utility task libraries
import csv


# define project root path
project_root = "/usr/local/airflow"

def create_staging_tables(table):
    """
    this will create staging table in the staging schema database

    :params table: name of the table to be created in dwh
    """
    from include.scripts.utils import create_database_hook
    from include.scripts import stg_operations

    conn = create_database_hook()
    postgres_hook = conn.cursor()
    postgres_hook.execute("SET search_path TO staging;")
    if table == "clickup":
        sql_operations = stg_operations.create_stg_clickup_table
    elif table == "float":
        sql_operations = stg_operations.create_stg_float_allocation_table
        #sql_operations = stg_operations.create_stg_time_logger_table
    else:
        sql_operations = stg_operations.create_stg_time_logger

    postgres_hook.execute(sql_operations)
    conn.commit()
    print(f"{table} was created successfully")



def staging_to_dwh(data_path: str, table_name: str) -> None:
    """
    This functions allows you to move data from a local file system to
    databases (Fs -> olap).

    :param data_path: directory path to file to copy from
    :param table_name: name of the table within the database
    output:
         ouputs a database table copied from a csv file.
    Return:
        None
    """
    from include.scripts.utils import create_database_hook

    conn = create_database_hook()
    cur = conn.cursor()
    cur.execute("SET search_path TO staging;")
    with open(data_path, 'r') as f:
        next(f) # skip the header row
        cur.copy_from(f, table_name, sep=',', null="")

    conn.commit()           
    print(f"{data_path} successfully copied to warehouse {table_name}...")



def cleanup_process() -> None:
    """
    This task cleans up the entire process after the pipeline have succeeded.
    drop all tables from our staging schema as it is no longer useful after data transformation.
            
    """
    from include.scripts.utils import create_database_hook
    from include.scripts import stg_operations    

    conn = create_database_hook()
    cursor = conn.cursor()
    cursor.execute("SET search_path TO staging;")
    cursor.execute(stg_operations.drop_staging_tables)
    if conn is not None:
        conn.commit()
        conn.close()
    print("staging tables has been successfully dropped")




# initialize default arguments that we'll pass to our DAG
default_args = {
    'owner': 'AnyOwner',
    'retries': 2,
    'start_date': datetime(2024, 1, 1),
    'retry_delay': timedelta(seconds=2)

}

with DAG(dag_id='project_time_tracking',
        schedule_interval='@daily',
        default_args = default_args,
        description ='pipeline to keep track of project time entries activities',
        catchup=False) as dag:
    
    
    # // creating staging tables //
    with TaskGroup("create_staging_tables", tooltip="this task will create the staging tables that will be utilized by dbt transform") as create_stging_tables:
        create_clickup_table = PythonOperator(
            task_id = 'create_clickup_table',
            op_kwargs={'table': 'clickup'},
            python_callable=create_staging_tables
        )

        create_float_table = PythonOperator(
            task_id = 'create_float_table',
            op_kwargs={'table': 'float'},
            python_callable=create_staging_tables
        )

        create_time_logger_table = PythonOperator(
            task_id = 'create_time_logger',
            op_kwargs={'table': 'stg_time_logger'},
            python_callable=create_staging_tables
        )
    
    # // analytics with quality data checks //
    transform = DbtTaskGroup(
        group_id='run_analytics_Ops',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
	    execution_config=DBT_EXECUTABLE_PATH,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/marts'],
            emit_datasets=False
        ),
        operator_args={
                "install_deps": True
            }
    )

    # // loading staging tables //
    with TaskGroup("load_fact_tables") as staging_tasks:
        staging_clickup = PythonOperator(
            task_id = 'staging_clickup',
            op_kwargs={'table_name': 'stg_clickups',
                    'data_path': f'{project_root}/include/dataset/clickup.csv',
            },
            python_callable=staging_to_dwh
            )
        
        staging_float = PythonOperator(
            task_id = 'staging_float',
            op_kwargs={'table_name': 'stg_float_allocation',
                    'data_path': f'{project_root}/include/dataset/float.csv',
            },
            python_callable=staging_to_dwh
            )
        
        staging_float = PythonOperator(
            task_id = 'staging_time_logger',
            op_kwargs={'table_name': 'stg_time_logger',
                    'data_path': f'{project_root}/include/dataset/time_entries_merged.csv',
            },
            python_callable=staging_to_dwh
            )


    start_execution = EmptyOperator(task_id='start_execution')
    

    end_execution = EmptyOperator(
        task_id = 'end_execution',    
    )

    clean_up_task= PythonOperator(
        task_id = 'cleanup',
        python_callable=cleanup_process,
    )



start_execution >> create_stging_tables >> staging_tasks
staging_tasks >> transform >> clean_up_task >> end_execution
