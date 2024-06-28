from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime


# define project root path
project_root = "/usr/local/airflow"

with DAG(
    'cricketSports',
    default_args={'owner': 'towhomitmayconern'},
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    @task
    def fetch_data():
        return BashOperator(
            task_id='run_first_python_script',
            bash_command=f'python {project_root}/include/scripts/download.py',
        ).execute(context={})

    @task
    def process_data():
        return BashOperator(
            task_id='run_second_python_script',
            bash_command=f'python {project_root}/include/scripts/process.py',
        ).execute(context={})

    # task dependencies
    run_data_extraction = fetch_data()
    run_processing_task = process_data()

    run_data_extraction >> run_processing_task