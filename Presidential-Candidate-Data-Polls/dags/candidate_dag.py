import psycopg2

# airflow operationss
import logging
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.dates import days_ago

# for utility tasks
import os
import io
import glob
import urllib.parse as up
import sql_statement



def get_connection():
    """
    Args:
        None

    simple function to allow easy connection to Postgres db without explicitly parsing 
    connection parameters each time we want to establish a connection. Credentials are fetched
    from airflow metastore"""
    DATABASE = str(Variable.get('db_name'))
    USER = str(Variable.get('username'))
    PASSWORD = str(Variable.get('password'))
    HOST = str(Variable.get('host'))
    #conn = psycopg2.connect("database={DATABASE} user={USER} host={HOST} password={PASSWORD}")
    up.uses_netloc.append("postgres")
    url = up.urlparse(str(Variable.get("DATABASE_URL")))
    conn = psycopg2.connect(database=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
    )
    return conn

    

def load_data(filename: str, datapath: str, table_name: str):
    """
    Args:
       - prefix: filename identifier
       - datapath: directory path to file
       - table_name: table name to copy file to in csv format

    this function takes each of the downloaded file and loads it into
    the staging schema tables.
    
    """
    # loop through and read all downloaded csv files 
    connection = get_connection()
    cur = connection.cursor()
    if prefix == 'twitter_fact':
        with open(datapath, 'r') as f:
            next(f) # Skip the header row.
            cur.copy_from(f, table_name, sep=',', null="")
        f.close()
    elif filename == 'twitter_dim':
        with open(datapath, 'r') as f:
            next(f)
            cur.copy_from(f, table_name, sep=',', null="")
        f.close()
 
    if connection is not None:
        connection.commit()
        cur.close()
        print("file was loaded successfully")


def create_table(table):
    """
    Args:
        table: name of the table to be created
    """
    conn = get_connection()
    postgres_hook = conn.cursor()
    if table == 'twitter_fact':
        sql_operations = sql_statement.create_twitter_fact
    elif table == 'candidate_table':
        sql_operations = sql_statement.create_candidate_analytics_table
    else:
        sql_operations = sql_statement.create_twitter_dim

    postgres_hook.execute(sql_operations)
    conn.commit()
    print(f"Table {table} was created successfully")



def aggregate_data(datapath: str):
    """
    Args:
        datapath: directory to where multiple csv files with same features are downloaded into
    
    this is an utility function that aggregates all the multiple downloaded csv files and 
    delete each of this file after aggregation.
    
    this function is applied to the directory containing all the downloaded twitter csv file
    for each candidate and article data for aggregation
    """
    files = [file for file in os.listdir(datapath) if ".csv" in file]
    # initialize a pandas dataframe 
    all_csv = pd.DataFrame()
    for file in files:
        current_csv = pd.read_csv(datapath+"/"+file)
        all_csv = pd.concat([all_csv, current_csv], axis=0)
    all_csv.to_csv(datapath + "agg-data.csv", encoding='utf8', index=False)
    # remove all the downloaded csv files except the aggregated csv file
    download_path = datapath + "*.csv"
    for f in glob.glob(download_path):
        if f.rsplit('/')[-1].startswith('agg-data'):
            continue
        print(f)
        os.remove(f)


def loading_table():
    """
    Args:
        None
    """
    conn = get_connection()
    postgres_hook = conn.cursor()
    sql_operations = sql.load_candidate_analytics_table
    postgres_hook.execute(sql_operations)
    if conn is not None:
        postgres_hook.execute(sql_statement.back_fills)
        conn.commit()

    print(f"Table was loaded successfully")



def data_quality_checks(table):
    """
    Args:
        table: table name to perform data quality checks on 
    """
    conn = get_connection()
    cursor = conn.cursor()
    query_records = '''SELECT COUNT(*) FROM {table}'''
    if len(query_records) < 1 or len(query_records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table} returned no result.")
    num_records = int(query_records[0][0])
    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table} contained 0 rows.")
    logging.info(f"Data quality on table {table} check passed with {len(query_records)} number of records")



def clean_up():
    """
    Args:
        None
    This task cleans up the entire process after the pipeline have succeeded.

    Inluding:
        removing all downloaded raw data files from local staging directory to make way for the next run.
        drop all tables from our staging schema as it is no longer useful after data transformation 
    """
    #download_path = '/opt/airflow/staging/*.csv'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql.drop_staging_tables)
    if conn is not None:
        conn.commit()
        cursor.close()
        conn.close()
    print("table has been successfully removed")



def sum_twitter_data(datapath, datatype):
    """
    Args:
        datapath: directory containing similar csv files for different candidates

    this function calls the aggregate_data method that sums up all the twitter data
    downloaded for each candidate into one single csv
    """
    if datatype == 'twitter_fact':
        data = aggregate_data(datapath)
    elif datatype == 'twitter_dim':
        data = aggregate_data(datapath)
    return data


default_args = {
    'owner': 'judeleonard',
    'retries': 1,
    'start_date': days_ago(5),
    'retry_delay': timedelta(seconds=3)
}


with DAG('presidential_election',
         schedule_interval='@daily',
         default_args = default_args,
         description ='2023 Presidential Election',
         catchup=False) as dag:
    

    #### creating tables ##########

    creating_twitter_fact_task = PythonOperator(
        task_id = 'twitter_fact',
        op_kwargs={'table': 'twitter_fact'},
        python_callable=create_table,
    )

    creating_twitter_dim_task = PythonOperator(
        task_id = 'twitter_dim',
        op_kwargs={'table': 'twitter_dim'},
        python_callable=create_table,
    )

    creating_candidate_table_task = PythonOperator(
        task_id = 'candidate_table',
        op_kwargs={'table': 'candidate_table'},
        python_callable=create_table,    
    )


    #### Loading tables ########

    loading_twitter_fact_task = PythonOperator(
        task_id = 'loading_twitter_fact',
        op_kwargs={'prefix': 'twitter_fact',
                    'datapath': '/opt/airflow/staging/agg-data.csv',
                   'table_name': 'twitter_fact'
        },
        python_callable=load_data,
    )


    loading_twitter_dim_task = PythonOperator(
        task_id = 'loading_twitter_dim',
        op_kwargs={'prefix': 'twitter_dim',
                    'datapath': '/opt/airflow/staging/dimensions/agg-data.csv',
                   'table_name': 'twitter_dim'
        },
        python_callable=load_data,
    )
    

    loading_candidate_task = PythonOperator(
        task_id = 'loading_candidate_table',
        python_callable=loading_table,
    )

    #### utility task ################

    twitter_fact_sensor_task = FileSensor(
        task_id='sensing_twitter_fact',
        fs_conn_id='twitter_fact_file',
        filepath='/opt/airflow/staging/agg-data.csv',
        poke_interval=10,
        
    )

    twitter_dim_sensor_task = FileSensor(
        task_id='sensing_twitter_dim',
        fs_conn_id='twitter_dim_file',
        filepath='/opt/airflow/staging/dimensions/agg-data.csv',
        poke_interval=10,
        
    )


    start_execution_task = DummyOperator(
        task_id = 'start_execution',
        
    )

    tables_created_dwh_task = DummyOperator(
        task_id = 'tables_created_in_dhw',
        
    )

    preprocessed_data_ready_task = DummyOperator(
        task_id = 'staging_data_ready',
    
    )

    loaded_data_ready_task = DummyOperator(
        task_id = 'loaded_data_ready',
        
    )

    end_execution_task = DummyOperator(
        task_id = 'end_execution',
        
    )

    clean_up_task= PythonOperator(
        task_id = 'cleaning_up',
        python_callable=clean_up,
    )


    data_quality_check_task = PythonOperator(
        task_id = 'data_quality_checks',
        python_callable=data_quality_checks,
        op_kwargs={'table': 'candidate_table'},
        
    )

    download_twitter_fact_table_task = BashOperator(
       task_id ='downloading_twitter_fact_table',
       bash_command='python /opt/airflow/scripts/base_twitter.py',

    )


    download_twitter_dim_table_task = BashOperator(
       task_id ='downloading_twitter_dim_table',
       bash_command='python /opt/airflow/scripts/twitter_dim.py',

    )

    preprocessing_task= PythonOperator(
        task_id = 'preprocessing_data',
        python_callable=sum_twitter_data,
    )




    start_execution_task >> [download_twitter_fact_table_task, download_twitter_dim_table_task]
    [download_twitter_fact_table_task, download_twitter_dim_table_task] >> preprocessing_task
    preprocessing_task >> [twitter_fact_sensor_task, twitter_dim_sensor_task]
    [twitter_fact_sensor_task, twitter_dim_sensor_task] >> preprocessed_data_ready_task
    preprocessed_data_ready_task >> [creating_twitter_fact_task,
                                     creating_twitter_dim_task,
                                     creating_candidate_table_task]
    [creating_twitter_fact_task,
     creating_twitter_dim_task,
     creating_candidate_table_task] >> tables_created_dwh_task
    tables_created_dwh_task >> [loading_twitter_fact_task,
                                loading_twitter_dim_task,
                                loading_candidate_task]
    [loading_twitter_fact_task,
     loading_twitter_dim_task,
     loading_candidate_task] >> loaded_data_ready_task >> data_quality_check_task
    data_quality_check_task >> clean_up_task >> end_execution_task




