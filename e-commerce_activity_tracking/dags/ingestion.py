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
import sql



def get_connection():
    """
    Args:
        None

    simple function to allow easy connection to Postgres db without explicitly parsing 
    connection parameters each time we want to establish a connection. Credentials are fetched
    from airflow metastore"""

    # config variables
    # task_config = Variable.get("my_credential_secrets", deserialize_json=True)
    # DATABASE = task_config["database"]
    DATABASE = str(Variable.get('db_name'))
    USER = str(Variable.get('username'))
    PASSWORD = str(Variable.get('password'))
    HOST = str(Variable.get('host'))
    #conn = psycopg2.connect("database={DATABASE} user={USER} host={HOST} password={PASSWORD}")
    conn = psycopg2.connect("dbname=d2b_assessment user=judendu4707 host=d2b-internal-assessment-dwh.cxeuj0ektqdz.eu-central-1.rds.amazonaws.com password=ksWSvpuBKa")
    return conn



def load_raw_data(prefix: str, datapath: str, table_name: str):
    """
    Args:
       - prefix: filename identifier
       - datapath: directory path to file
       - table_name: table name to copy file to in csv format

    this function takes each of the downloaded file and loads it into
    our staging schema tables.
    
    """
    # loop through and read all downloaded csv files 
    connection = get_connection()
    cur = connection.cursor()
    if prefix == 'orders':
        with open(datapath, 'r') as f:
            next(f) # Skip the header row.
            cur.copy_from(f, table_name, sep=',', null="")
            f.close()
    elif prefix == 'reviews':
        with open(datapath, 'r') as f:
            next(f)
            cur.copy_from(f, table_name, sep=',', null="")
            f.close()
    elif prefix == 'shipment_deliveries':
        with open(datapath, 'r') as f:
            next(f)
            cur.copy_from(f, table_name, sep=',', null="")
            f.close()
    if connection is not None:
        connection.commit()
        cur.close()
        print("file was loaded successfully")



def loading_table(table):
    """
    Args:
        table: table name to be loaded
    """
    conn = get_connection()
    postgres_hook = conn.cursor()
    if table == 'agg_order_public_holiday':
        sql_operations = sql.load_agg_order_public_holiday_table
    elif table == 'agg_shipment':
        sql_operations = sql.load_agg_shipment_table
    else:
        sql_operations = sql.load_best_performing_product
    postgres_hook.execute(sql_operations)
    conn.commit()

    print(f"Table {table} was loaded successfully")



def create_table(table):
    """
    Args:
        table: name of the table to be created
    """
    conn = get_connection()
    postgres_hook = conn.cursor()
    if table == 'orders_staging':
        sql_operations = sql.create_orders_staging
    elif table == 'reviews_staging':
        sql_operations = sql.create_reviews_staging
    elif table == 'shipment_del_staging':
        sql_operations = sql.create_shipment_delivery_staging
    elif table == 'agg_public_holiday':
        sql_operations = sql.create_agg_order_public_holiday_table
    elif table == 'agg_shipment_deliveries':
        sql_operations = sql.create_agg_shipment_table
    else:
        sql_operations = sql.create_best_performing_product_table

    postgres_hook.execute(sql_operations)
    conn.commit()

    print(f"Table {table} was created successfully")



def clean_up():
    """
    Args:
        None
    This task cleans up the entire process after the pipeline have succeeded.

    Inluding:
            removing all downloaded raw data files from local staging directory to make way for the next run.
            drop all tables from our staging schema as it is no longer useful after data transformation
        
    """
    download_path = '/opt/airflow/Files/*.csv'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql.drop_staging_tables)
    if conn is not None:
        conn.commit()
        cursor.close()
        conn.close()
    # clear csv file for the next run
    for f in glob.glob(download_path):
        if f.rsplit('/')[-1]:
            os.remove(f)
    print(f"{f} has been successfully removed")



def export_data(table_name, bucket, location):
    """
    Args:
        bucket: s3 bucket name to export files to
        table_name: name of the schema containing the table we want to export
        location: location to export file in s3

    function that exports each of the transformed tables analytics schema to s3
    """
    import boto3
    from botocore.handlers import disable_signing
    
    conn = get_connection()
    cur = conn.cursor()
    #s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='eu-central-1')
    resource = boto3.resource('s3')
    resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    if table_name == 'agg_public_holiday':
        select_query = 'judendu4707_analytics.agg_public_holiday'
        query = f"""COPY {select_query} TO STDIN \
            WITH (FORMAT csv, DELIMITER ',', QUOTE '"', HEADER TRUE)"""
        file = io.StringIO()
        cur.copy_expert(query, file)
        resource.Object(bucket, location).put(Body=file.getvalue())

    elif table_name == 'agg_shipments':
        select_query = 'judendu4707_analytics.agg_shipments'
        query = f"""COPY {select_query} TO STDIN \
            WITH (FORMAT csv, DELIMITER ',', QUOTE '"', HEADER TRUE)"""
        file = io.StringIO()
        cur.copy_expert(query, file)
        resource.Object(bucket, location).put(Body=file.getvalue())
    else: 
        select_query = 'judendu4707_analytics.best_performing_product'
        query = f"""COPY {select_query} TO STDIN \
                WITH (FORMAT csv, DELIMITER ',', QUOTE '"', HEADER TRUE)"""
        file = io.StringIO()
        cur.copy_expert(query, file)
        resource.Object(bucket, location).put(Body=file.getvalue())
    print(f"{table_name}.csv was successfully copied to S3")



def data_quality_checks(tables):
    """
    Args:
        tables: list of tables to perform data quality checks on 
    """
    tables = tables.split(',')
    for table in tables:
        conn = get_connection()
        cursor = conn.cursor()
        query_records = '''SELECT COUNT(*) FROM {table}'''
        if len(query_records) < 1 or len(query_records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no result.")
       # num_records = int(query_records[0][0])
       # if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows.")
        logging.info(f"Data quality on table {table} check passed with {len(query_records)} number of records")


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'judeleonard',
    'retries': 1,
    'start_date': days_ago(5),
    'retry_delay': timedelta(seconds=3)
}

with DAG('d2b_assessment',
         schedule_interval='@daily',
         default_args = default_args,
         description ='Data2Bots Data Engineering Technical Assessment',
         catchup=False) as dag:


#### creating tables ##########

    creating_orders_staging_task = PythonOperator(
        task_id = 'orders_staging',
        op_kwargs={'table': 'orders_staging'},
        python_callable=create_table,
    )

    creating_reviews_staging_task = PythonOperator(
        task_id = 'reviews_staging',
        op_kwargs={'table': 'reviews_staging'},
        python_callable=create_table,
    )

    creating_shipment_staging_task = PythonOperator(
        task_id = 'shipment_staging',
        op_kwargs={'table': 'shipment_del_staging'},
        python_callable=create_table,
    )

    creating_agg_public_holiday_task = PythonOperator(
        task_id = 'agg_public_holiday',
        op_kwargs={'table': 'agg_public_holiday'},
        python_callable=create_table,
    )

    creating_agg_shipment_task = PythonOperator(
        task_id = 'agg_shipment_deliveries',
        op_kwargs={'table': 'agg_shipment_deliveries'},
        python_callable=create_table,
    )

    creating_best_performing_products_task = PythonOperator(
        task_id = 'best_performing_products',
        op_kwargs={'table': 'best_performing_product'},
        python_callable=create_table,
    )

    #### Loading tables ########

    loading_order_staging_task = PythonOperator(
        task_id = 'loading_order_staging',
        op_kwargs={'prefix': 'orders',
                    'datapath': '/opt/airflow/Files/orders.csv',
                   'table_name': 'judendu4707_staging.orders_staging'
        },
        python_callable=load_raw_data,
    )


    loading_reviews_staging_task= PythonOperator(
        task_id = 'loading_reviews_staging',
        op_kwargs={'prefix': 'reviews',
                    'datapath': '/opt/airflow/Files/reviews.csv',
                   'table_name': 'judendu4707_staging.reviews_staging'
        },
        python_callable=load_raw_data,
    )

    loading_shipment_delivery_staging_task = PythonOperator(
        task_id = 'loading_shipments_staging',
        op_kwargs={'prefix': 'shipment_deliveries',
                    'datapath': '/opt/airflow/Files/shipment_deliveries.csv',
                   'table_name': 'judendu4707_staging.shipment_del_staging'
        },
        python_callable=load_raw_data,
    )


    loading_agg_order_public_holiday_task = PostgresOperator(
        task_id = 'loading_agg_public_holiday',
        sql = sql.load_agg_order_public_holiday_table,
        postgres_conn_id="postgres_conn"
    #     op_kwargs={'table': 'agg_order_public_holiday'},
    #     python_callable=loading_table,
    )

    loading_agg_shipment_task = PostgresOperator(  
        task_id = 'loading_agg_shipment',
        sql = sql.load_agg_shipment_table,
        postgres_conn_id="postgres_conn"
       
    )

    loading_best_product_task = PostgresOperator(
        task_id = 'loading_best_product',
        sql=sql.load_best_performing_product,
        postgres_conn_id="postgres_conn"
    )
    
##########  utility tasks ##############

    download_Staging_data_from_s3_task = BashOperator(
    task_id ='download_data_from_s3',
    bash_command='python /opt/airflow/scripts/get_s3data.py',

    )

    order_file_sensor_task = FileSensor(
        task_id='order_file_sensor',
        fs_conn_id='order_file_system',
        filepath='/opt/airflow/Files/orders.csv',
        poke_interval=10,
        
    )

    review_file_sensor_task = FileSensor(
        task_id='review_file_sensor',
        fs_conn_id='review_file_system',
        filepath='/opt/airflow/Files/reviews.csv',
        poke_interval=10,
        
    )

    shipment_del_file_sensor_task = FileSensor(
        task_id='shipment_del_file_sensor',
        fs_conn_id='shipment_del_file_system',
        filepath='/opt/airflow/Files/shipment_deliveries.csv',
        poke_interval=10,
        
    )

    start_execution_task = DummyOperator(
        task_id = 'start_execution',
        
    )

    downloaded_data_ready_task = DummyOperator(
        task_id = 'staging_data_ready',
    
    )

    tables_created_dwh_task = DummyOperator(
        task_id = 'tables_created_in_dhw',
        
    )

    end_execution_task = DummyOperator(
        task_id = 'end_execution',
        
    )

    loaded_data_ready_task = DummyOperator(
        task_id = 'loaded_data_ready',
        
    )

    exporting_agg_public_holiday_task = PythonOperator(
        task_id = 'agg_public_holiday_to_S3',
        python_callable=export_data,
        op_kwargs={
            'table_name': 'agg_public_holiday',
            'bucket': str(Variable.get('s3_bucketname')),
            'location': str(Variable.get('agg_public_location'))
            },
    
    )


    exporting_agg_shipments_task = PythonOperator(
        task_id = 'agg_shipments_to_S3',
        python_callable=export_data,
        op_kwargs={
            'table_name': 'agg_shipments',
            'bucket': str(Variable.get('s3_bucketname')),
            'location': str(Variable.get('agg_shipment_location'))
            },
    )


    exporting_best_performing_product_task = PythonOperator(
        task_id = 'best_performing_product_to_S3',
        python_callable=export_data,
        op_kwargs={
            'table_name': 'best_performing_product',
            'bucket': str(Variable.get('s3_bucketname')),
            'location': str(Variable.get('best_product_location'))
            },

    )


    clean_up_task= PythonOperator(
        task_id = 'clean_up',
        python_callable=clean_up,
    )


    data_quality_check_task = PythonOperator(
        task_id = 'data_quality_checks',
        python_callable=data_quality_checks,
        op_kwargs={'tables': 'judendu4707_analytics.agg_public_holiday, judendu4707_analytics.agg_shipments, judendu4707_analytics.best_performing_product'},
        
    )



    start_execution_task >> download_Staging_data_from_s3_task >> [order_file_sensor_task, 
                                                                   review_file_sensor_task, 
                                                                   shipment_del_file_sensor_task]
    [order_file_sensor_task,  
    shipment_del_file_sensor_task,
    review_file_sensor_task] >> downloaded_data_ready_task
    downloaded_data_ready_task >> [creating_orders_staging_task,
                                creating_reviews_staging_task,
                                creating_shipment_staging_task,
                                creating_agg_public_holiday_task,
                                creating_agg_shipment_task,
                                creating_best_performing_products_task]

    [creating_orders_staging_task,
    creating_reviews_staging_task, 
    creating_shipment_staging_task,
    creating_agg_public_holiday_task,
    creating_agg_shipment_task,
    creating_best_performing_products_task] >> tables_created_dwh_task
    tables_created_dwh_task >> [loading_order_staging_task,
                                loading_reviews_staging_task,
                                loading_shipment_delivery_staging_task,
                                loading_agg_order_public_holiday_task,
                                loading_agg_shipment_task,
                                loading_best_product_task]

    [loading_order_staging_task,
    loading_reviews_staging_task,
    loading_shipment_delivery_staging_task,
    loading_agg_order_public_holiday_task,
    loading_agg_shipment_task,
    loading_best_product_task] >> loaded_data_ready_task  
    loaded_data_ready_task >> data_quality_check_task >> clean_up_task 
    clean_up_task >> [exporting_agg_public_holiday_task,
                                exporting_agg_shipments_task,
                                exporting_best_performing_product_task] >> end_execution_task