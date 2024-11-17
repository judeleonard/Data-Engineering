import psycopg2
from airflow.models import Variable



def create_database_hook():
    """This will create a database connection and return the hook for both prod and dwh database
     Returns:
            Database connection hook (conn)
    """
    # fetch data warehouse credentials from airflow metastore
    dwh = Variable.get("warehouse_secrets", deserialize_json=True)

    ###### data warehouse creds ###########
    DATABASE_DWH = dwh["database"]
    DB_HOST = dwh["db_host"]
    USER_DWH = dwh["user"]
    PASSWORD_DWH = dwh["password"]
    DB_PORT = int(dwh["db_port"])
   
    hook = psycopg2.connect(database = DATABASE_DWH,
                            user = USER_DWH,
                            host = DB_HOST,
                            password = PASSWORD_DWH,
                            port = DB_PORT)
    if hook is not None:
        print(f"connection established...")
    else:
        print("No connection")
    return hook