import boto3
from botocore import UNSIGNED
from botocore.client import Config
from airflow.models import Variable
import os


def download_path(filename):
    """
    Args:
        filename: name of the file located in s3 bucket that will be downloaded
    
    output:
        downloads a specific filename from s3 and saves it to a local directory called Files
        for staging
    """
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket_name = str(Variable.get('s3_bucketname'))
    response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")
    # specify directory to save downloaded files
    local = '/opt/airflow/Files'
    if not os.path.exists(local):
       os.makedirs(local)

    s3.download_file(bucket_name, "orders_data/{}".format(filename), local + "/" "{}".format(filename))





if __name__ == "__main__":

    download_path("orders.csv")
    download_path("reviews.csv")
    download_path("shipment_deliveries.csv")