Overview
========
This is a simple ETL data pipeline example that demonstrates the use of
the TaskFlow API using two tasks for extract and transform. The sole aim of this project is to demonstrate the use of Terraform to provision data infrastructures on Google cloud platform such as BigQuery and Cloud Storage. 

About the Data
============
The data comes from  this [website](https://cricsheet.org/), that covers alot of different cricket tournaments which is saved in a json format which is quite a heavy file. The data contains alot of information about every game including other metadata to help with the processing and normalization. This data requires alot of inspection and experiment before proceeding with the preprocessing and normalization.

Technologies
================
- Python: Data Extraction

- Pandas: Data Normalization and preprocessing

- Airflow: Orchestration using Astronomer

- Terraform: IAC tool for Provisioning infrastructure

- Bigquery: Data Warehouse

- Cloud Storage: This can serve as a place for storing Terraform state files and staging the normalized data for further preprocessing before moving to the provisioned Bigquery instance.






