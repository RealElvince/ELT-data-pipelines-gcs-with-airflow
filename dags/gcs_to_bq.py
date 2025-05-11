from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from  datetime import datetime
from dotenv import load_dotenv
import os

# load environment variable

load_dotenv()

# set default args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'depend_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


# configuration variables
BUCKET_NAME = os.getenv('BUCKET_NAME')
SOURCE_OBJECT = os.getenv('SOURCE_OBJECT')
DESTINATION_PROJECT_DATASET_TABLE = os.getenv('DESTINATION_PROJECT_DATASET_TABLE')
BIGQUERY_TABLE=os.getenv('BIGQUERY_TABLE')

# Validate required environment variables
if not BUCKET_NAME or not SOURCE_OBJECT or not BIGQUERY_TABLE or not DESTINATION_PROJECT_DATASET_TABLE:
    raise ValueError("Required environment variables (BUCKET_NAME, SOURCE_OBJECT, or DESTINATION_PROJECT_DATASET_TABLE) are missing.")

# define the DAG
with DAG(
    dag_id='load_csv_to_bq',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date = datetime(2023, 10, 1),
    tags=['bigquery', 'gcs', 'csv'],

) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task',
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Task 1: define the task to load CSV from GCS to Bigquery
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket=BUCKET_NAME,
        source_objects=[SOURCE_OBJECT],
        destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
        source_format='CSV',
        allow_jagged_rows=False,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=True,
        field_delimiter=',',
        autodetect=True
    )
    

    # define dependencies
    (
        start_task
        >>load_csv_to_bq
        >>end_task
    )