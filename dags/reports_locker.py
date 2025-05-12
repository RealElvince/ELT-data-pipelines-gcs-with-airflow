from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators import BigqueryInsertJobOperator
from dotenv import load_dotenv
import os

# load environment variable
load_dotenv()


# configuration variables
PROJECT_ID = os.getenv('PROJECT_ID')
TRANSFORMATION_DATASET = os.getenv('TRANSFORMATION_DATASET')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
REPORTS_DATASET = os.getenv('REPORTS_DATASET')

# define de

default_args = {
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# define the DAG
with DAG(
    dag_id='split_table_by_country',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'partition'],
) as dag: