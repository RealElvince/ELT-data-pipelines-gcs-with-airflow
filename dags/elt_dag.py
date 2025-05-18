from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'depend_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Environment variable configurations
PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
SOURCE_OBJECT = os.getenv('SOURCE_OBJECT')
DESTINATION_PROJECT_DATASET_TABLE = os.getenv('DESTINATION_PROJECT_DATASET_TABLE')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
TRANSFORMATION_DATASET = os.getenv('TRANSFORMATION_DATASET')
REPORTING_DATASET = os.getenv('REPORTING_DATASET')
HEALTH_DATASET = os.getenv('HEALTH_DATASET')

# List of countries to generate specific tables and views for
countries = ['UK', 'India', 'Mexico','Canada','South Africa','Germany','France','Japan','Brazil','Italy','Spain','Australia','Netherlands','Russia','Turkey','Sweden','Norway','Finland','Denmark']

# Validate required environment variables
required_vars = [BUCKET_NAME, SOURCE_OBJECT, DESTINATION_PROJECT_DATASET_TABLE, BIGQUERY_TABLE]
if not all(required_vars):
    raise ValueError("Required environment variables (BUCKET_NAME, SOURCE_OBJECT, BIGQUERY_TABLE, or DESTINATION_PROJECT_DATASET_TABLE) are missing.")

# Define the DAG
with DAG(
    dag_id='ELT_data_pipelines',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2023, 10, 1),
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    # Start and end markers
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    # Check if file exists in GCS
    check_if_file_exists = GCSObjectExistenceSensor(
        task_id='sensor_to_check_if_csv_file_exists',
        bucket=BUCKET_NAME,
        object=SOURCE_OBJECT,
        poke_interval=30,
        timeout=300,
        mode='poke',
    )

    # Load CSV from GCS to BigQuery
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

   

    # Create table and view tasks for each country
    split_tasks = []
    view_tasks = []
    for country in countries:
        # Table creation task
        split_task = BigQueryInsertJobOperator(
            task_id=f"create_table_{country.lower()}",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{TRANSFORMATION_DATASET}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{PROJECT_ID}.{HEALTH_DATASET}.{BIGQUERY_TABLE}`
                        WHERE country='{country}'
                    """,
                    "useLegacySql": False,
                }
            },
        )


        # View creation task
        view_task = BigQueryInsertJobOperator(
            task_id=f"create_{country.lower()}_view",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{PROJECT_ID}.{REPORTING_DATASET}.{country.lower()}_view` AS
                        SELECT
                            `Year` AS `year`, 
                            `Disease Name` AS `disease_name`, 
                            `Disease Category` AS `disease_category`, 
                            `Prevalence Rate` AS `prevalence_rate`, 
                            `Incidence Rate` AS `incidence_rate`
                        FROM `{PROJECT_ID}.{TRANSFORMATION_DATASET}.{country.lower()}_table`
                        WHERE `Availability of Vaccines Treatment` = False
                    """,
                    "useLegacySql": False,
                }
            },
        )

    
       
        
        #append tasks to the list
        split_tasks.append(split_task)
        view_tasks.append(view_task)

    # Task dependencies
     # Link initial tasks
    start_task >> check_if_file_exists >> load_csv_to_bq
    
    for split_task, view_task in zip(split_tasks, view_tasks):
        load_csv_to_bq >> split_task >> view_task >> end_task
        

    