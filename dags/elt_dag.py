from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
SOURCE_OBJECT = os.getenv('SOURCE_OBJECT')
DESTINATION_PROJECT_DATASET_TABLE = os.getenv('DESTINATION_PROJECT_DATASET_TABLE')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
TRANSFORMATION_DATASET= os.getenv('TRANSFORMATION_DATASET')
REPORTS_DATASET = os.getenv('REPORTS_DATASET')
HEALTH_DATASET = os.getenv('HEALTH_DATASET')

# Country to create seperate tables
countries =['UK','India','Mexico']

# Validate required environment variables
if not BUCKET_NAME or not SOURCE_OBJECT or not BIGQUERY_TABLE or not DESTINATION_PROJECT_DATASET_TABLE:
    raise ValueError("Required environment variables (BUCKET_NAME, SOURCE_OBJECT, or DESTINATION_PROJECT_DATASET_TABLE) are missing.")

# define the DAG
with DAG(
    dag_id='ELT_data_pipelines',
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

    # Task 1: check if the health global data exists in the bucket
    check_if_file_exists = GCSObjectExistenceSensor(
        task_id='sensor_to_check_if_csv_file_exists',
        bucket=BUCKET_NAME,
        object=SOURCE_OBJECT,
        poke_interval=30,
        timeout=300,
        mode='poke',
    )
    # Task 2: define the task to load CSV from GCS to Bigquery
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

    # Task 3: split global health data by country
    split_country_data_tasks = []
    for country in countries:
       split_task = BigQueryInsertJobOperator(
        task_id=f"create_table_{country.lower()}",
        configuration={
            "query":{
                "query":f"""
                          CREATE OR REPLACE TABLE `{PROJECT_ID}.{TRANSFORMATION_DATASET}.{country.lower()}_table` AS
                          SELECT *
                          FROM `{PROJECT_ID}.{HEALTH_DATASET}.{BIGQUERY_TABLE}`
                          WHERE country='{country}'
                         """,
                "useLegacySql":False,
            }
        },
    )
    split_country_data_tasks.append(split_task)

    #   Task to create view for each country-specific table with selected columns and filter
    create_countries_views_tasks= []
    for country in countries:
       view_tasks= BigQueryInsertJobOperator(
        task_id=f"create_{country.lower()}_view",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE VIEW `{PROJECT_ID}.{TRANSFORMATION_DATASET}.{country.lower()}_view` AS
                    SELECT
                      `Year` AS `year`, 
                            `Disease Name` AS `disease_name`, 
                            `Disease Category` AS `disease_category`, 
                            `Prevalence Rate` AS `prevalence_rate`, 
                            `Incidence Rate` AS `incidence_rate`
                    FROM
                      `{PROJECT_ID}.{TRANSFORMATION_DATASET}.{country.lower()}_table`
                    WHERE `Availability of Vaccines Treatment` = False
                """,
                "useLegacySql": False,
            }
        }
    )
    create_countries_views_tasks.append(view_tasks)
    

    # define dependencies
    start_task >> check_if_file_exists >> load_csv_to_bq

    for split_task, view_task in zip(split_country_data_tasks, create_countries_views_tasks):
       load_csv_to_bq >> split_task >> view_task >> end_task

  