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
countries = ['UK', 'India', 'Mexico']
# define the DAG
with DAG(
    dag_id='split_table_by_country',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'partition'],
) as dag:
    
    # Task to create view for each country-specific table with selected columns and filter
   for country in countries:
       create_countries_views = BigqueryInsertJobOperator(
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
