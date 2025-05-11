# from airflow import DAG
# from datetime import datetime
# from airflow.providers.google.cloud.operators import BigqueryInsertJobOperator
# from dotenv import load_dotenv
# import os

# load_dotenv()

# PROJECT_ID = os.getenv('PROJECT_ID')
# TRANSFORMATION_DATASET = os.getenv('TRANSFORMATION_DATASET')
# BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE')
# # Country to create seperate tables
# countries =['UK','India','Mexico','Canada','South Africa','Germany','France']

# default_args = {
#     'start_date': datetime(2023, 10, 1),
#     'retries': 1,
# }

# with DAG(
#     dag_id='split_table_by_country',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=['bigquery', 'partition'],
# ) as dag:
#     for country in countries:
#        BigqueryInsertJobOperator(
#         task_id=f"create_table.{country.lower()}",
#         configuration={
#             "query":{
#                 "query":f"""
#                           CREATE OR REPLACE TABLE `{PROJECT_ID}.{TRANSFORMATION_DATASET}.{country.lower()}_table` AS
#                           SELECT *
#                           FROM `{BIGQUERY_TABLE}`
#                           WHERE country='{country}'
#                          """,
#                 "useLegacySQL":False,
#             }
#         }
#     ).set_upstream()
