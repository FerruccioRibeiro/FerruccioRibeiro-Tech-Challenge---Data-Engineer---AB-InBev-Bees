import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# SRC Functions
from src.bronze_ingestion import fetch_raw_breweries
from src.silver_transformation import separate_in_location
from src.gold_analytics import count_breweries_per_type_location


# ENV Variables
API_URL = os.getenv("API_URL")
BASE_PATH = os.getenv('DATA_PATH', './data')


# DAGs config
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['ribeiroferruccio@gmail.com'],
    'email_on_failure': True,  
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


# DAG definition
with DAG(
    'etl_breweries_pipeline',
    default_args=default_args,
    description='Pipeline for extracting data from breweries and assembling a medallion style.',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Bronze layer
    task_bronze_extract = PythonOperator(
        task_id='extract_api',
        python_callable=fetch_raw_breweries,
        op_kwargs={'base_url':API_URL,'path': BASE_PATH}
    )

    #Silver layer
    task_silver_transformation = PythonOperator(
        task_id='transform_data',
        python_callable=separate_in_location,
        op_kwargs={'path': BASE_PATH}
    )

    #Gold layer
    task_gold_analytics = PythonOperator(
        task_id='analyte_gold',
        python_callable=count_breweries_per_type_location,
        op_kwargs={'path': BASE_PATH}
    )

    #Transformations tests
    task_validate_logic = BashOperator(
        task_id='validate_logic_with_pytest',
        bash_command='python3 -m pytest /opt/airflow/tests/'
    )

    #Flow
    task_bronze_extract >> task_validate_logic >> task_silver_transformation >> task_gold_analytics