from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import os
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1
}

data_file = os.path.join(Variable.get("data_path", "/opt/airflow/data"), 'extracted_data.json')

def extract(**kwargs):
    file_path = os.path.join(Variable.get("data_path", "/opt/airflow/data"), 'source_data.csv')
    data = extract_data(file_path)
    data.to_json(data_file, orient='records')

def transform(**kwargs):
    data = pd.read_json(data_file)
    transformed_data = transform_data(data)
    transformed_data.to_json(data_file, orient='records')

def load(**kwargs):
    data = pd.read_json(data_file)
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    db_uri = postgres_hook.get_uri()
    load_data(data, db_uri, "transactions")

with DAG(
    dag_id='retail_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for retail analytics',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load
    )

    task_extract >> task_transform >> task_load
