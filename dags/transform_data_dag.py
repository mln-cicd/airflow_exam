from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import json
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    os.makedirs(parent_folder, exist_ok=True)  # Ensure the directory exists

    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    clean_data_folder = '/app/clean_data'
    os.makedirs(clean_data_folder, exist_ok=True)  # Ensure the directory exists

    df.to_csv(os.path.join(clean_data_folder, filename), index=False)

def transform_latest_data(**kwargs):
    transform_data_into_csv(n_files=20, filename='data.csv')

def transform_all_data(**kwargs):
    transform_data_into_csv(filename='fulldata.csv')

with DAG('transform_data_dag', default_args=default_args, schedule_interval='*/1 * * * *') as dag:
    transform_latest_data_task = PythonOperator(
        task_id='transform_latest_data',
        python_callable=transform_latest_data,
    )

    transform_all_data_task = PythonOperator(
        task_id='transform_all_data',
        python_callable=transform_all_data,
    )
    