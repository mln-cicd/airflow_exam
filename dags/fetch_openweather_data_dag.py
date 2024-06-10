from datetime import datetime, timedelta
import os
import requests
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather_data(**kwargs):
    cities = Variable.get("cities", deserialize_json=True)
    api_key = "36174e6314d900c4ea70a58dd2c85d4a"  # Replace with your actual API key

    parent_folder = "/app/raw_files"
    os.makedirs(parent_folder, exist_ok=True)  # Ensure the directory exists

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{timestamp}.json"
    file_path = os.path.join(parent_folder, filename)

    data = []
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url)
        if response.status_code == 200:
            data.append(response.json())
        else:
            print(f"Error fetching data for {city}: {response.status_code}")

    with open(file_path, "w") as f:
        json.dump(data, f)
    
    print(f"Data fetched and saved to {file_path}")

# Define the DAG
with DAG('fetch_weather_data_dag', default_args=default_args, schedule_interval='*/1 * * * *') as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )