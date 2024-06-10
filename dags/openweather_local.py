from datetime import datetime, timedelta
import os
import requests
import json
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
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

import os

def fetch_weather_data(**kwargs):
    cities = Variable.get("cities", deserialize_json=True)
    api_key = "36174e6314d900c4ea70a58dd2c85d4a"  # Replace with your actual API key

    parent_folder = "/app/raw_files"
    os.makedirs(parent_folder, exist_ok=True)  # Ensure the directory exists

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
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

# Define the function to transform data into CSV
import os
import json
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

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

# Define the function to transform data for the latest 20 files
def transform_latest_data(**kwargs):
    transform_data_into_csv(n_files=20, filename='data.csv')

# Define the function to transform data for all files
def transform_all_data(**kwargs):
    transform_data_into_csv(filename='fulldata.csv')

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score

def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def train_and_evaluate_model(model_name, model, **kwargs):
    X, y = prepare_data('/app/clean_data/fulldata.csv')
    model_score = compute_model_score(model, X, y)
    print(f"{model_name} Score: {model_score}")
    kwargs['ti'].xcom_push(key=model_name, value=model_score)

def train_and_save_model(model, X, y, path_to_model='./app/clean_data/best_model.pickle'):
    # training the model
    model.fit(X, y)
    # saving model
    model_folder = os.path.dirname(path_to_model)
    os.makedirs(model_folder, exist_ok=True)  # Ensure the directory exists
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)

def select_best_model(**kwargs):
    X, y = prepare_data('/app/clean_data/fulldata.csv')

    linear_regression_score = kwargs['ti'].xcom_pull(key='LinearRegression', task_ids='train_evaluate_model_group.train_linear_regression')
    decision_tree_score = kwargs['ti'].xcom_pull(key='DecisionTreeRegressor', task_ids='train_evaluate_model_group.train_decision_tree')
    random_forest_score = kwargs['ti'].xcom_pull(key='RandomForestRegressor', task_ids='train_evaluate_model_group.train_random_forest')

    best_score = min(linear_regression_score, decision_tree_score, random_forest_score)

    if best_score == linear_regression_score:
        model = LinearRegression()
    elif best_score == decision_tree_score:
        model = DecisionTreeRegressor()
    else:
        model = RandomForestRegressor()

    train_and_save_model(model, X, y, path_to_model='./app/clean_data/best_model.pickle')

with DAG('weather_data_pipeline', default_args=default_args, schedule_interval='*/1 * * * *') as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    with TaskGroup('transform_data_group') as transform_data_group:
        transform_latest_data_task = PythonOperator(
            task_id='transform_latest_data',
            python_callable=transform_latest_data,
        )

        transform_all_data_task = PythonOperator(
            task_id='transform_all_data',
            python_callable=transform_all_data,
        )

    with TaskGroup('train_evaluate_model_group') as train_evaluate_model_group:
        train_linear_regression_task = PythonOperator(
            task_id='train_linear_regression',
            python_callable=train_and_evaluate_model,
            op_kwargs={'model_name': 'LinearRegression', 'model': LinearRegression()}
        )

        train_decision_tree_task = PythonOperator(
            task_id='train_decision_tree',
            python_callable=train_and_evaluate_model,
            op_kwargs={'model_name': 'DecisionTreeRegressor', 'model': DecisionTreeRegressor()}
        )
        
        train__random_forest_task = PythonOperator(
            task_id='train_random_forest',
            python_callable=train_and_evaluate_model,
            op_kwargs={'model_name': 'RandomForestRegressor', 'model': RandomForestRegressor()}
        )
        
    select_best_model_task = PythonOperator(
        task_id='select_best_model',
        python_callable=select_best_model
    )

# Set dependencies
select_best_model_task << train_evaluate_model_group
train_evaluate_model_group << transform_data_group
transform_data_group << fetch_data_task