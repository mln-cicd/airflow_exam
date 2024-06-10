from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from fetch_weather_data import fetch_weather_data
from transform_data import transform_latest_data, transform_all_data
from train_evaluate_model import train_and_evaluate_model, select_best_model

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