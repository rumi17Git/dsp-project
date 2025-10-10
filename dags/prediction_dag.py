from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
import pandas as pd

GOOD_FOLDER = "good-data"
API_URL = "http://127.0.0.1:8000/predict"  # FastAPI URL

def check_for_new_data(ti):
    files = os.listdir(GOOD_FOLDER)
    if not files:
        print("No new files.")
        return None
    ti.xcom_push(key='files', value=files)
    return files

def make_predictions(ti):
    files = ti.xcom_pull(key='files', task_ids='check_for_new_data')
    if not files:
        print("No files to predict.")
        return

    for file_name in files:
        file_path = os.path.join(GOOD_FOLDER, file_name)
        df = pd.read_csv(file_path)
        payload = df.to_dict(orient='records')
        response = requests.post(API_URL, json=payload)
        print(f"Predictions for {file_name}: {response.json()}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 10),
}

with DAG(
    dag_id='prediction_dag',
    default_args=default_args,
    schedule_interval='*/2 * * * *',  # every 2 minutes
    catchup=False
) as dag:

    check_task = PythonOperator(
        task_id='check_for_new_data',
        python_callable=check_for_new_data
    )

    predict_task = PythonOperator(
        task_id='make_predictions',
        python_callable=make_predictions
    )

    check_task >> predict_task
