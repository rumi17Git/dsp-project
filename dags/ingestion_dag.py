from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import random
import shutil

RAW_FOLDER = "raw-data"
GOOD_FOLDER = "good-data"

def read_data():
    files = os.listdir(RAW_FOLDER)
    if not files:
        print("No files in raw-data folder.")
        return None
    file_to_process = random.choice(files)
    print(f"Selected file: {file_to_process}")
    return os.path.join(RAW_FOLDER, file_to_process)

def save_file(ti):
    file_path = ti.xcom_pull(task_ids='read_data')
    if file_path is None:
        print("No file to move.")
        return
    dest_path = os.path.join(GOOD_FOLDER, os.path.basename(file_path))
    shutil.move(file_path, dest_path)
    print(f"Moved {file_path} to {dest_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 10),
}

with DAG(
    dag_id='ingestion_dag',
    default_args=default_args,
    schedule_interval='@minute',
    catchup=False
) as dag:

    read_task = PythonOperator(
        task_id='read_data',
        python_callable=read_data
    )

    save_task = PythonOperator(
        task_id='save_file',
        python_callable=save_file
    )

    read_task >> save_task
