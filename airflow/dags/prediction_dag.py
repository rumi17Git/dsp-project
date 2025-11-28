from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import os
import pandas as pd
import numpy as np
import requests
import json

GOOD_DATA_PATH = "/opt/airflow/good-data"
API_URL = "http://dsp_api:8000/predict"

def check_for_new_data(**context):
    processed_flag = "/opt/airflow/processed_files.json"

    if os.path.exists(processed_flag):
        with open(processed_flag, "r") as f:
            processed_files = set(json.load(f))
    else:
        processed_files = set()

    all_files = {f for f in os.listdir(GOOD_DATA_PATH) if f.endswith(".csv")}
    new_files = list(all_files - processed_files)

    if not new_files:
        raise AirflowSkipException("No new data to predict.")

    context["ti"].xcom_push(key="new_files", value=new_files)
    return new_files


def make_predictions(**context):
    files = context["ti"].xcom_pull(task_ids="check_for_new_data", key="new_files")
    if not files:
        return

    for file in files:
        file_path = os.path.join(GOOD_DATA_PATH, file)
        df = pd.read_csv(file_path)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.fillna(0, inplace=True)

        records = json.loads(df.to_json(orient="records", default_handler=str))

        payload = {
            "source": "scheduled",
            "data": records
        }

        requests.post(API_URL, json=payload)

    processed_flag = "/opt/airflow/processed_files.json"
    if os.path.exists(processed_flag):
        with open(processed_flag, "r") as f:
            processed = set(json.load(f))
    else:
        processed = set()

    processed.update(files)
    with open(processed_flag, "w") as f:
        json.dump(list(processed), f)


with DAG(
    dag_id="prediction_dag",
    description="Scheduled predictions on ingested data",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
) as dag:

    check_for_new_data_task = PythonOperator(
        task_id="check_for_new_data",
        python_callable=check_for_new_data,
    )

    make_predictions_task = PythonOperator(
        task_id="make_predictions",
        python_callable=make_predictions,
    )

    check_for_new_data_task >> make_predictions_task
