from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def train_model():
    print("train started")


default_args = {
    "owner": "data_engineer",
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_retraining_v1",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    train_task = PythonOperator(
        task_id="train_user_behavior_model",
        python_callable=train_model,
    )