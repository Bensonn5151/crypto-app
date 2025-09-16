from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

def print_hello():
    logging.info("Hello from Airflow!")

with DAG(
    dag_id='z',
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
    },
    description='A simple test DAG',
    schedule=None,
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=['test'],
) as dag:
    hello_task = PythonOperator(
        task_id='z',
        python_callable=print_hello,
    )

    hello_task
