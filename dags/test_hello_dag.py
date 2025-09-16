from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

<<<<<<< HEAD
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
=======
sys.path.append('/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app')

def run_yf_hourly_pipeline():
    logging.info("Starting yf_hourly pipeline execution")
    try:
        from yf_hourly import main as yf_hourly_main
        yf_hourly_main()
        logging.info("yf_hourly pipeline completed successfully")
    except ImportError as e:
        logging.error(f"Failed to import yf_hourly: {e}")
        raise
    except Exception as e:
        logging.error(f"yf_hourly pipeline failed: {e}")
        raise

with DAG(
    dag_id='zyf_hourly_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='Fetch hourly Yahoo Finance data',
    schedule_interval='@hourly',
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=['yfinance', 'crypto', 'hourly'],
) as dag:
    run_yf_hourly_task = PythonOperator(
        task_id='run_yf_hourly_pipeline',
        python_callable=run_yf_hourly_pipeline,
    )

    run_yf_hourly_task
>>>>>>> bdbd84168ca4d4cbed8b7df6b06f5054036eeb01
