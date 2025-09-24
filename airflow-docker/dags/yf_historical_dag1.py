from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Add your project path to sys.path
sys.path.append('/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app')

def run_yf_historical_pipeline():
    """Execute the yf_hourly pipeline"""
    logging.info("Starting yf_hourly pipeline execution")
    try:
        from yf_hourly import main as yf_historical_main
        yf_historical_main()
        logging.info("yf_hourly pipeline completed successfully")
    except ImportError as e:
        logging.error(f"Failed to import yf_hourly: {e}")
        raise
    except Exception as e:
        logging.error(f"yf_hourly pipeline failed: {e}")
        raise

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    dag_id='yf_historical_dag',
    default_args=default_args,
    description='Fetch hourly Yahoo Finance data',
    schedule_interval='0 0 * * *',  # Runs every day at midnight
    catchup=False,
    tags=['yfinance', 'crypto', 'hourly'],
)

# Create the task
run_yf_hourly_task = PythonOperator(
    task_id='run_yf_hourly_pipeline',
    python_callable=run_yf_historical_pipeline,
    dag=dag,
)

# Task dependencies (if you add more tasks later)
run_yf_hourly_task