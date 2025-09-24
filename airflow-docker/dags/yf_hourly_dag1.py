from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging
from dotenv import load_dotenv

# Load .env file inside the Airflow container
load_dotenv(dotenv_path="/opt/airflow/.env")

# Add your project path to sys.path (adjust to container path if mounted)
sys.path.append('/opt/airflow/app')

# If mounted in docker-compose, use:
# sys.path.append('/opt/airflow/crypto-app')

def run_yf_hourly_pipeline():
    """Execute the yf_hourly pipeline"""
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
    dag_id='yf_hourly_dag',
    default_args=default_args,
    description='Fetch hourly Yahoo Finance data',
    schedule_interval='0 * * * *',  # Cron: every hour at minute 0
    catchup=False,
    tags=['yfinance', 'crypto', 'hourly'],
)

# Create the task
run_yf_hourly_task = PythonOperator(
    task_id='run_yf_hourly_pipeline',
    python_callable=run_yf_hourly_pipeline,
    dag=dag,
)

# Define DAG structure
run_yf_hourly_task
