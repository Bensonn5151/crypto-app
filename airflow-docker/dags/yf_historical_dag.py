from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging
import runpy

#sys.path.append('/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app')
sys.path.append('/opt/airflow/crypto-app')


def run_yf_historical_pipeline():
    logging.info("Starting yf_historical_pipeline execution")
    try:
        from yf_historical import main as yf_historical__main
        yf_historical__main()
        logging.info("yf_historical pipeline completed successfully (module import)")
    except ImportError as e:
        logging.error(f"Failed to import yf_historical via module: {e}. Falling back to runpy execution.")
        script_path = os.path.join('/opt/airflow/crypto-app', 'yf_historical.py')
        if not os.path.exists(script_path):
            logging.error(f"yf_historical.py not found at {script_path}")
            raise
        try:
            runpy.run_path(script_path, run_name="__main__")
            logging.info("yf_historical pipeline completed successfully (runpy fallback)")
        except Exception as ex:
            logging.error(f"yf_historical pipeline failed during runpy execution: {ex}")
            raise
    except Exception as e:
        logging.error(f"yf_historical pipeline failed: {e}")
        raise


with DAG(
    dag_id='yf_historical__dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='Fetch historical Yahoo Finance data',
    schedule='0 0 * * *',  # Runs every day at midnight
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=['yfinance', 'crypto', 'historical'],
) as dag:
    run_yf_historical_task = PythonOperator(
        task_id='run_yf_historical_pipeline',
        python_callable=run_yf_historical_pipeline,
    )
    run_yf_historical_task