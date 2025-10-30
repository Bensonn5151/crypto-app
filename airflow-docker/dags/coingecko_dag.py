from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

sys.path.append('/opt/airflow/crypto-app/src/elt')

def run_coingecko_pipeline():
    logging.info("Starting CoinGecko pipeline execution")
    try:
        from coingecko_ingest_postgres import main as coingecko_main
        coingecko_main()
        logging.info("CoinGecko pipeline completed successfully")
    except ImportError as e:
        logging.error(f"Failed to import coingecko_ingest_postgres: {e}")
        raise
    except Exception as e:
        logging.error(f"CoinGecko pipeline failed: {e}")
        raise

with DAG(
    dag_id='coingecko_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='Fetch CoinGecko cryptocurrency data',
    schedule='0 * * * *',  # Every hour
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=['coingecko', 'crypto'],
) as dag:
    run_coingecko_task = PythonOperator(
        task_id='run_coingecko_pipeline',
        python_callable=run_coingecko_pipeline,
    )

    run_coingecko_task