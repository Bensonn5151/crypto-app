from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
import logging

sys.path.append('/opt/airflow/crypto-app')

def fetch_coindesk_data():
    """Fetch data from CoinDesk API"""
    API_KEY = os.getenv('COINDESK_API_KEY', 'YOUR_API_KEY_HERE')
    url = "https://data-api.coindesk.com/index/cc/v1/latest/tick"
    
    params = {
        "market": "ccix",                                                        
        "instruments": "BTC-USD,ETH-USD,SOL-USD,ADA-USD",
        "api_key": API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        records = []

        if "Data" in data:
            for instrument, values in data["Data"].items():
                record = {
                    "symbol": instrument.split("-")[0].upper(),
                    "current_price": values.get("VALUE"),
                    "hour_high": values.get("CURRENT_HOUR_HIGH"),
                    "hour_low": values.get("CURRENT_HOUR_LOW"),
                    "day_high": values.get("CURRENT_DAY_HIGH"),
                    "day_low": values.get("CURRENT_DAY_LOW"),
                    "volume": values.get("CURRENT_DAY_VOLUME"),
                    "price_change": values.get("VALUE_CHANGE"),
                    "price_change_percent": values.get("VALUE_CHANGE_PERCENT"),
                    "last_update_ts": values.get("VALUE_LAST_UPDATE_TS", 0),
                    "last_update": pd.to_datetime(values.get("VALUE_LAST_UPDATE_TS", 0), unit='s'),
                    "timestamp": datetime.utcnow()
                }
                records.append(record)
        
        return pd.DataFrame(records)
    except Exception as e:
        logging.error(f"Error fetching CoinDesk data: {e}")
        return pd.DataFrame()

def save_coindesk_data():
    """Save CoinDesk data to database"""
    logging.info("Starting CoinDesk data pipeline...")
    
    # Fetch data
    df = fetch_coindesk_data()
    
    if df.empty:
        logging.warning("No data fetched from CoinDesk API")
        return
    
    logging.info(f"Fetched {len(df)} records from CoinDesk API")
    
    # Save to database
    try:
        from yf_hourly import load_db_env, get_engine  # Reuse your existing DB functions
        
        db_params = load_db_env()
        engine = get_engine(db_params)
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS coindesk_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            current_price DECIMAL(20, 8),
            hour_high DECIMAL(20, 8),
            hour_low DECIMAL(20, 8),
            day_high DECIMAL(20, 8),
            day_low DECIMAL(20, 8),
            volume DECIMAL(30, 8),
            price_change DECIMAL(20, 8),
            price_change_percent DECIMAL(10, 6),
            last_update_ts BIGINT,
            last_update TIMESTAMP WITH TIME ZONE,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, last_update_ts)
        );
        
        CREATE INDEX IF NOT EXISTS idx_coindesk_symbol_time 
        ON coindesk_prices (symbol, timestamp);
        """
        
        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
        
        # Insert data (ignore duplicates)
        df.to_sql(
            'coindesk_prices', 
            engine, 
            if_exists='replace', 
            index=False,
            method='multi'
        )
        
        logging.info(f"✅ Successfully saved {len(df)} records to coindesk_prices table")
        
    except Exception as e:
        logging.error(f"Error saving data to database: {e}")
        raise

def run_coindesk_pipeline():
    """Main pipeline function"""
    logging.info("Starting CoinDesk pipeline execution")
    try:
        save_coindesk_data()
        logging.info("CoinDesk pipeline completed successfully")
    except Exception as e:
        logging.error(f"CoinDesk pipeline failed: {e}")
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='coindesk_data_pipeline',
    default_args=default_args,
    description='Collect real-time crypto data from CoinDesk API for Streamlit dashboard',
    schedule= '*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['coindesk', 'crypto', 'streamlit', 'realtime'],
) as dag:
    
    coindesk_task = PythonOperator(
        task_id='fetch_and_store_coindesk_data',
        python_callable=run_coindesk_pipeline,
        execution_timeout=timedelta(minutes=10),
    )

    coindesk_task