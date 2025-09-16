# Crypto-App

A data engineering and analytics project for ingesting, transforming, and visualizing cryptocurrency market data using Python, Airflow, Streamlit, and PostgreSQL.

## Features

- Fetches historical and hourly price data for BTC, ETH, and SOL from Yahoo Finance using `yfinance`.
- Supports additional sources like Coingecko.
- ETL scripts for schema enforcement and loading data into PostgreSQL.
- Automated pipelines using Apache Airflow for scheduled data updates (hourly/daily).
- Interactive dashboards built with Streamlit for exploring historical price and volume trends.
- Jupyter notebooks for ad-hoc analysis and data ingestion.

## Tech Stack

- **Python**: Data ingestion, ETL, and analytics
- **PostgreSQL**: Database for storing crypto price data
- **Apache Airflow**: Workflow orchestration and scheduling
- **Streamlit**: Data visualization dashboard
- **Jupyter Notebook**: Exploratory analysis and prototyping

## Directory Structure

```
crypto-app/
├── dags/                  # Airflow DAGs for scheduled pipelines
│   ├── yf_hourly_dag.py
│   ├── yf_historical_dag.py
├── yf_hourly.py           # Script for hourly Yahoo Finance data ingestion
├── yf_historical.py       # Script for historical Yahoo Finance data ingestion
├── coingecko_ingest.ipynb # Notebook for Coingecko data ingestion
├── streamlit_app.py       # Streamlit dashboard app
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables for DB credentials


## Getting Started

### Prerequisites
- Python 3.9+
- PostgreSQL

### Setup

1. Clone the repo:
    ```bash
    git clone <repo-url>
    cd crypto-app
    ```
2. Create and activate a Python virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4. Configure environment variables:
    - Copy `.env.example` to `.env` and update with your PostgreSQL credentials.

5. (Optional) Initialize Airflow for scheduled pipelines:
    ```bash
    export AIRFLOW_HOME=$(pwd)
    airflow db init
    airflow webserver &
    airflow scheduler &
    ```

## Usage

- **Run ETL scripts manually:**
    ```bash
    python yf_hourly.py
    python yf_historical.py
    ```
- **Trigger Airflow DAGs:**
    - Access Airflow UI at [http://localhost:8080](http://localhost:8080) and trigger DAGs as needed.
- **Start Streamlit dashboard:**
    ```bash
    streamlit run streamlit_app.py
    ```


