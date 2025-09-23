# Crypto-App

A full stack data engineering and analytics project for ingesting, transforming, and visualizing cryptocurrency market data using **Python,Next.js, Apache Airflow, Streamlit, and PostgreSQL**.

## 🚀 Features

- Fetches marjet data  from Yahoo Finance, CoinDesk, coin gecko
- Supports additional data sources like Coingecko
- ETL scripts for schema enforcement and loading data into PostgreSQL
- Automated pipelines using **Apache Airflow** for scheduled updates (hourly/daily)
- Interactive dashboards with **Streamlit** for exploring historical price and volume trends
- **Jupyter notebooks** for ad-hoc analysis and experimentation

## 🛠 Tech Stack

- **Python** → Data ingestion, ETL, analytics  
- **PostgreSQL** → Database for storing crypto market data  
- **Apache Airflow** → Workflow orchestration and scheduling  
- **Streamlit** → Dashboard for data visualization  
- **Jupyter Notebook** → Exploratory analysis and prototyping  

## 📂 Directory Structure

```
crypto-app/
├── dags/ # Airflow DAGs for pipelines
│ ├── yf_hourly_dag.py
│ ├── yf_historical_dag.py
├── yf_hourly.py # Hourly Yahoo Finance ingestion
├── yf_historical.py # Historical Yahoo Finance ingestion
├── coingecko_ingest.ipynb # Notebook for Coingecko ingestion
├── streamlit_app.py # Streamlit dashboard
├── requirements.txt # Python dependencies
├── .env # Environment variables
```
## ⚙️ Getting Started

### ✅ Prerequisites
- Python 3.9+  
- PostgreSQL  

### 🔧 Setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/Bensonn5151/crypto-app.git
   cd crypto-app
Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate
Install dependencies
pip install -r requirements.txt
Configure environment variables
Copy .env.example to .env
Update with PostgreSQL credentials
(Optional) Initialize Airflow
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow webserver &
airflow scheduler &
▶️ Usage
Run ETL scripts manually
python yf_hourly.py
python yf_historical.py
Trigger Airflow DAGs
Visit http://localhost:8080 to manage pipelines
Start Streamlit dashboard
streamlit run streamlit_app.py
📜 License
