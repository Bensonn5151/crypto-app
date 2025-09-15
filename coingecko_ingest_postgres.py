import requests
import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv

from sqlalchemy import create_engine, text

load_dotenv()

# --- Postgres details ---
DB_USERNAME = os.getenv('DB_USERNAME', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ben')


PG_URL = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Improved ingestion function ---
def get_crypto_data(vs_currency="usd", coin_list=["bitcoin", "ethereum", "solana"], save_raw=True, retries=3):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coin_list),
        "price_change_percentage": "1h,24h,7d,30d"
    }
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY")
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=20)
            response.raise_for_status()
            data = response.json()
            break
        except requests.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt == retries-1:
                raise

    if save_raw:
        base_dir = os.path.join(os.getcwd(), "data", "bronze", "coingecko")
        os.makedirs(base_dir, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        file_path = os.path.join(base_dir, f"markets_{timestamp}.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"✅ Raw JSON saved to {file_path}")

    # Silver layer: return as DataFrame
    df = pd.DataFrame(data)
    return df

def enforce_schema(df):
    schema = {
        "id": "string",
        "symbol": "string",
        "name": "string",
        "image": "string",
        "current_price": "float64",
        "market_cap": "Int64",
        "market_cap_rank": "Int64",
        "fully_diluted_valuation": "Int64",
        "total_volume": "Int64",
        "high_24h": "float64",
        "low_24h": "float64",
        "price_change_24h": "float64",
        "price_change_percentage_24h": "float64",
        "market_cap_change_24h": "Int64",
        "market_cap_change_percentage_24h": "float64",
        "circulating_supply": "float64",
        "total_supply": "float64",
        "max_supply": "float64",
        "ath": "float64",
        "ath_change_percentage": "float64",
        "ath_date": "string",
        "atl": "float64",
        "atl_change_percentage": "float64",
        "atl_date": "string",
        "roi": "string",
        "last_updated": "string"
    }
    for col, dtype in schema.items():
        if col in df.columns:
            if dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
    return df

# --- Create Postgres table ---
def create_postgres_table(engine, table_name="silver_coingecko"):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id TEXT,
        symbol TEXT,
        name TEXT,
        image TEXT,
        current_price DOUBLE PRECISION,
        market_cap BIGINT,
        market_cap_rank INTEGER,
        fully_diluted_valuation BIGINT,
        total_volume BIGINT,
        high_24h DOUBLE PRECISION,
        low_24h DOUBLE PRECISION,
        price_change_24h DOUBLE PRECISION,
        price_change_percentage_24h DOUBLE PRECISION,
        market_cap_change_24h BIGINT,
        market_cap_change_percentage_24h DOUBLE PRECISION,
        circulating_supply DOUBLE PRECISION,
        total_supply DOUBLE PRECISION,
        max_supply DOUBLE PRECISION,
        ath DOUBLE PRECISION,
        ath_change_percentage DOUBLE PRECISION,
        ath_date TIMESTAMP,
        atl DOUBLE PRECISION,
        atl_change_percentage DOUBLE PRECISION,
        atl_date TIMESTAMP,
        roi TEXT,
        last_updated TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        print(f"✅ Table '{table_name}' ensured in Postgres.")

def main():
    # 1. Ingest data
    df = get_crypto_data()
    df = enforce_schema(df)

    # 2. Convert date columns
    for date_col in ["ath_date", "atl_date", "last_updated"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # 3. Create table in Postgres
    engine = create_engine(PG_URL)
    create_postgres_table(engine)

    # 4. Write DataFrame to Postgres table
    df.to_sql("silver_coingecko", engine, if_exists="append", index=False)
    print("✅ Data ingested to Postgres table 'silver_coingecko'.")

if __name__ == "__main__":
    main()