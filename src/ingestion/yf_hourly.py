import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import psycopg2
from psycopg2.extras import execute_values

from src.config.coins import CoinRegistry

# -------------------------
# 1. Symbols
# -------------------------
def get_symbols():
    return CoinRegistry.get_yf_map()

# -------------------------
# 2. Fetch Data
# -------------------------
def fetch_hourly_data(symbols):
    all_data = []
    for yf_symbol, short_symbol in symbols.items():
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period="7d", interval="1h")
        hist = hist.reset_index()
        hist["Symbol"] = short_symbol
        all_data.append(hist)

    bronze_df = pd.concat(all_data, ignore_index=True)
    bronze_df = bronze_df[["Symbol", "Datetime", "Open", "High", "Low", "Close", "Volume"]]
    return bronze_df

# -------------------------
# 3. Enforce Schema
# -------------------------
def enforce_schema(df):
    schema = {
        "Symbol": "string",
        "Datetime": "datetime64[ns]",
        "Open": "float64",
        "High": "float64",
        "Low": "float64",
        "Close": "float64",
        "Volume": "Int64",
    }
    for col, dtype in schema.items():
        if col in df.columns:
            if dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
    return df

# -------------------------
# 4. DB Config
# -------------------------
#load_dotenv(dotenv_path="/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env")

load_dotenv(dotenv_path="/opt/airflow/.env")


# def load_db_env():
#     return {
#         "DB_USERNAME": os.getenv('DB_USERNAME'),
#         "DB_PASSWORD": os.getenv('DB_PASSWORD'),
#         "DB_HOST": os.getenv('DB_HOST'),
#         "DB_PORT": os.getenv('DB_PORT'),
#         "DB_NAME": os.getenv('DB_NAME')
#     }

def load_db_env():
    """Load database parameters depending on environment."""
    # Try .env.local first (for host)
    local_env = "/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env.local"
    docker_env = "/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env"

    if os.path.exists(local_env):
        load_dotenv(dotenv_path=local_env)
    else:
        load_dotenv(dotenv_path=docker_env)

    return {
        "DB_USERNAME": os.getenv('DB_USERNAME', 'postgres'),
        "DB_PASSWORD": os.getenv('DB_PASSWORD', 'bens'),
        "DB_HOST": os.getenv('DB_HOST', 'localhost'),
        "DB_PORT": os.getenv('DB_PORT', '5434'),
        "DB_NAME": os.getenv('DB_NAME', 'crypto_app')
    }


# def get_engine(db_params):
#     url = f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}@{db_params["DB_HOST"]}:{db_params["DB_PORT"]}/{db_params["DB_NAME"]}'
#     return create_engine(url)

def get_engine(db_params):
    try:
        # Validate port is integer
        port = int(db_params["DB_PORT"])
        
        url = f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}@{db_params["DB_HOST"]}:{port}/{db_params["DB_NAME"]}'
        print(f"Database URL: postgresql://{db_params['DB_USERNAME']}:****@{db_params['DB_HOST']}:{port}/{db_params['DB_NAME']}")
        
        engine = create_engine(url)
        return engine
    except ValueError as e:
        raise ValueError(f"Invalid database port: {db_params['DB_PORT']}") from e
    except Exception as e:
        raise Exception(f"Failed to create database engine: {e}") from e

# -------------------------
# 5. DB Functions
# -------------------------
def test_db_connection(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("Connected to:", result.scalar())
            return True
    except Exception as e:
        print("Connection failed:", e)
        return False

def create_table(engine):
    create_sql = """
        -- DROP TABLE IF EXISTS yfinance_hourly;
        CREATE TABLE IF NOT EXISTS yfinance_hourly (
            Symbol VARCHAR(10),
            Datetime TIMESTAMP,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume BIGINT
        );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

def insert_data_psycopg2(db_params, df):
    columns_to_keep = ["Symbol", "Datetime", "Open", "High", "Low", "Close", "Volume"]
    df_subset = df[columns_to_keep].copy()

    # Convert NaNs to None for psycopg2
    df_subset = df_subset.where(pd.notnull(df_subset), None)

    records = [tuple(x) for x in df_subset.to_numpy()]

    insert_sql = """
        INSERT INTO yfinance_hourly (symbol, datetime, open, high, low, close, volume)
        VALUES %s
    """

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_params["DB_NAME"],
            user=db_params["DB_USERNAME"],
            password=db_params["DB_PASSWORD"],
            host=db_params["DB_HOST"],
            port=db_params["DB_PORT"]
        )
        cur = conn.cursor()
        execute_values(cur, insert_sql, records)
        conn.commit()
        cur.close()
        print(f"Inserted {len(records)} rows into yfinance_hourly ✅")
    except Exception as e:
        print("Insert failed:", e)
    finally:
        if conn:
            conn.close()

def list_tables(engine):
    inspector = inspect(engine)
    print("Tables in DB:", inspector.get_table_names())

# -------------------------
# 6. Main Pipeline
# -------------------------
def main():
    symbols = get_symbols()
    print("Fetching hourly data...")
    hourly_df = fetch_hourly_data(symbols)

    print("Enforcing schema...")
    silver_df = enforce_schema(hourly_df)

    db_params = load_db_env()
    engine = get_engine(db_params)

    print("Testing DB connection...")
    if not test_db_connection(engine):
        return

    print("Creating table...")
    create_table(engine)

    print("Inserting data...")
    insert_data_psycopg2(db_params, silver_df)

    print("Available tables:")
    list_tables(engine)

    print("✅ Done.")

if __name__ == "__main__":
    main()