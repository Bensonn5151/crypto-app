import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

def get_symbols():
    return {
        "BTC-USD": "BTC",
        "ETH-USD": "ETH",
        "SOL-USD": "SOL"
    }

def fetch_yfinance_data(symbols):
    all_data = []
    for yf_symbol, short_symbol in symbols.items():
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period="max")
        hist = hist.reset_index()
        hist["Symbol"] = short_symbol
        all_data.append(hist)
    historical_df = pd.concat(all_data, ignore_index=True)
    historical_df = historical_df[["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]]
    return historical_df

def enforce_schema(df):
    schema = {
        "Symbol": "string",
        "Date": "datetime64[ns]",
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

load_dotenv(dotenv_path="/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env")

def load_db_env():
    load_dotenv()
    db_params = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USERNAME": os.getenv("DB_USERNAME"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
    }
    return db_params

def get_engine(db_params):
    url = f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}@{db_params["DB_HOST"]}:{db_params["DB_PORT"]}/{db_params["DB_NAME"]}'
    return create_engine(url)

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
        DROP TABLE IF EXISTS yfinance_historical;
        CREATE TABLE yfinance_historical (
            Symbol VARCHAR(10),
            Date TIMESTAMP,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume BIGINT
        );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

def insert_data(engine, df):
    columns_to_keep = ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]
    df_subset = df[columns_to_keep].copy()
    df_subset.columns = [col.lower() for col in df_subset.columns]
    df_subset.to_sql(
        "yfinance_historical",
        con=engine,
        if_exists="append",
        index=False
    )

def list_tables(engine):
    inspector = inspect(engine)
    print(inspector.get_table_names())

def main():
    symbols = get_symbols()
    print("Fetching historical data...")
    historical_df = fetch_yfinance_data(symbols)
    print("Enforcing schema...")
    silver_historical_df = enforce_schema(historical_df)
    db_params = load_db_env()
    engine = get_engine(db_params)
    print("Testing DB connection...")
    if not test_db_connection(engine):
        return
    print("Creating table...")
    create_table(engine)
    print("Inserting data...")
    insert_data(engine, silver_historical_df)
    print("Available tables:")
    list_tables(engine)
    print("Done.")

if __name__ == "__main__":
    main()