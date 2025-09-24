import os
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# -------------------------------------------------------
# Symbols to track
# -------------------------------------------------------
def get_symbols():
    return {
        "BTC-USD": "BTC",
        "ETH-USD": "ETH",
        "SOL-USD": "SOL"
    }

# -------------------------------------------------------
# Fetch historical data from yfinance
# -------------------------------------------------------
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

# -------------------------------------------------------
# Enforce schema consistency
# -------------------------------------------------------
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

# -------------------------------------------------------
# Load database credentials
# -------------------------------------------------------
load_dotenv(dotenv_path="/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env")

def load_db_env():
    db_config = {
        "DB_USERNAME": os.getenv('DB_USERNAME','postgres'),
        "DB_PASSWORD": os.getenv('DB_PASSWORD','bens'),
        "DB_HOST": os.getenv('DB_HOST','postgres'),  # Default to 'postgres' for Docker
        "DB_PORT": os.getenv('DB_PORT','5432'),
        "DB_NAME": os.getenv('DB_NAME','crypto_app')
    }
    
    print("=== ACTUAL DATABASE CONFIGURATION ===")
    print(f"Username: {db_config['DB_USERNAME']}")
    print(f"Password: {'*' * len(db_config['DB_PASSWORD']) if db_config['DB_PASSWORD'] else 'None'}")
    print(f"Host: {db_config['DB_HOST']}")
    print(f"Port: {db_config['DB_PORT']}")
    print(f"Database: {db_config['DB_NAME']}")
    print("=====================================")
    
    return db_config

# -------------------------------------------------------
# Database utilities with psycopg2
# -------------------------------------------------------
def get_connection(db_params):
    return psycopg2.connect(
        host=db_params["DB_HOST"],
        port=db_params["DB_PORT"],
        dbname=db_params["DB_NAME"],
        user=db_params["DB_USERNAME"],
        password=db_params["DB_PASSWORD"]
    )

def test_db_connection(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            print("Connected to:", cur.fetchone()[0])
            return True
    except Exception as e:
        print("Connection failed:", e)
        return False

def create_table(conn):
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
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

def insert_data(conn, df):
    insert_sql = sql.SQL("""
        INSERT INTO yfinance_historical (Symbol, Date, Open, High, Low, Close, Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)

    # Ensure each column is the right Python type
    df = df.copy()
    df["Symbol"] = df["Symbol"].astype(str)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")   # safe datetime
    df["Open"] = df["Open"].astype(float)
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = df["Volume"].astype(float)   # float avoids numpy.int64 issue

    # Replace NaN with None (Postgres NULL)
    rows = df.where(pd.notnull(df), None).values.tolist()

    with conn.cursor() as cur:
        cur.executemany(insert_sql.as_string(conn), rows)  # batch insert
    conn.commit()


def list_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name 
                       FROM information_schema.tables 
                       WHERE table_schema='public';""")
        print("Tables:", [t[0] for t in cur.fetchall()])

# -------------------------------------------------------
# Main
# -------------------------------------------------------
def main():
    symbols = get_symbols()
    print("Fetching historical data...")
    historical_df = fetch_yfinance_data(symbols)

    print("Enforcing schema...")
    silver_historical_df = enforce_schema(historical_df)

    db_params = load_db_env()
    conn = get_connection(db_params)

    print("Testing DB connection...")
    if not test_db_connection(conn):
        return

    print("Creating table...")
    create_table(conn)

    print("Inserting data...")
    insert_data(conn, silver_historical_df)

    print("Available tables:")
    list_tables(conn)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
