import os
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# -------------------------------------------------------
# Symbols to track
# -------------------------------------------------------
def get_symbols():
    return {
        "BTC-USD": "BTC",
        "ETH-USD": "ETH", 
        "SOL-USD": "SOL",
    }

# -------------------------------------------------------
# Fetch DAILY historical data from yfinance
# -------------------------------------------------------
def fetch_daily_historical_data(symbols):
    all_data = []
    
    # Define date range - from 2024 to today
    start_date = "2024-01-01"
    end_date = datetime.now()
    
    print(f"📅 Fetching data from {start_date} to {end_date.strftime('%Y-%m-%d')}")
    
    for yf_symbol, short_symbol in symbols.items():
        print(f"📈 Fetching DAILY historical data for {yf_symbol}...")
        
        try:
            ticker = yf.Ticker(yf_symbol)
            
            # Fetch daily historical data from 2024 to today
            hist = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if not hist.empty:
                print(f"   ✅ SUCCESS: {hist.shape[0]} DAILY records")
                
                # Reset index to get Date as a column
                hist = hist.reset_index()
                hist["symbol"] = short_symbol
                
                # Select only the columns we need for daily data
                hist = hist[["symbol", "Date", "Open", "High", "Low", "Close", "Volume"]]
                all_data.append(hist)
                
                # Show date range for this symbol
                actual_start = hist['Date'].min().strftime('%Y-%m-%d')
                actual_end = hist['Date'].max().strftime('%Y-%m-%d')
                print(f"   📅 Date range: {actual_start} to {actual_end}")
                print(f"   📊 Sample data shape: {hist.shape}")
                
            else:
                print(f"   ❌ No daily data found for {yf_symbol}")
                
        except Exception as e:
            print(f"   ❌ Error fetching {yf_symbol}: {e}")
    
    if all_data:
        # Combine all symbol data
        historical_df = pd.concat(all_data, ignore_index=True)
        print(f"✅ Combined DAILY historical data: {historical_df.shape[0]} total records")
        print(f"✅ Date range overall: {historical_df['Date'].min()} to {historical_df['Date'].max()}")
        return historical_df
    else:
        print("❌ No daily historical data was fetched from any symbol!")
        return pd.DataFrame()

# -------------------------------------------------------
# Enforce schema consistency for daily data
# -------------------------------------------------------
def enforce_daily_schema(df):
    if df.empty:
        return df
        
    schema = {
        "symbol": "string",
        "Date": "datetime64[ns]",
        "Open": "float64",
        "High": "float64",
        "Low": "float64",
        "Close": "float64",
        "Volume": "float64",
    }
    
    for col, dtype in schema.items():
        if col in df.columns:
            try:
                if dtype.startswith("datetime"):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(dtype, errors="ignore")
            except Exception as e:
                print(f"   ⚠️  Failed to convert {col}: {e}")
    
    return df

# -------------------------------------------------------
# Load database credentials
# -------------------------------------------------------
load_dotenv(dotenv_path="/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env")

def load_db_env():
    db_config = {
        "DB_USERNAME": os.getenv('DB_USERNAME','postgres'),
        "DB_PASSWORD": os.getenv('DB_PASSWORD','bens'),
        "DB_HOST": os.getenv('DB_HOST','postgres'),
        "DB_PORT": os.getenv('DB_PORT','5432'), #5434 if local
        "DB_NAME": os.getenv('DB_NAME','crypto_app')
    }
    
    print("=== DATABASE CONFIGURATION ===")
    print(f"Host: {db_config['DB_HOST']}:{db_config['DB_PORT']}")
    print(f"Database: {db_config['DB_NAME']}")
    print(f"Username: {db_config['DB_USERNAME']}")
    print("==============================")
    
    return db_config

# -------------------------------------------------------
# Database utilities with SQLAlchemy
# -------------------------------------------------------
def get_engine(db_params):
    """Create SQLAlchemy engine."""
    url = (
        f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}'
        f'@{db_params["DB_HOST"]}:{db_params["DB_PORT"]}/{db_params["DB_NAME"]}'
    )
    print(f"🔗 Database URL: postgresql://{db_params['DB_USERNAME']}:****@{db_params['DB_HOST']}:{db_params['DB_PORT']}/{db_params['DB_NAME']}")
    return create_engine(url)

def test_db_connection(engine):
    """Test database connection."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.scalar()
            print(f"✅ Connected to: {version.split(',')[0]}")
            return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def create_historical_table(engine):
    """Create table for daily historical data."""
    create_sql = """
    -- DROP TABLE IF EXISTS yfinance_historical;
    CREATE TABLE IF NOT EXISTS yfinance_historical (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_yfinance_historical_symbol_date ON yfinance_historical(symbol, date);
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    print("✅ Table yfinance_historical created successfully")

def insert_historical_data(engine, df):
    """Insert daily historical data into database using SQLAlchemy."""
    if df.empty:
        print("⚠️  No data to insert")
        return
    
    # Prepare data for insertion
    df = df.copy()
    
    # Rename columns to match database schema
    df = df.rename(columns={
        "Date": "date",
        "Open": "open", 
        "High": "high",
        "Low": "low",
        "Close": "close", 
        "Volume": "volume"
    })
    
    # Ensure proper data types
    df["symbol"] = df["symbol"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date  # Store as date only
    
    # Convert numeric columns
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Replace NaN with None for proper NULL handling
    df = df.where(pd.notnull(df), None)
    
    try:
        # OPTION 1: Use pandas to_sql without method='multi' (simpler)
        print("📤 Inserting data using pandas to_sql...")
        df.to_sql(
            name='yfinance_historical',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000  # Process in chunks for better memory usage
        )
        print(f"✅ Successfully inserted {len(df)} DAILY records into yfinance_historical")
        
    except Exception as e:
        print(f"❌ Data insertion failed: {e}")
        
        # OPTION 2: Fallback - manual insertion with conflict handling
        print("🔄 Trying alternative insertion method...")
        try:
            insert_historical_data_manual(engine, df)
        except Exception as e2:
            print(f"❌ Alternative insertion also failed: {e2}")
            raise

def insert_historical_data_manual(engine, df):
    """Alternative manual insertion method with conflict handling."""
    insert_sql = text("""
        INSERT INTO yfinance_historical (symbol, date, open, high, low, close, volume)
        VALUES (:symbol, :date, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high, 
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """)
    
    # Convert DataFrame to list of dictionaries
    records = df.to_dict('records')
    
    with engine.begin() as conn:
        # Process in batches to avoid memory issues
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                conn.execute(insert_sql, batch)
                total_inserted += len(batch)
                print(f"   ✅ Processed batch {i//batch_size + 1}: {len(batch)} records")
            except Exception as e:
                print(f"   ❌ Error in batch {i//batch_size + 1}: {e}")
                # Continue with next batch
                continue
        
        print(f"✅ Manually inserted {total_inserted} records with conflict handling")

def verify_data_insertion(engine):
    """Verify that data was inserted correctly."""
    try:
        with engine.connect() as conn:
            # Count total records
            result = conn.execute(text("SELECT COUNT(*) FROM yfinance_historical"))
            total_records = result.scalar()
            print(f"📊 Total records in database: {total_records}")
            
            # Count records by symbol
            result = conn.execute(text("""
                SELECT symbol, COUNT(*) as record_count 
                FROM yfinance_historical 
                GROUP BY symbol 
                ORDER BY symbol
            """))
            symbol_counts = result.fetchall()
            print("📈 Records by symbol:")
            for symbol, count in symbol_counts:
                print(f"   {symbol}: {count} records")
                
            # Date range in database
            result = conn.execute(text("""
                SELECT MIN(date), MAX(date) FROM yfinance_historical
            """))
            min_date, max_date = result.fetchone()
            print(f"📅 Date range in DB: {min_date} to {max_date}")
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")

# -------------------------------------------------------
# Main
# -------------------------------------------------------
def main():
    print("🚀 Starting Yahoo Finance DAILY Historical Data Pipeline")
    print("=" * 50)
    
    # Step 1: Fetch daily historical data
    symbols = get_symbols()
    print(f"📊 Tracking symbols: {list(symbols.keys())}")
    
    historical_df = fetch_daily_historical_data(symbols)
    
    if historical_df.empty:
        print("❌ Pipeline stopped: No daily historical data fetched")
        return
    
    # Step 2: Enforce schema
    print("\n🔧 Enforcing schema...")
    silver_historical_df = enforce_daily_schema(historical_df)
    
    # Show data summary
    print(f"\n📦 Data Summary:")
    print(f"   Total rows: {len(silver_historical_df)}")
    print(f"   Date range: {silver_historical_df['Date'].min()} to {silver_historical_df['Date'].max()}")
    print(f"   Symbols: {sorted(silver_historical_df['symbol'].unique())}")
    
    # Step 3: Database operations
    print("\n🗄️  Database Operations:")
    db_params = load_db_env()
    
    engine = get_engine(db_params)
    
    if not test_db_connection(engine):
        return
    
    try:
        # Create table
        create_historical_table(engine)
        
        # Insert data
        insert_historical_data(engine, silver_historical_df)
        
        # Verify insertion
        print("\n🔍 Verifying data insertion...")
        verify_data_insertion(engine)
        
        print("\n✅ DAILY HISTORICAL PIPELINE COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
    finally:
        # Always dispose the engine
        engine.dispose()
    
    print("\n🔚 Pipeline finished")

if __name__ == "__main__":
    main()