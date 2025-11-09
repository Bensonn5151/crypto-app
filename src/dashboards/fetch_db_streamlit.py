import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

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

@st.cache_resource(ttl=3600)
def get_engine(db_params: dict):
    """Create and cache SQLAlchemy engine."""
    url = (
        f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}'
        f'@{db_params["DB_HOST"]}:{db_params["DB_PORT"]}/{db_params["DB_NAME"]}'
    )
    return create_engine(url)

@st.cache_data(ttl=600, show_spinner="Loading database info...")
def get_database_info(_engine):
    """Get database connection info and table list."""
    try:
        # Get database connection info
        with _engine.connect() as conn:
            # Get database version
            db_version_result = conn.execute(text("SELECT version();"))
            db_version = db_version_result.scalar()
            
            # Get current database name
            db_name_result = conn.execute(text("SELECT current_database();"))
            db_name = db_name_result.scalar()
            
            # Get all tables in the database
            inspector = inspect(_engine)
            tables = inspector.get_table_names()
            
            # Get table schemas
            table_schemas = {}
            for table in tables:
                columns = inspector.get_columns(table)
                table_schemas[table] = columns
            
            return {
                "db_version": db_version,
                "db_name": db_name,
                "tables": tables,
                "table_schemas": table_schemas,
                "connection_success": True
            }
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return {
            "connection_success": False,
            "error": str(e)
        }

@st.cache_data(ttl=600, show_spinner="Loading hourly data...")
def fetch_hourly_data(_engine, symbol: str) -> pd.DataFrame:
    """Fetch hourly price data for a given symbol from the database."""
    query = """
        SELECT symbol, datetime, open, high, low, close, volume
        FROM yfinance_hourly
        WHERE symbol = :symbol
        ORDER BY date DESC, hour DESC
        LIMIT 1000
    """
    try:
        with _engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Error fetching hourly data: {e}")
        return pd.DataFrame()

def main():
    st.title("Cryptocurrency Dashboard")
    
    # Load database configuration
    db_config = load_db_env()
    
    # Display database configuration (with password masked)
    st.sidebar.header("Database Configuration")
    st.sidebar.write(f"**Host:** {db_config['DB_HOST']}")
    st.sidebar.write(f"**Port:** {db_config['DB_PORT']}")
    st.sidebar.write(f"**Database:** {db_config['DB_NAME']}")
    st.sidebar.write(f"**Username:** {db_config['DB_USERNAME']}")
    st.sidebar.write(f"**Password:** {'*' * len(db_config['DB_PASSWORD']) if db_config['DB_PASSWORD'] else 'None'}")
    
    # Create engine and get database info
    try:
        engine = get_engine(db_config)
        
        # Test connection and get database info
        with st.spinner("Connecting to database..."):
            db_info = get_database_info(engine)
        
        if db_info["connection_success"]:
            st.success("✅ Successfully connected to database!")
            
            # Display database information
            st.header("Database Information")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Database Name", db_info["db_name"])
            
            with col2:
                st.metric("Total Tables", len(db_info["tables"]))
            
            # Display database version
            st.subheader("Database Version")
            st.code(db_info["db_version"], language="sql")
            
            # Display tables information
            st.header("Database Tables")
            
            if db_info["tables"]:
                # Create tabs for different views
                tab1, tab2 = st.tabs(["Table List", "Table Schemas"])
                
                with tab1:
                    st.subheader("Available Tables")
                    for i, table in enumerate(db_info["tables"], 1):
                        st.write(f"{i}. `{table}`")
                
                with tab2:
                    st.subheader("Table Schemas")
                    for table in db_info["tables"]:
                        with st.expander(f"Table: `{table}`"):
                            if table in db_info["table_schemas"]:
                                columns_info = []
                                for col in db_info["table_schemas"][table]:
                                    col_info = {
                                        "Column Name": col["name"],
                                        "Type": str(col["type"]),
                                        "Nullable": col["nullable"],
                                        "Primary Key": col.get("primary_key", False)
                                    }
                                    columns_info.append(col_info)
                                
                                if columns_info:
                                    st.dataframe(columns_info, use_container_width=True)
                                else:
                                    st.info("No column information available")
                            else:
                                st.warning("No schema information available")
                
                # Show sample data from tables
                st.header("Sample Data")
                selected_table = st.selectbox("Select a table to view sample data:", db_info["tables"])
                
                if selected_table:
                    try:
                        with st.spinner(f"Loading sample data from {selected_table}..."):
                            sample_query = text(f"SELECT * FROM {selected_table} LIMIT 10")
                            with engine.connect() as conn:
                                sample_result = conn.execute(sample_query)
                                sample_df = pd.DataFrame(sample_result.fetchall(), columns=sample_result.keys())
                        
                        st.subheader(f"Sample data from `{selected_table}`")
                        st.dataframe(sample_df, use_container_width=True)
                        
                        # Show basic statistics
                        st.subheader("Basic Statistics")
                        st.write(f"**Total rows in sample:** {len(sample_df)}")
                        st.write(f"**Columns:** {', '.join(sample_df.columns)}")
                        
                    except Exception as e:
                        st.error(f"Error loading sample data from {selected_table}: {e}")
            
            else:
                st.warning("No tables found in the database.")
        
        else:
            st.error(f"❌ Failed to connect to database: {db_info.get('error', 'Unknown error')}")
    
    except Exception as e:
        st.error(f"❌ Error creating database engine: {e}")
    
    # Your existing cryptocurrency dashboard functionality
    st.header("Cryptocurrency Data")
    
    # Example usage of your existing function
    if st.button("Test Data Fetch"):
        with st.spinner("Fetching sample cryptocurrency data..."):
            sample_data = fetch_hourly_data(engine, "BTC-USD")
            if not sample_data.empty:
                st.success(f"Successfully fetched {len(sample_data)} records for BTC-USD")
                st.dataframe(sample_data.head(), use_container_width=True)
            else:
                st.warning("No data returned for BTC-USD")

if __name__ == "__main__":
    main()