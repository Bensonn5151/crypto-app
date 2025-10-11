import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------
# Database utilities
# -------------------------------
def load_db_env():
    """Load DB settings for Streamlit (running outside Docker)."""
    load_dotenv(dotenv_path="/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env")

    return {
        "DB_USERNAME": os.getenv("DB_USERNAME", "postgres"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", "bens"),
        "DB_HOST": "localhost",  # 👈 force local host
        "DB_PORT": "5434",       # 👈 use mapped port
        "DB_NAME": os.getenv("DB_NAME", "crypto_app"),
    }

@st.cache_resource(ttl=3600)
def get_engine(db_params: dict):
    """Create and cache SQLAlchemy engine."""
    url = (
        f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}'
        f'@{db_params["DB_HOST"]}:{db_params["DB_PORT"]}/{db_params["DB_NAME"]}'
    )
    return create_engine(url)

# -------------------------------
# Data fetching
# -------------------------------
@st.cache_data(ttl=600, show_spinner="Loading historical data...")
def fetch_historical_data(_engine, symbol: str) -> pd.DataFrame:
    """Fetch historical price data for a given symbol from the database."""
    # First try yfinance_historical table
    query = """
        SELECT symbol, datetime, open, high, low, close, volume
        FROM yfinance_historical
        WHERE symbol = :symbol
        ORDER BY datetime DESC
        LIMIT 1000
    """
    try:
        with _engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            # If no data found, try yfinance_hourly table
            if df.empty:
                st.info("No data in yfinance_historical, trying yfinance_hourly...")
                query_hourly = """
                    SELECT symbol, date, open, high, low, close, volume,created_at as datetime
                    FROM yfinance_historical
                    WHERE symbol = :symbol
                    ORDER BY date DESC
                    LIMIT 1000
                """
                result = conn.execute(text(query_hourly), {"symbol": symbol})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
        return df
    except Exception as e:
        st.error(f"Error fetching historical data: {e}")
        return pd.DataFrame()

def check_available_tables(engine):
    """Check what tables are available in the database."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result]
            return tables
    except Exception as e:
        st.error(f"Error checking tables: {e}")
        return []

# -------------------------------
# UI Components
# -------------------------------
def show_title_and_description():
    st.set_page_config(
        page_title="Yahoo Finance Historical Dashboard",
        layout="wide",
        page_icon="📊",
    )
    st.title("📈 Yahoo Finance Historical Dashboard")
    st.markdown(
        """
        <div style="background-color:gold;padding:10px;border-radius:5px;margin-bottom:20px">
            <p style="color:black;">Historical crypto prices loaded from your own PostgreSQL database (Yahoo Finance source)</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

def test_connection(engine):
    """Test DB connection and show status in sidebar."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            st.sidebar.success(f"Connected: {result.scalar()}")
            
            # Show available tables
            tables = check_available_tables(engine)
            if tables:
                st.sidebar.info(f"Available tables: {', '.join(tables)}")
            else:
                st.sidebar.warning("No tables found in database")
                
    except Exception as e:
        st.sidebar.error(f"Connection failed: {str(e)}")

def display_data_panel(df: pd.DataFrame, symbol: str, engine):
    """Show latest data, price chart, and volume chart."""
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader(f"Latest {symbol} Prices")
        if not df.empty:
            st.dataframe(df.head(10), height=400)

            latest = df.iloc[0]
            st.metric("Latest Close", f"${latest['close']:,.2f}")
            st.metric("24h Volume", f"${latest['volume']:,.0f}")
        else:
            st.info("No data to display")

        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.session_state.data = fetch_historical_data(engine, symbol)
            st.rerun()

    with col2:
        if not df.empty:
            st.subheader("Price History")
            # Convert datetime and sort
            df['datetime'] = pd.to_datetime(df['datetime'])
            plot_data = df.sort_values("datetime", ascending=True).copy()

            # Price chart
            fig = px.line(
                plot_data,
                x="datetime",
                y="close",
                title=f"{symbol} Price History",
                labels={"close": "Price (USD)", "datetime": "Date"},
                template="plotly_dark",
            )
            fig.update_xaxes(
                rangeslider_visible=True,
                rangeselector=dict(
                    buttons=[
                        dict(count=7, label="1w", step="day", stepmode="backward"),
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(step="all"),
                    ]
                ),
            )
            
            # Add moving averages if we have enough data
            if len(plot_data) >= 20:
                fig.add_scatter(
                    x=plot_data["datetime"],
                    y=plot_data["close"].rolling(20).mean(),
                    mode="lines",
                    name="20-period MA",
                    line=dict(color="orange"),
                )
            if len(plot_data) >= 50:
                fig.add_scatter(
                    x=plot_data["datetime"],
                    y=plot_data["close"].rolling(50).mean(),
                    mode="lines",
                    name="50-period MA",
                    line=dict(color="purple"),
                )
            st.plotly_chart(fig, use_container_width=True)

            # Volume chart
            st.subheader("Trading Volume")
            fig_vol = px.bar(
                plot_data,
                x="datetime",
                y="volume",
                labels={"volume": "Volume (USD)", "datetime": "Date"},
                color="volume",
                color_continuous_scale="blues",
            )
            st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.info("No data available for charts")

def show_sidebar_status(df: pd.DataFrame):
    """Show dataset summary in sidebar."""
    st.sidebar.divider()
    st.sidebar.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.sidebar.markdown(f"**Data points:** {len(df)}")

    if not df.empty and 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
        st.sidebar.markdown(
            f"**Time range:** {df['datetime'].min().strftime('%Y-%m-%d')} "
            f"to {df['datetime'].max().strftime('%Y-%m-%d')}"
        )

def show_debug_info(db_params: dict, df: pd.DataFrame, engine):
    """Show debug info in sidebar expander."""
    with st.sidebar.expander("Debug Info"):
        st.write("DB Connection details:")
        st.json(
            {
                k: (v if k == 'DB_PORT' else (v[:8] + "..." if isinstance(v, str) and v else ""))
                for k, v in db_params.items()
            }
        )
        
        # Show table info
        tables = check_available_tables(engine)
        st.write(f"Available tables: {tables}")
        
        if not df.empty:
            st.write("Data sample:")
            st.dataframe(df.head(3))
            st.write(f"Data columns: {list(df.columns)}")
            st.write(f"Data types: {df.dtypes.to_dict()}")

# -------------------------------
# Main App
# -------------------------------
def main():
    show_title_and_description()

    db_params = load_db_env()
    engine = get_engine(db_params)

    symbol = st.sidebar.selectbox("Symbol", ["BTC-USD", "ETH-USD", "SOL-USD", "BTC", "ETH", "SOL"], index=0)

    if st.sidebar.button("🔌 Test Connection"):
        test_connection(engine)

    if (
        "data" not in st.session_state
        or st.session_state.get("symbol") != symbol
        or st.session_state.get("engine_hash") != hash(str(engine))
    ):
        with st.spinner("Loading initial data..."):
            st.session_state.data = fetch_historical_data(engine, symbol)
            st.session_state.symbol = symbol
            st.session_state.engine_hash = hash(str(engine))

    df = st.session_state.data

    if not df.empty:
        display_data_panel(df, symbol, engine)
    else:
        st.warning("""
        No data available. This could be because:
        
        - The table doesn't exist in your database
        - The symbol doesn't have any data
        - There's a connection issue
        
        Check the 'Debug Info' section in the sidebar for more details about your database structure.
        """)

    show_sidebar_status(df)
    show_debug_info(db_params, df, engine)

# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    main()