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

# -------------------------------
# Data fetching
# -------------------------------
@st.cache_data(ttl=600, show_spinner="Loading hourly data...")
def fetch_hourly_data(_engine, symbol: str) -> pd.DataFrame:
    """Fetch hourly price data for a given symbol from the database."""
    query = """
        SELECT symbol, datetime, open, high, low, close, volume
        FROM yfinance_hourly
        WHERE symbol = :symbol
        ORDER BY datetime DESC
        LIMIT 1000
    """
    try:
        # Alternative: Use connection explicitly with text() for proper parameter binding
        with _engine.connect() as conn:
            result = conn.execute(text(query), {"symbol": symbol})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Error fetching hourly data: {e}")
        return pd.DataFrame()

# -------------------------------
# UI Components
# -------------------------------
def show_title_and_description():
    st.set_page_config(
        page_title="Yahoo Finance Hourly Dashboard",
        layout="wide",
        page_icon="⏰",
    )
    st.title("⏰ Yahoo Finance Hourly Dashboard")
    st.markdown(
        """
        <div style="background-color:lightblue;padding:10px;border-radius:5px;margin-bottom:20px">
            <p style="color:black;">Hourly crypto prices loaded from your own PostgreSQL database (Yahoo Finance source)</p>
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
    except Exception as e:
        st.sidebar.error(f"Connection failed: {str(e)}")

def display_data_panel(df: pd.DataFrame, symbol: str, engine):
    """Show latest hourly data, price chart, and volume chart."""
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader(f"Latest {symbol} Hourly Prices")
        st.dataframe(df.head(10), height=400)

        latest = df.iloc[0]
        st.metric("Hourly Open", f"${latest['open']:,.2f}")
        st.metric("Hourly Volume", f"${latest['volume']:,.0f}")

        if st.button("🔄 Refresh Hourly Data"):
            st.cache_data.clear()
            st.session_state.hourly_data = fetch_hourly_data(engine, symbol)
            st.rerun()

    with col2:
        st.subheader("Hourly Price History")
        plot_data = df.copy().sort_values(["datetime", "hour"], ascending=True)

        fig = px.line(
            plot_data,
            x="hour",
            y="close",
            color="datetime",
            title=f"{symbol} Hourly Price History",
            labels={"close": "Price (USD)", "hour": "Hour"},
            template="plotly_dark",
        )
        fig.add_scatter(
            x=plot_data["hour"],
            y=plot_data["close"].rolling(24).mean(),
            mode="lines",
            name="24-hour MA",
            line=dict(color="orange"),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Hourly Trading Volume")
        fig_vol = px.bar(
            plot_data,
            x="hour",
            y="volume",
            color="datetime",
            labels={"volume": "Volume (USD)", "hour": "Hour"},
            color_continuous_scale="blues",
        )
        st.plotly_chart(fig_vol, use_container_width=True)

def show_sidebar_status(df: pd.DataFrame):
    """Show dataset summary in sidebar."""
    st.sidebar.divider()
    st.sidebar.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.sidebar.markdown(f"**Data points:** {len(df)}")

    if not df.empty:
        st.sidebar.markdown(
            f"**Time range:** {df['date'].min().strftime('%Y-%m-%d')} "
            f"to {df['date'].max().strftime('%Y-%m-%d')}"
        )

def show_debug_info(db_params: dict, df: pd.DataFrame):
    """Show debug info in sidebar expander."""
    with st.sidebar.expander("Debug Info"):
        st.write("DB Connection details:")
        st.json(
            {
                k: (v[:8] + "..." if isinstance(v, str) and v else "")
                for k, v in db_params.items()
            }
        )
        if not df.empty:
            st.write("Hourly data sample:")
            st.dataframe(df.head(3))

# -------------------------------
# Main App
# -------------------------------
def main():
    show_title_and_description()

    db_params = load_db_env()
    engine = get_engine(db_params)

    symbol = st.sidebar.selectbox("Symbol", ["BTC", "ETH", "SOL"], index=0)

    if st.sidebar.button("🔌 Test Connection"):
        test_connection(engine)

    if (
        "hourly_data" not in st.session_state
        or st.session_state.get("symbol") != symbol
        or st.session_state.get("engine_hash") != hash(str(engine))
    ):
        with st.spinner("Loading initial hourly data..."):
            st.session_state.hourly_data = fetch_hourly_data(engine, symbol)
            st.session_state.symbol = symbol
            st.session_state.engine_hash = hash(str(engine))

    df = st.session_state.hourly_data

    if not df.empty:
        display_data_panel(df, symbol, engine)
    else:
        st.warning("No hourly data available. Please check your database connection and query.")

    show_sidebar_status(df)
    show_debug_info(db_params, df)

# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    main()