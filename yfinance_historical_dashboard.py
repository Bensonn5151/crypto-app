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
    """Load database parameters from .env file."""
    load_dotenv(dotenv_path="/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/.env")
    return {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USERNAME": os.getenv("DB_USERNAME"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
    }


@st.cache_resource(ttl=3600)
def get_engine(db_params: dict):
    """Create and cache SQLAlchemy engine."""
    url = (
        f'postgresql://{db_params["DB_USERNAME"]}:{db_params["DB_PASSWORD"]}'
        f'@{db_params["DB_HOST"]}:{db_params["DB_PORT"]}/{db_params["DB_NAME"]}'
    )
    return create_engine(url)


@st.cache_data(ttl=600, show_spinner="Loading historical data...")
def fetch_historical_data(_engine, symbol: str = "BTC") -> pd.DataFrame:
    """Fetch historical price data for a given symbol from the database."""
    query = text(
        """
        SELECT symbol, date, open, high, low, close, volume
        FROM yfinance_historical
        WHERE symbol = :symbol
        ORDER BY date DESC
        LIMIT 1000
        """
    )
    try:
        df = pd.read_sql(query, _engine, params={"symbol": symbol})
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()


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
    except Exception as e:
        st.sidebar.error(f"Connection failed: {str(e)}")


def display_data_panel(df: pd.DataFrame, engine, symbol: str):
    """Show latest data, price chart, and volume chart."""
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader(f"Latest {symbol} Prices")
        st.dataframe(df.head(10), height=400)

        latest = df.iloc[0]
        st.metric("Daily Open", f"${latest['open']:,.2f}")
        st.metric("24h Volume", f"${latest['volume']:,.0f}")

        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.session_state.data = fetch_historical_data(engine, symbol)
            st.rerun()

    with col2:
        st.subheader("Price History")
        plot_data = df.copy().sort_values("date", ascending=True)

        # Price chart
        fig = px.line(
            plot_data,
            x="date",
            y="close",
            title=f"{symbol} Price History",
            labels={"close": "Price (USD)", "date": "Date"},
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
        fig.add_scatter(
            x=plot_data["date"],
            y=plot_data["close"].rolling(20).mean(),
            mode="lines",
            name="20-day MA",
            line=dict(color="orange"),
        )
        fig.add_scatter(
            x=plot_data["date"],
            y=plot_data["close"].rolling(50).mean(),
            mode="lines",
            name="50-day MA",
            line=dict(color="purple"),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Volume chart
        st.subheader("Trading Volume")
        fig_vol = px.bar(
            plot_data,
            x="date",
            y="volume",
            labels={"volume": "Volume (USD)", "date": "Date"},
            color="volume",
            color_continuous_scale="blues",
        )
        st.plotly_chart(fig_vol, use_container_width=True)


def show_sidebar_status(df: pd.DataFrame):
    """Show dataset summary in sidebar."""
    st.sidebar.divider()
    st.sidebar.markdown(
        f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
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
            st.write("Data sample:")
            st.dataframe(df.head(3))


# -------------------------------
# Main App
# -------------------------------
def main():
    show_title_and_description()

    # Load DB + engine
    db_params = load_db_env()
    engine = get_engine(db_params)

    # Sidebar controls
    symbol = st.sidebar.selectbox("Symbol", ["BTC", "ETH", "SOL"], index=0)
    if st.sidebar.button("🔌 Test Connection"):
        test_connection(engine)

    # Load or refresh data
    if (
        "data" not in st.session_state
        or st.session_state.get("symbol") != symbol
    ):
        with st.spinner("Loading initial data..."):
            st.session_state.data = fetch_historical_data(engine, symbol)
            st.session_state.symbol = symbol

    df = st.session_state.data

    # Show main panel
    if not df.empty:
        display_data_panel(df, engine, symbol)
    else:
        st.warning(
            "No data available. Please check your database connection and query."
        )

    # Sidebar info
    show_sidebar_status(df)
    show_debug_info(db_params, df)


# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    main()
