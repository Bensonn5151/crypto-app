import streamlit as st
import requests
from datetime import datetime, timedelta
import time
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from sqlalchemy import create_engine, text
import json

# --------------------------
# CONFIG & SETUP
# --------------------------
st.set_page_config(
    page_title="Crypto Dashboard", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_KEY = os.getenv('COINDESK_API_KEY', 'YOUR_API_KEY_HERE')
COINDESK_URL = "https://data-api.coindesk.com/index/cc/v1/latest/tick"

# Database Configuration
def get_db_engine():
    """Get database engine for historical data"""
    try:
        db_config = {
            "DB_USERNAME": os.getenv('DB_USERNAME', 'postgres'),
            "DB_PASSWORD": os.getenv('DB_PASSWORD', 'bens'),
            "DB_HOST": os.getenv('DB_HOST', 'localhost'),
            "DB_PORT": os.getenv('DB_PORT', '5432'),
            "DB_NAME": os.getenv('DB_NAME', 'crypto_app')
        }
        url = f'postgresql://{db_config["DB_USERNAME"]}:{db_config["DB_PASSWORD"]}@{db_config["DB_HOST"]}:{db_config["DB_PORT"]}/{db_config["DB_NAME"]}'
        return create_engine(url)
    except Exception as e:
        st.warning(f"Database connection failed: {e}")
        return None

# --------------------------
# DATA FUNCTIONS
# --------------------------
@st.cache_data(ttl=30)  # Cache for 30 seconds
def fetch_realtime_data():
    """Fetch real-time data from CoinDesk API"""
    params = {
        "market": "ccix",                                                        
        "instruments": "BTC-USD,ETH-USD,SOL-USD,ADA-USD",
        "api_key": API_KEY
    }
    
    try:
        response = requests.get(COINDESK_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        records = []

        if "Data" in data:
            for instrument, values in data["Data"].items():
                # Safely handle all values with defaults
                record = {
                    "symbol": instrument.split("-")[0].upper(),
                    "current_price": values.get("VALUE", 0),
                    "hour_high": values.get("CURRENT_HOUR_HIGH", 0),
                    "hour_low": values.get("CURRENT_HOUR_LOW", 0),
                    "day_high": values.get("CURRENT_DAY_HIGH", 0),
                    "day_low": values.get("CURRENT_DAY_LOW", 0),
                    "volume": values.get("CURRENT_DAY_VOLUME", 0),
                    "price_change": values.get("VALUE_CHANGE", 0),
                    "price_change_percent": values.get("VALUE_CHANGE_PERCENT", 0),
                    "last_update_ts": values.get("VALUE_LAST_UPDATE_TS", 0),
                    "last_update": datetime.utcfromtimestamp(values.get("VALUE_LAST_UPDATE_TS", 0)) if values.get("VALUE_LAST_UPDATE_TS") else datetime.utcnow(),
                    "timestamp": datetime.utcnow()
                }
                records.append(record)
        return pd.DataFrame(records)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def fetch_historical_data(symbol, days=7):
    """Fetch historical data from database with comprehensive error handling"""
    engine = get_db_engine()
    if engine is None:
        st.warning("Database connection not available")
        return pd.DataFrame()
    
    try:
        # Check if table exists first
        from sqlalchemy import inspect
        inspector = inspect(engine)
        
        if not inspector.has_table('coindesk_prices'):
            st.warning("coindesk_prices table does not exist yet")
            return pd.DataFrame()
        
        # Use parameterized query
        query = text("""
            SELECT timestamp, current_price 
            FROM coindesk_prices 
            WHERE symbol = :symbol 
            AND timestamp >= CURRENT_TIMESTAMP - INTERVAL ':days days'
            ORDER BY timestamp
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {'symbol': symbol, 'days': days})
            rows = result.fetchall()
            
            if not rows:
                return pd.DataFrame()
                
            df = pd.DataFrame(rows, columns=result.keys())
            return df
            
    except Exception as e:
        st.warning(f"Could not fetch historical data for {symbol}: {e}")
        return pd.DataFrame()

# --------------------------
# UI COMPONENTS
# --------------------------
def display_price_card(record):
    """Display individual cryptocurrency card"""
    # Handle None values safely
    price_change = record.get('price_change', 0) or 0
    change_percent = record.get('price_change_percent', 0) or 0
    
    # Ensure values are floats (handle None cases)
    try:
        price_change = float(price_change) if price_change is not None else 0
        change_percent = float(change_percent) if change_percent is not None else 0
    except (TypeError, ValueError):
        price_change = 0
        change_percent = 0
    
    # Determine color and arrow based on price change
    color = "green" if price_change >= 0 else "red"
    arrow = "↑" if price_change >= 0 else "↓"
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label=f"{record['symbol']} Price",
            value=f"${record['current_price']:,.2f}" if record.get('current_price') else "N/A",
            delta=f"{arrow} ${abs(price_change):.2f} ({abs(change_percent):.2f}%)" if price_change != 0 else None,
            delta_color="normal" if price_change >= 0 else "inverse"
        )
    
    with col2:
        st.metric(
            label=f"{record['symbol']} 24h High",
            value=f"${record['day_high']:,.2f}" if record.get('day_high') else "N/A"
        )
    
    with col3:
        st.metric(
            label=f"{record['symbol']} 24h Low",
            value=f"${record['day_low']:,.2f}" if record.get('day_low') else "N/A"
        )

def create_price_chart(symbol, historical_data):
    """Create price chart for a cryptocurrency"""
    if historical_data.empty:
        return None
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=historical_data['timestamp'],
        y=historical_data['current_price'],
        mode='lines',
        name=f'{symbol} Price',
        line=dict(color='#00D4AA', width=2)
    ))
    
    fig.update_layout(
        title=f"{symbol} Price Trend (Last 7 Days)",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        template="plotly_dark",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig

# --------------------------
# SIDEBAR
# --------------------------
st.sidebar.title("🛠️ Dashboard Controls")

# Refresh controls
refresh_interval = st.sidebar.slider(
    "⏳ Refresh interval (seconds)", 
    min_value=5, max_value=300, value=30, step=5
)

auto_refresh = st.sidebar.checkbox("Enable Auto Refresh", value=True)

# Coin selection
available_coins = ["BTC", "ETH", "SOL", "ADA"]
selected_coins = st.sidebar.multiselect(
    "Select Cryptocurrencies",
    options=available_coins,
    default=available_coins
)

# Display options
show_charts = st.sidebar.checkbox("Show Price Charts", value=True)
show_volume = st.sidebar.checkbox("Show Volume Data", value=True)

# --------------------------
# MAIN DASHBOARD
# --------------------------
st.title("📊 Real-time Crypto Dashboard")
st.markdown("---")

# Initialize session state for refresh control
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

# Refresh button
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("🔄 Manual Refresh", use_container_width=True):
        st.session_state.last_refresh = datetime.now()
        st.rerun()

# Main data display
placeholder = st.empty()

# Auto-refresh logic
if auto_refresh:
    time_since_refresh = datetime.now() - st.session_state.last_refresh
    if time_since_refresh.total_seconds() >= refresh_interval:
        st.session_state.last_refresh = datetime.now()
        st.rerun()

# Data fetching and display
with placeholder.container():
    # Fetch real-time data
    realtime_df = fetch_realtime_data()
    
    if not realtime_df.empty:
        # Filter selected coins
        filtered_df = realtime_df[realtime_df['symbol'].isin(selected_coins)]
        
        # Display last update time
        latest_update = filtered_df['last_update'].max()
        st.write(f"**Last updated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        st.write(f"**Latest data timestamp:** {latest_update.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        st.markdown("---")
        
        # Display price cards
        for _, record in filtered_df.iterrows():
            display_price_card(record)
            
            # Show volume data if enabled
            if show_volume and record.get('volume'):
                st.write(f"**24h Volume:** ${record['volume']:,.0f}")
            
            # Show price chart if enabled
            if show_charts:
                historical_data = fetch_historical_data(record['symbol'])
                if not historical_data.empty:
                    chart = create_price_chart(record['symbol'], historical_data)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
            
            st.markdown("---")
        
        # Summary statistics
        st.subheader("📈 Market Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        total_volume = filtered_df['volume'].sum()
        avg_price_change = filtered_df['price_change_percent'].mean()
        
        with col1:
            st.metric("Total Tracked", f"{len(filtered_df)} coins")
        with col2:
            st.metric("Total Volume", f"${total_volume:,.0f}")
        with col3:
            st.metric("Avg Change", f"{avg_price_change:.2f}%")
        with col4:
            st.metric("Last Update", "Live")
    
    else:
        st.error("❌ No data available. Please check your API key and connection.")

# --------------------------
# FOOTER
# --------------------------
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>"
    "Data provided by CoinDesk API | Built with Streamlit"
    "</div>", 
    unsafe_allow_html=True
)