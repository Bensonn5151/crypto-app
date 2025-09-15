import streamlit as st
from databricks.sql import connect
import pandas as pd
import plotly.express as px
import time
from datetime import datetime

# Page configuration
st.set_page_config(page_title="Crypto Dashboard", layout="wide", page_icon="📊")

# Initialize connection with caching and error handling
@st.cache_resource(ttl=3600)
def init_connection():
    return connect(
        server_hostname=st.secrets["DATABRICKS_HOST"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"]
    )


# Get data with caching
@st.cache_data(ttl=600, show_spinner="Loading crypto data...")
def get_data(_conn):  # Prefix with underscore so Streamlit doesn't try to hash it
    try:
        with _conn.cursor() as cursor:
            cursor.execute("""
                SELECT date, open, high, low, close, volume
                FROM gold_btc_historical
                WHERE symbol = 'BTC'
                ORDER BY date DESC
                LIMIT 1000
            """)
            df = pd.DataFrame(cursor.fetchall(), 
                             columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        return df
    except Exception as e:
        st.error(f"❌ Query failed: {str(e)}")
        return pd.DataFrame()


# Initialize connection
conn = init_connection()


# Title and description
st.title("📈 Interactive Crypto Dashboard")
st.markdown("""
    <div style="background-color:gold;padding:10px;border-radius:5px;margin-bottom:20px">
        <p style="color:black;">Real-time Bitcoin price data visualized from Databricks SQL warehouse</p>
    </div>
""", unsafe_allow_html=True)


# Test connection button
if st.sidebar.button("🔌 Test Connection"):
    with st.spinner("Testing connection..."):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 'Connection successful!' AS message")
                result = cursor.fetchone()
            st.sidebar.success(result.message)
        except Exception as e:
            st.sidebar.error(f"Connection failed: {str(e)}")

# Load data automatically on startup
if 'data' not in st.session_state:
    with st.spinner("Loading initial data..."):
        st.session_state.data = get_data(conn)

# Display data if available
if not st.session_state.data.empty:
    # Create layout
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Latest Bitcoin Prices")
        st.dataframe(st.session_state.data.head(10), height=400)
        
        # Summary stats
        latest = st.session_state.data.iloc[0]
        st.metric("Daily Open", f"${latest['open']:,.2f}")
        st.metric("24h Volume", f"${latest['volume']:,.0f}")
        
        # Refresh button
        if st.button("🔄 Refresh Data"):
            with st.spinner("Updating data..."):
                # Clear cache to force reload
                st.cache_data.clear()
                st.session_state.data = get_data(conn)
                st.rerun()
    
    with col2:
        # Create interactive chart
        st.subheader("Price History")
        
        # Create a copy for plotting to avoid modifying cached data
        plot_data = st.session_state.data.copy().sort_values('date', ascending=True)
        
        # Candlestick chart
        fig = px.line(plot_data, x='date', y='close', 
                     title='Bitcoin Price History',
                     labels={'close': 'Price (USD)', 'date': 'Date'},
                     template='plotly_dark')
        
        # Add range slider for time navigation
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=7, label="1w", step="day", stepmode="backward"),
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(step="all")
                ])
            )
        )
        
        # Add moving averages
        fig.add_scatter(x=plot_data['date'], y=plot_data['close'].rolling(20).mean(), 
                        mode='lines', name='20-day MA', line=dict(color='orange'))
        fig.add_scatter(x=plot_data['date'], y=plot_data['close'].rolling(50).mean(), 
                        mode='lines', name='50-day MA', line=dict(color='purple'))
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Volume chart
        st.subheader("Trading Volume")
        fig_vol = px.bar(plot_data, x='date', y='volume', 
                         labels={'volume': 'Volume (USD)', 'date': 'Date'},
                         color='volume',
                         color_continuous_scale='blues')
        st.plotly_chart(fig_vol, use_container_width=True)

else:
    st.warning("No data available. Please check your database connection and query.")
    
# Add status footer
st.sidebar.divider()
st.sidebar.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.markdown(f"**Data points:** {len(st.session_state.data)}")
st.sidebar.markdown(f"**Time range:** {st.session_state.data['date'].min().strftime('%Y-%m-%d') if not st.session_state.data.empty else ''} to {st.session_state.data['date'].max().strftime('%Y-%m-%d') if not st.session_state.data.empty else ''}")

# Add debug info in expander
with st.sidebar.expander("Debug Info"):
    st.write("Connection details:")
    st.json({
        "host": st.secrets["DATABRICKS_HOST"][:10] + "..." if st.secrets["DATABRICKS_HOST"] else "Not set",
        "http_path": st.secrets["DATABRICKS_HTTP_PATH"][:10] + "..." if st.secrets["DATABRICKS_HTTP_PATH"] else "Not set",
        "token": st.secrets["DATABRICKS_TOKEN"][:5] + "..." if st.secrets["DATABRICKS_TOKEN"] else "Not set"
    })
    
    if not st.session_state.data.empty:
        st.write("Data sample:")
        st.dataframe(st.session_state.data.head(3))