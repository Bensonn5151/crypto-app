import streamlit as st
import requests
from datetime import datetime
import time

# --------------------------
# CONFIG
# --------------------------
API_KEY = "YOUR_API_KEY_HERE"
url = "https://data-api.coindesk.com/index/cc/v1/latest/tick"

# --------------------------
# STREAMLIT UI
# --------------------------
st.set_page_config(page_title="Crypto Tracker", page_icon="📊", layout="wide")

st.title("📊 Real-time Crypto Tracker (CoinDesk API)")


# Refresh control
refresh_interval = st.slider("⏳ Refresh every (seconds)", min_value=5, max_value=60, value=10, step=5)
auto_refresh = st.checkbox("Enable Auto Refresh", value=True)

# --------------------------
# FETCH FUNCTION
# --------------------------
def fetch_data():
    params = {
        "market": "ccix",                                                        
        "instruments": "BTC-USD,ETH-USD,SOL-USD",
        "api_key": API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()
    records = []

    if "Data" in data:
        for instrument, values in data["Data"].items():
            record = {
                "symbol": instrument.split("-")[0].upper(),
                "current_price": values.get("VALUE"),
                "hour_high": values.get("CURRENT_HOUR_HIGH"),
                "hour_low": values.get("CURRENT_HOUR_LOW"),
                "day_high": values.get("CURRENT_DAY_HIGH"),
                "day_low": values.get("CURRENT_DAY_LOW"),
                "volume": values.get("CURRENT_DAY_VOLUME"),
                "last_update": datetime.utcfromtimestamp(values.get("VALUE_LAST_UPDATE_TS", 0))
            }
            records.append(record)
    return records

# --------------------------
# DISPLAY DATA (METRICS)
# --------------------------
placeholder = st.empty()

while True:
    data = fetch_data()
    with placeholder.container():
        st.write(f"**Last updated:** {datetime.utcnow()} UTC")

        # Display each coin's metrics in columns
        for record in data:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(f"{record['symbol']} Price", f"${record['current_price']:,.2f}")
            with col2:
                st.metric(f"{record['symbol']} 24h High", f"${record['day_high']:,.2f}")
            with col3:
                st.metric(f"{record['symbol']} 24h Low", f"${record['day_low']:,.2f}")

    if not auto_refresh:
        break
    time.sleep(refresh_interval)
