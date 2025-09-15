import streamlit as st
import yfinance_historical_dashboard
import coindesk_dashboard

st.set_page_config(page_title="Combined Dashboards", layout="wide")

st.title("📊 Combined Crypto Dashboards")

# Show CoinDesk dashboard
st.header("🪙 CoinDesk Dashboard")
coindesk_dashboard.show()

st.markdown("---")  # Separator line

# Show Yahoo Finance dashboard
st.header("📈 Yahoo Finance Dashboard")
yfinance_historical_dashboard.show()
