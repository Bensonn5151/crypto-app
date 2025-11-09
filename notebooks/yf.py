import streamlit as st
from databricks.sql import connect
import pandas as pd

# Page configuration
st.set_page_config(page_title="Databricks + Streamlit", layout="wide")

# Initialize connection in a way that persists
@st.cache_resource(ttl=3600)  # Reconnect every hour
def init_connection():
    conn = connect(
        server_hostname=st.secrets["DATABRICKS_HOST"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"]
    )
    return conn

# Run query with proper connection management
@st.cache_data(ttl=600, show_spinner="Running query...")
def run_query(query):
    # Get a NEW connection each time
    conn = init_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
    finally:
        # Close connection immediately after use
        conn.close()

# UI Components
st.title("Databricks + Streamlit Integration")

query = st.text_area("SQL Query", value="SELECT * FROM gold_btc_historical")

if st.button("Run Query"):
    with st.spinner("Executing query..."):
        try:
            df = run_query(query)
            st.dataframe(df)
            
            st.download_button(
                label="Download as CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name='databricks_data.csv',
                mime='text/csv'
            )
        except Exception as e:
            st.error(f"Query failed: {str(e)}")