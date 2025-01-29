import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import json

st.set_page_config(page_title="Post Analyzer", layout="wide", page_icon="📱")

# Define links to other pages
PAGES = {
    "📊 Overview": "https://smp-bizbuddy-accountoverview.streamlit.app/",
    "📱 Posts": "https://smp-bizbuddy-postoverview.streamlit.app",
    "🗓️ Scheduler": "https://smp-bizbuddy-postscheduler.streamlit.app/",
    "💡 Brainstorm": "https://smp-bizbuddy-v1-brainstorm.streamlit.app/"
}

# Sidebar navigation
st.sidebar.title("Navigation")
for page, url in PAGES.items():
    st.sidebar.markdown(f"[**{page}**]({url})", unsafe_allow_html=True)

# Load the configuration file
def load_config(file_path="config.json"):
    with open(file_path, "r") as f:
        return json.load(f)

# Load the account configuration
config = load_config()

# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
project_id = st.secrets["gcp_service_account"]["project_id"]

# Function to fetch data from BigQuery
def fetch_data(query: str) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = client.query(query)  # Make a query request
    result = query_job.result()  # Wait for the query to finish
    return result.to_dataframe()

# Define filter functions
def filter_last_30_days(df):
    cutoff = date.today() - timedelta(days=30)
    return df[df["post_date"] >= cutoff].sort_values(by="post_date", ascending=False)

def filter_last_6_months(df):
    cutoff = date.today() - timedelta(days=182)  # Approx. 6 months
    return df[df["post_date"] >= cutoff].sort_values(by="post_date", ascending=False)

def top_10_by_column(df, column):
    return df.sort_values(by=column, ascending=False).head(10)


# Use the variables in your app
account_name = config["ACCOUNT_NAME"]
datasetid = config["TESTING_DATASET_ID"]
tableid = config["ANALYSIS_TABLE_ID"]

### Get data ###
query = f"""
SELECT *
FROM `bizbuddydemo-v2.{datasetid}.{tableid}`
ORDER BY created_time DESC
"""

# Load/Transform Data
data = fetch_data(query)
data['post_date'] = data['created_time'].dt.date


def main():
    st.title("Social Buddy - Post Deep Dive")

    # Filtering Options
    st.sidebar.header("Filter Posts")
    filter_option = st.sidebar.selectbox("Select Timeframe", ["Last 30 Days", "Last 6 Months", "All Time"])

    if filter_option == "Last 30 Days":
        filtered_data = filter_last_30_days(data)
    elif filter_option == "Last 6 Months":
        filtered_data = filter_last_6_months(data)
    else:
        filtered_data = data
    
    col_left1, col_right1 = st.columns(2)
    
    with col_left1:

        param_col1, param_col2 = st.columns(2)
            
        with param_col1:
            # Select variable for visualization
            metric_option = st.selectbox("Select Metric", ["reach", "like_count", "comments"])

        with param_col2:
            # Select variable for visualization
            dim_option = st.selectbox("Select Dimension", ["time_bucket", "weekday"])

        timing_analysis = filtered_data.groupby([dim_option]).agg({metric_option: "mean"}).reset_index()
        
        # Create bar chart with selected metric
        fig = px.bar(
            timing_analysis,
            x=dim_option,
            y=metric_option,
            title=f"{metric_option.replace('_', ' ').title()} by Time Bucket",
            labels={metric_option: "Average Value", "dim_option": "Time"},
            template="plotly_white"
        )
        
        # Display in Streamlit
        st.plotly_chart(fig)

    with col_right1:
        # Analysis - Sound Type & Engagement
        st.subheader("Sound Type & Engagement")
        sound_analysis = filtered_data.groupby("sound_type").agg({"reach": "mean", "like_count": "mean"}).reset_index()
        st.dataframe(sound_analysis)


if __name__ == "__main__":
    main()
