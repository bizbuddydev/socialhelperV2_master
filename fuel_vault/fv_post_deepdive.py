import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import json

st.set_page_config(page_title="Post Analyzer 🚀", layout="wide", page_icon="📡")

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
@st.cache_data
def fetch_data(query: str) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = client.query(query)  # Execute query
    result = query_job.result()  # Wait for the query to finish
    return result.to_dataframe()

### **Fetch Post Data (Filtered by Page ID)**
datasetid = config["DATASET_ID"]
post_tableid = config["POST_TABLE_ID"]
analysis_tableid = config["ANALYSIS_TABLE_ID"]

page_id = 17841467121671609  # Page ID for filtering

# Query to get post data
post_query = f"""
SELECT *
FROM `bizbuddydemo-v2.{datasetid}.{post_tableid}`
WHERE page_id = {page_id}
AND DATE(insert_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY created_time DESC
"""

# Query to get analysis data
analysis_query = f"""
SELECT *
FROM `bizbuddydemo-v2.{datasetid}.{analysis_tableid}`
ORDER BY created_time DESC
"""

# Load post data
post_data = fetch_data(post_query)
post_data["Like Rate"] = round(post_data["like_count"] / post_data["reach"] * 100, 2)
post_data["created_time"] = pd.to_datetime(post_data["created_time"]).dt.date

# Load analysis data
analysis_data = fetch_data(analysis_query)

# Merge post data with analysis data on post_id = video_id
merged_data = post_data.merge(
    analysis_data, left_on="post_id", right_on="video_id", how="left", suffixes=("_posts", "_aps")
)

# Drop unnecessary duplicate columns after merge
merged_data = merged_data.drop(
    columns=["reach_aps", "like_count_aps", "comments_count_aps", "shares_aps", "saved_aps", "created_time_aps"]
)

### **Main App UI**
def main():
    st.title("Social Buddy 🚀 - Post Deep Dive")

    # Sidebar Filter
    st.sidebar.header("Filter Posts")
    filter_option = st.sidebar.selectbox("Select Timeframe", ["All Time", "Last 30 Days", "Last 6 Months"])

    # Timeframe Filtering
    if filter_option == "Last 30 Days":
        filtered_data = merged_data[merged_data["created_time"] >= date.today() - timedelta(days=30)]
    elif filter_option == "Last 6 Months":
        filtered_data = merged_data[merged_data["created_time"] >= date.today() - timedelta(days=182)]
    else:
        filtered_data = merged_data

    # Display Data Table
    st.subheader("Final Merged Data")
    st.dataframe(filtered_data)

if __name__ == "__main__":
    main()
