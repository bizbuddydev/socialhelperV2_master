import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import json

# For wordcloud
from wordcloud import WordCloud
import matplotlib.pyplot as plt

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

def assign_time_buckets(df):
    # Ensure datetime conversion retains time values
    df["created_time_posts"] = pd.to_datetime(df["created_time_posts"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
    
    # Ensure datetime conversion worked properly
    # if df["created_time_og"].isna().any():
    #     raise ValueError("Some created_time_posts values failed to parse. Check input format.")

    # Extract hour
    df["hour"] = df["created_time_og"].dt.hour

    # Define bucket mapping
    def bucketize(hour):
        if 9 <= hour < 12:  # 9 AM - 11 AM
            return f"{hour} AM"
        elif hour == 12:  # Noon case
            return "12 PM"
        elif 13 <= hour <= 23:  # Convert 13-23 (1 PM - 11 PM) correctly
            return f"{hour - 12} PM"
        elif hour == 0:  # Midnight
            return "12 AM"
        else:  # 1-8 AM
            return "1-8 AM"

    # Assign time buckets
    df["time_bucket"] = df["hour"].apply(bucketize)

    # Define categorical ordering
    time_bucket_order = [
        "9 AM", "10 AM", "11 AM", "12 PM", "1 PM", "2 PM", "3 PM", "4 PM", "5 PM",
        "6 PM", "7 PM", "8 PM", "9 PM", "10 PM", "11 PM", "12 AM", "1-8 AM"
    ]
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # Assign categorical values
    df["time_bucket"] = pd.Categorical(df["time_bucket"], categories=time_bucket_order, ordered=True)
    df["weekday"] = pd.Categorical(df["created_time_posts"].dt.day_name(), categories=weekday_order, ordered=True)

    # Drop the temporary hour column
    df = df.drop(columns=["hour"])

    return df
    
### **Fetch Post Data (Filtered by Page ID)**
datasetid = config["DATASET_ID"]
post_tableid = config["POST_TABLE_ID"]
analysis_tableid = config["ANALYSIS_TABLE_ID"]

page_id = 17841410640947509  # Page ID for filtering

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

#Fix post timing but maintain old created_time column
post_data["created_time_og"] = post_data["created_time"]

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

assign_time_buckets(merged_data)

merged_data = merged_data.rename(columns={"reach_posts":"reach", "like_count_posts":"like_count", "comments_count_posts":"comments_count", "shares_posts":"shares", "saved_posts":"saved"})

# Function to fetch data from BigQuery
def fetch_data(query: str) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = client.query(query)  # Make a query request
    result = query_job.result()  # Wait for the query to finish
    return result.to_dataframe()

# Define filter functions
def filter_last_30_days(df):
    cutoff = date.today() - timedelta(days=30)
    return df[df["created_time"] >= cutoff].sort_values(by="created_time", ascending=False)

def filter_last_6_months(df):
    cutoff = date.today() - timedelta(days=182)  # Approx. 6 months
    return df[df["created_time"] >= cutoff].sort_values(by="created_time", ascending=False)

def top_10_by_column(df, column):
    return df.sort_values(by=column, ascending=False).head(10)

def main():
    st.title("Social Buddy 🚀 - Harorview Social Report")

    # Define links to other pages
    PAGES = {
        "📊 Account Overview": "https://hv-bizbuddyv2-home.streamlit.app/",
        "📱 Posts Overview": "https://bizbuddyv2-hv-postoverview.streamlit.app/",
        "🔬 Posts Deepdive": "https://bizbuddyv2-hv-postdeepdive.streamlit.app/",
        "🗓️ Scheduler / Idea Generator": "https://bizbuddyv2-hv-postscheduler.streamlit.app/",
        "💡 Inspiration Upload": "https://hv-bizbuddyv2-inspiration.streamlit.app/",
        "Report" : "https://hv-bizbuddyv2-report.streamlit.app/"
    }
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    for page, url in PAGES.items():
        st.sidebar.markdown(f"[**{page}**]({url})", unsafe_allow_html=True)# Filtering Options

    # SECTION 1: Time-of-Day Analysis
    col_text, col_viz = st.columns([1, 2])  # Wider column for visuals

    with col_text:
        st.subheader("When Are Posts Performing Best?")
        st.markdown("🚧 Placeholder summary here. You can later insert auto-generated insights or your own write-up about post timing performance based on reach.")

    with col_viz:
        # Prepare data (already categorized earlier)
        timing_data = (
            merged_data.groupby("time_bucket")["reach"]
            .mean()
            .reset_index()
            .sort_values("time_bucket")
        )

        fig = px.bar(
            timing_data,
            x="time_bucket",
            y="reach",
            title="Average Reach by Time of Day",
            labels={"time_bucket": "Time of Day", "reach": "Average Reach"},
            template="plotly_white"
        )
        st.plotly_chart(fig)


if __name__ == "__main__":
    main()
