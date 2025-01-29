import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta

st.set_page_config(page_title="Post Analyzer", layout="wide", page_icon="📱")

# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
project_id = st.secrets["gcp_service_account"]["project_id"]

# Function to fetch data from BigQuery
def fetch_data(query: str) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = client.query(query)
    result = query_job.result()
    return result.to_dataframe()

# Define queries
QUERY = '''
SELECT *
FROM `bizbuddydemo-v1.${datasetid}.${tableid}`
ORDER BY created_time DESC
'''

# Load/Transform Data
data = fetch_data(QUERY)
data["Like Rate"] = round(data["like_count"] / data["reach"] * 100, 2)
data["created_time"] = pd.to_datetime(data["created_time"]).dt.date

def main():
    st.title("Social Buddy - Post Analyzer")

    # Filtering Options
    st.sidebar.header("Filter Posts")
    filter_option = st.sidebar.selectbox("Select Timeframe", ["Last 30 Days", "Last 6 Months", "All Time"])

    if filter_option == "Last 30 Days":
        filtered_data = filter_last_30_days(data)
    elif filter_option == "Last 6 Months":
        filtered_data = filter_last_6_months(data)
    else:
        filtered_data = data

    # Display filtered data
    st.subheader("Filtered Data")
    st.dataframe(filtered_data.head(10))

    # Analysis - Post Timing Recommendations
    st.subheader("Post Timing Recommendations")
    timing_analysis = filtered_data.groupby(["weekday", "time_of_day"]).agg({"reach": "mean", "like_count": "mean"}).reset_index()
    st.dataframe(timing_analysis)

    # Analysis - Sound Type & Engagement
    st.subheader("Sound Type & Engagement")
    sound_analysis = filtered_data.groupby("sound_type").agg({"reach": "mean", "like_count": "mean"}).reset_index()
    st.dataframe(sound_analysis)


if __name__ == "__main__":
    main()
