import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import json

st.set_page_config(page_title="Post Analyzer 🚀", layout="wide", page_icon="📱")

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
WHERE page_id = 17841467554159158
ORDER BY created_time DESC
"""

# Load/Transform Data
data = fetch_data(query)
data['post_date'] = data['created_time'].dt.date

def main():
    st.title("Social Buddy 🚀 - Post Deep Dive")

    metric_option = st.selectbox("Select Metric", ["reach", "like_count"])
    
    # Filtering Options
    st.sidebar.header("Filter Posts")
    filter_option = st.sidebar.selectbox("Select Timeframe", ["All Time", "Last 30 Days", "Last 6 Months"])

    if filter_option == "Last 30 Days":
        filtered_data = filter_last_30_days(data)
    elif filter_option == "Last 6 Months":
        filtered_data = filter_last_6_months(data)
    else:
        filtered_data = data
    
    col_left1, col_right1 = st.columns(2)
    
    with col_left1:
        st.subheader("Timing Analysis")
        
        dim_option = st.selectbox("Select Dimension", ["time_bucket", "weekday"])

        time_bucket_order = ["9 AM", "10 AM", "11 AM", "12 PM", "1 PM", "2 PM", "3 PM", "4 PM", "5 PM", "6 PM", "7 PM", "8 PM", "9 PM", "10 PM", "11 PM", "12 AM", "1-8 AM"]
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        filtered_data["time_bucket"] = pd.Categorical(filtered_data["time_bucket"], categories=time_bucket_order, ordered=True)
        filtered_data["weekday"] = pd.Categorical(filtered_data["weekday"], categories=weekday_order, ordered=True)

        timing_analysis = filtered_data.groupby([dim_option]).agg({metric_option: "mean"}).reset_index()
        
        fig = px.bar(
            timing_analysis,
            x=dim_option,
            y=metric_option,
            title=f"{metric_option.replace('_', ' ').title()} by Time Bucket",
            labels={metric_option: "Average Value", "dim_option": "Time"},
            template="plotly_white"
        )
        
        st.plotly_chart(fig)
    
    with col_right1:
        st.subheader("Video Structure Optimization")
        video_metric = st.selectbox("Select Video Metric", ["avg_shot_len", "shot_count", "video_len"])
        
        video_analysis = filtered_data.groupby(video_metric).agg({"reach": "mean", "like_count": "mean"}).reset_index()
        
        fig_video = px.scatter(
            video_analysis,
            x=video_metric,
            y=metric_option,
            title=f"{video_metric.replace('_', ' ').title()} vs Engagement",
            labels={video_metric: "Video Metric", "reach": "Average Reach", "like_count": "Average Likes"},
            template="plotly_white"
        )

        # Set the marker size statically
        fig_video.update_traces(marker=dict(size=10))
        
        st.plotly_chart(fig_video)

    col_left2, col_mid2, col_right2 = st.columns(3)
    
    with col_left2:
        fig_text_length = px.scatter(
        filtered_data,
        x="caption_length",
        y="reach",
        size="like_count",
        color="speech_length",
        title="Text Length vs Engagement",
        labels={"caption_length": "Caption Length", "reach": "Reach", "speech_length": "Speech Length"},
        template="plotly_white"
        )
        st.plotly_chart(fig_text_length)
    with col_mid2:
        cta_analysis = filtered_data.groupby("call_to_action").agg({"reach": "mean", "like_count": "mean"}).reset_index()

        fig_cta = px.bar(
            cta_analysis,
            x="call_to_action",
            y="reach",
            title="Effectiveness of Call-to-Action Phrases",
            labels={"call_to_action": "CTA Phrase", "reach": "Average Reach"},
            template="plotly_white"
        )
        st.plotly_chart(fig_cta)
    with col_right2:
        fig_words = px.scatter(
        filtered_data,
        x="most_common_word",
        y="common_word_count",
        size="reach",
        color="theme_repetition",
        title="Impact of Word Choice on Engagement",
        labels={"most_common_word": "Most Common Word", "common_word_count": "Word Frequency"},
        template="plotly_white"
        )
        st.plotly_chart(fig_words)

if __name__ == "__main__":
    main()
