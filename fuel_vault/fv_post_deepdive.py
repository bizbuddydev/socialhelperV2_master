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

    metric_option = st.selectbox("Select Metric", ["reach", "like_count"])

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

    # **Timing Analysis**
    col_left1, col_right1 = st.columns(2)

    with col_left1:
        st.subheader("Timing Analysis")

        dim_option = st.selectbox("Select Dimension", ["time_bucket", "weekday"])

        time_bucket_order = [
            "9 AM", "10 AM", "11 AM", "12 PM", "1 PM", "2 PM", "3 PM", "4 PM", "5 PM",
            "6 PM", "7 PM", "8 PM", "9 PM", "10 PM", "11 PM", "12 AM", "1-8 AM"
        ]
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        filtered_data["time_bucket"] = pd.Categorical(
            filtered_data["time_bucket"], categories=time_bucket_order, ordered=True
        )
        filtered_data["weekday"] = pd.Categorical(
            filtered_data["weekday"], categories=weekday_order, ordered=True
        )

        timing_analysis = filtered_data.groupby([dim_option]).agg({metric_option: "mean"}).reset_index()

        fig = px.bar(
            timing_analysis,
            x=dim_option,
            y=metric_option,
            title=f"{metric_option.replace('_', ' ').title()} by {dim_option}",
            labels={metric_option: "Average Value"},
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
            labels={video_metric: "Video Metric"},
            template="plotly_white"
        )
        fig_video.update_traces(marker=dict(size=10))
        st.plotly_chart(fig_video)

    # **Text Length vs Engagement**
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        fig_text_length = px.scatter(
            filtered_data,
            x="caption_length",
            y=metric_option,
            title="Text Length vs Engagement",
            labels={"caption_length": "Caption Length"},
            template="plotly_white"
        )
        st.plotly_chart(fig_text_length)

    with col_right2:
        cta_analysis = filtered_data.groupby("call_to_action").agg(
            {"reach": "mean", "like_count": "mean"}
        ).reset_index()

        fig_cta = px.bar(
            cta_analysis,
            x=metric_option,
            y="call_to_action",
            title="Effectiveness of Call-to-Action Phrases",
            labels={"call_to_action": "CTA Phrase"},
            template="plotly_white"
        )
        st.plotly_chart(fig_cta)

    # **Word Cloud for Speech & Captions**
    st.subheader("Terms from Speech/Captions")
    text_data = " ".join(filtered_data["processed_speech"].astype(str) + " " + filtered_data["caption"].astype(str))
    
    wordcloud = WordCloud(width=800, height=400, background_color="white", colormap="viridis").generate(text_data)
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # **Raw Data Table**
    st.subheader("Raw Data")
    st.dataframe(merged_data)

if __name__ == "__main__":
    main()
