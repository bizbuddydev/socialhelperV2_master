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
    # Convert created_time to datetime if it's not already
    df["created_time_posts"] = pd.to_datetime(df["created_time_posts"])
    
    # Extract hour
    df["hour"] = df["created_time_posts"].dt.hour

    # Define bucket mapping
    def bucketize(hour):
        if 9 <= hour <= 11:
            return f"{hour} AM"
        elif hour == 12:
            return "12 PM"
        elif 13 <= hour <= 23:
            return f"{hour - 12} PM"
        elif hour == 0:
            return "12 AM"
        else:  # Covers 1 AM - 7 AM
            return "1-7 AM"

    # Assign time buckets
    df["time_bucket"] = df["hour"].apply(bucketize)

    # Define categorical ordering
    time_bucket_order = [
        "9 AM", "10 AM", "11 AM", "12 PM", "1 PM", "2 PM", "3 PM", "4 PM", "5 PM",
        "6 PM", "7 PM", "8 PM", "9 PM", "10 PM", "11 PM", "12 AM", "1-7 AM"
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
    st.title("Social Buddy 🚀 - Post Deep Dive")

    metric_option = st.selectbox("Select Metric", ["reach", "like_count"])
    
    # Define links to other pages
    PAGES = {
        "📊 Account Overview": "https://hv-bizbuddyv2-home.streamlit.app/",
        "📱 Posts Overview": "https://bizbuddyv2-hv-postoverview.streamlit.app/",
        "🔬 Posts Deepdive": "https://bizbuddyv2-hv-postdeepdive.streamlit.app/",
        "🗓️ Scheduler / Idea Generator": "https://bizbuddyv2-hv-postscheduler.streamlit.app/",
        "💡 Inspiration Upload": "https://hv-bizbuddyv2-inspiration.streamlit.app/"
    }
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    for page, url in PAGES.items():
        st.sidebar.markdown(f"[**{page}**]({url})", unsafe_allow_html=True)# Filtering Options

    filtered_data = merged_data
    
    col_left1, col_right1 = st.columns(2)
    
    with col_left1:
        st.subheader("Timing Analysis")
        
        dim_option = st.selectbox("Select Dimension", ["weekday", "time_bucket"])

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

    col_left2, col_right2 = st.columns(2)
    
    with col_left2:
        fig_text_length = px.scatter(
        filtered_data,
        x="caption_length",
        y= metric_option,
        title="Text Length vs Engagement",
        labels={"caption_length": "Caption Length", "reach": "Reach", "speech_length": "Speech Length"},
        template="plotly_white"
        )
        st.plotly_chart(fig_text_length)
    with col_right2:
        cta_analysis = filtered_data.groupby("call_to_action").agg({"reach": "mean", "like_count": "mean"}).reset_index()

        fig_cta = px.bar(
            cta_analysis,
            x=metric_option,
            y="call_to_action",
            title="Effectiveness of Call-to-Action Phrases",
            labels={"call_to_action": "CTA Phrase", "reach": "Average Reach"},
            template="plotly_white"
        )
        st.plotly_chart(fig_cta)

    col_left3, col_right3 = st.columns(2)
    
    with col_left3:
        fig_words = px.scatter(
        filtered_data,
        x="common_word_count",
        y="most_common_word",
        size="reach",
        color="theme_repetition",
        title="Impact of Word Choice on Engagement",
        labels={"most_common_word": "Most Common Word", "common_word_count": "Word Frequency"},
        template="plotly_white"
        )
        st.plotly_chart(fig_words)

    with col_right3:

        st.subheader("Terms from Speech/Captions")
        st.write("Word clouds show words used in a corpus of text with larger words appearing more often.")
        
        # Convert NaNs to empty strings before joining
        filtered_data["processed_speech"] = filtered_data["processed_speech"].fillna("")
        filtered_data["caption"] = filtered_data["caption"].fillna("")
        
        # Combine text from processed_speech and caption columns
        text_data = " ".join(filtered_data["processed_speech"].astype(str) + " " + filtered_data["caption"].astype(str)).strip()
        
        # Generate the word cloud
        wordcloud = WordCloud(width=800, height=400, background_color="white", colormap="viridis").generate(text_data)
        
        # Display in Streamlit
        st.subheader("Word Cloud of Speech & Captions")
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

    col_left4, col_right4 = st.columns(2)
    
    with col_left4:
        st.subheader("Polarity & Engagement Correlation")
        st.write("Polarity is a measure of how positive or negative text is.")
        fig_polarity = px.scatter(
            filtered_data,
            x="polarity",
            y=metric_option,
            title="Sentiment vs. Engagement",
            labels={"polarity": "Sentiment", metric_option: metric_option},
            template="plotly_white"
        )
        st.plotly_chart(fig_polarity)
    
    with col_right4:
        st.subheader("Opinionated vs. Factual Content")
        st.write("Polarity is a measure of how opinionated the text is.")
        fig_subjectivity = px.scatter(
            filtered_data,
            x="subjectivity",
            y=metric_option,
            title="Subjectivity vs. Engagement",
            labels={"subjectivity": "Subjectivity (0 = Factual, 1 = Opinionated)", "reach": "Reach"},
            template="plotly_white"
        )
        st.plotly_chart(fig_subjectivity)

    st.subheader("Raw Data")
    st.dataframe(merged_data)

if __name__ == "__main__":
    main()
