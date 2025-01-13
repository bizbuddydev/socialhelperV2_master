import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
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
    return df[df["created_time"] >= cutoff].sort_values(by="created_time", ascending=False)

def filter_last_6_months(df):
    cutoff = date.today() - timedelta(days=182)  # Approx. 6 months
    return df[df["created_time"] >= cutoff].sort_values(by="created_time", ascending=False)

def top_10_by_column(df, column):
    return df.sort_values(by=column, ascending=False).head(10)

# Use the variables in your app
account_name = config["ACCOUNT_NAME"]
datasetid = config["DATASET_ID"]
tableid = config["POST_TABLE_ID"]

### Get data ###
query = f"""
SELECT *
FROM `bizbuddydemo-v1.{datasetid}.{tableid}`
ORDER BY created_time DESC
"""

# Load/Transform Data
data = fetch_data(query)
data["Like Rate"] = round(data["like_count"]/data["reach"] * 100, 2)
data["created_time"] = pd.to_datetime(data["created_time"]).dt.date


# Main app
def main():
    # Add custom CSS for centering text
    st.markdown("""
    <style>
        .centered-title {
            text-align: center;
            font-size: 36px;
            font-weight: bold;
            margin-bottom: 10px;
        }
        .centered-header {
            text-align: center;
            font-size: 24px;
            margin-bottom: 20px;
        }
        .left-header {
            text-align: left;
            font-size: 20px;
            margin-bottom: 20px;
            font-style: italic;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Centered title
    st.markdown('<div class="centered-title">Social Buddy - Post Analyzer</div>', unsafe_allow_html=True)
    
    # Centered header
    st.markdown(f'<div class="centered-header">{account_name}</div>', unsafe_allow_html=True)

    # Centered header
    st.markdown(f'<div class="left-header">Filter Posts:</div>', unsafe_allow_html=True)

    # Add buttons for filtering options
    st.markdown('<div style="text-align: center;">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5, col6, col7 = st.columns([1, 1, 1, 1, 1, 1, 1])
    
    with col1:
        if st.button("Last 30 Days"):
            filtered_data = filter_last_30_days(data)
    
    with col2:
        if st.button("Last 6 Months"):
            filtered_data = filter_last_6_months(data)

    with col3:
        if st.button("All Time"):
            filtered_data = data
    
    with col4:
        if st.button("Top 10 by Reach"):
            filtered_data = top_10_by_column(data, "reach")
    
    with col5:
        if st.button("Top 10 by Likes"):
            filtered_data = top_10_by_column(data, "like_count")
    
    with col6:
        if st.button("Top 10 by Like Rate"):
            filtered_data = top_10_by_column(data, "Like Rate")
    
    with col7:
        if st.button("Top 10 by Comments"):
            filtered_data = top_10_by_column(data, "comments_count")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # If no filter is selected, display all data sorted by date
    if "filtered_data" not in locals():
        filtered_data = data.sort_values(by="created_time", ascending=False).head(25)
    
    st.markdown("""
    <style>
        .media {
            padding: 10px;
            background-color: #f9f9f9;
            border-radius: 10px;
            display: flex;
            justify-content: center;
        }
        .scorecards {
            margin-left: 20px;
            margin-right: 20px;
        }
        .caption {
            font-size: 16px;
            font-weight: bold;
            margin-bottom: 5px;
        }
        .details {
            font-size: 16px;
            font-weight: bold;
            margin-bottom: 10px;
            color: #555555;
        }
        .scorecard {
            display: inline-block;
            padding: 8px 16px;
            margin: 5px;
            background-color: #f3f3f3;
            border-radius: 8px;
            text-align: center;
            font-size: 14px;
            font-weight: bold;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Define a consistent media width
    MEDIA_WIDTH = 500

    st.markdown("---") 
    
    # Iterate through the top posts and display them
    for index, row in filtered_data.iterrows():
        # Create three columns for spacing and content
        spacer1, col1, col2, spacer2 = st.columns([0.5, 2, 1, 0.5])  # Adjust widths as needed
        
        with col1:
            # Display created_time
            st.markdown(f"<div class='details'>Posted On: {row['created_time']}</div>", unsafe_allow_html=True)
    
            # Display caption with title
            st.markdown(f"<div class='caption'>Caption: {row['caption']}</div>", unsafe_allow_html=True)
            
            # Display metrics in scorecards
            metrics_html = f"""
            <div class="scorecards">
                <div class="scorecard">Reach: {row['reach']}</div>
                <div class="scorecard">Likes: {row['like_count']}</div>
                <div class="scorecard">Like Rate: {row['Like Rate']}%</div>
                <div class="scorecard">Comments: {row['comments_count']}</div>
                <div class="scorecard">Saves: {row['saved']}</div>
            </div>
            """
            st.markdown(metrics_html, unsafe_allow_html=True)
        
        with col2:
            # Display media in a styled container
            st.markdown('<div class="media">', unsafe_allow_html=True)
            if row['media_type'] == 'IMAGE':
                st.image(row['source'], width=MEDIA_WIDTH)
            elif row['media_type'] == 'VIDEO':
                st.video(row['source'], start_time=0, format="video/mp4")
            st.markdown('</div>', unsafe_allow_html=True)
    
        st.markdown("---")  # Divider between posts

if __name__ == "__main__":
    main()
