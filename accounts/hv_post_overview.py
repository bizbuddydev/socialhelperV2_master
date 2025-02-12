import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import json

st.set_page_config(page_title="Post Analyzer", layout="wide", page_icon="📱")

# Define links to other pages
PAGES = {
    "📊 Overview": "https://smp-bizbuddyv2-homepage.streamlit.app/",
    "📱 Posts": "https://smp-bizbuddyv2-postoverview.streamlit.app/",
    # "🗓️ Scheduler": "https://smp-bizbuddy-postscheduler.streamlit.app/",
    "📡 Deep Dive": "https://bizbuddy-postdd-smp.streamlit.app/",
    "🚝 Inspiration Upload": "https://smp-bizbuddyv2-inspoupload.streamlit.app/",
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
    return df[df["created_time_posts"] >= cutoff].sort_values(by="created_time_posts", ascending=False)

def filter_last_6_months(df):
    cutoff = date.today() - timedelta(days=182)  # Approx. 6 months
    return df[df["created_time_posts"] >= cutoff].sort_values(by="created_time_posts", ascending=False)

def top_10_by_column(df, column):
    return df.sort_values(by=column, ascending=False).head(10)

# Use the variables in your app
account_name = "The Harborview"
datasetid = config["DATASET_ID"]
tableid = config["POST_TABLE_ID"]

### Get data ###
query = f"""
SELECT *
FROM `bizbuddydemo-v2.{datasetid}.{tableid}`
WHERE page_id = 17841467554159158
AND DATE(insert_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY created_time DESC
"""

testing_dataset = config["TESTING_DATASET_ID"]
testing_table_id = config["ANALYSIS_TABLE_ID"]

### Get data ###
ap_query = f"""
SELECT *
FROM `bizbuddydemo-v2.{testing_dataset}.{testing_table_id}`
WHERE page_id = 17841410640947509
ORDER BY created_time DESC
"""

# Load/Transform Data
data = fetch_data(query)
data["Like Rate"] = round(data["like_count"]/data["reach"] * 100, 2)
data["created_time"] = pd.to_datetime(data["created_time"]).dt.date

# Get analyzed posts data and merge
ap_data = fetch_data(ap_query)

merged_data = data.merge(ap_data, left_on="post_id", right_on="video_id", how="left", suffixes=("_posts","_aps"))

merged_data["speech_rate"] = round(merged_data["speech_rate"], 2)

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
            filtered_data = filter_last_30_days(merged_data)
    
    with col2:
        if st.button("Last 6 Months"):
            filtered_data = filter_last_6_months(merged_data)

    with col3:
        if st.button("All Time"):
            filtered_data = merged_data
    
    with col4:
        if st.button("Top 10 by Reach"):
            filtered_data = top_10_by_column(merged_data, "reach")
    
    with col5:
        if st.button("Top 10 by Likes"):
            filtered_data = top_10_by_column(merged_data, "like_count")
    
    with col6:
        if st.button("Top 10 by Like Rate"):
            filtered_data = top_10_by_column(merged_data, "Like Rate")
    
    with col7:
        if st.button("Top 10 by Comments"):
            filtered_data = top_10_by_column(merged_data, "comments_count")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # If no filter is selected, display all data sorted by date
    if "filtered_data" not in locals():
        filtered_data = merged_data.sort_values(by="created_time_posts", ascending=False).head(25)
    
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
        }
        .scorecard {
            display: inline-block;
            padding: 8px 16px;
            margin: 5px;
            border-radius: 8px;
            text-align: center;
            font-size: 14px;
            font-weight: bold;
            background-color: #808080;
            color: #000000;
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
            # Display created_time_posts
            st.markdown(f"<div class='details'>Posted On: {row['created_time_posts']}</div>", unsafe_allow_html=True)
    
            with st.expander("See post caption"):
                # Display caption with title
                st.markdown(f"<div class='caption'>Caption: {row['caption_posts']}</div>", unsafe_allow_html=True)
            
            # Display metrics in scorecards
            metrics_html = f"""
            <div class="scorecards">
                <div class="scorecard">Reach: {row['reach_posts']}</div>
                <div class="scorecard">Likes: {row['like_count_posts']}</div>
                <div class="scorecard">Like Rate: {row['Like Rate']}%</div>
                <div class="scorecard">Comments: {row['comments_count_posts']}</div>
                <div class="scorecard">Saves: {row['saved_posts']}</div>
            </div>
            """
            st.write("Performance Metrics:")
            st.markdown(metrics_html, unsafe_allow_html=True)

            # Display post info in scorecards
            basic_info_html = f"""
            <div class="scorecards">
                <div class="scorecard">Theme: {row['main_theme']}</div>
                <div class="scorecard">Most Common Word: {row['most_common_word']}</div>
                <div class="scorecard">Initial Imagery: {row['main_focus']}</div>
                <div class="scorecard">Intial Color Schemes: {row['color_scheme']}</div>
            </div>
            """

            # Display post info in scorecards
            post_structure_html = f"""
            <div class="scorecards">
                <div class="scorecard">Time of Day: {row['time_bucket']}</div>
                <div class="scorecard">Weekday: {row['weekday']}</div>
                <div class="scorecard">Speech Length: {row['speech_length']} words</div>
                <div class="scorecard">Speech Rate: {row['speech_rate']} words per sec</div>
            </div>
            """

            st.write("Post Attributes:")
            st.markdown(basic_info_html, unsafe_allow_html=True)

            st.write("Post Structure:")
            st.markdown(post_structure_html, unsafe_allow_html=True)
        
        with col2:
            # Display media in a styled container
            try:
                st.markdown('<div class="media">', unsafe_allow_html=True)
                if row['media_type'] == 'IMAGE':
                    st.image(row['source'], width=MEDIA_WIDTH)
                elif row['media_type'] == 'VIDEO':
                    st.video(row['source'], start_time=0, format="video/mp4")
                st.markdown('</div>', unsafe_allow_html=True)
            except Exception as e:
                st.warning(f"Skipping post due to media error: {str(e)}")
        st.markdown("---")  # Divider between posts

if __name__ == "__main__":
    main()
