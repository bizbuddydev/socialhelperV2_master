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

import plotly.graph_objects as go

st.set_page_config(page_title="Social Media Report 🚀", layout="wide", page_icon="📝")

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

client = bigquery.Client(credentials=credentials, project=project_id)

# Function to fetch data from BigQuery
@st.cache_data
def fetch_data(query: str) -> pd.DataFrame:
    query_job = client.query(query)  # Execute query
    result = query_job.result()  # Wait for the query to finish
    return result.to_dataframe()

DEMOGRAPHIC_TABLE_ID = config["DEMOGRAPHIC_TABLE_ID"]
DATASET_ID = config["DATASET_ID"]
PAGE_ID = 17841410640947509
PROJECT_ID = config["PROJECT_ID"]
ACCOUNT_TABLE_ID = config["ACCOUNT_TABLE_ID"]

# Function to pull data from BigQuery
def pull_dataframes(dataset_id, table_id):
    
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` WHERE page_id = {PAGE_ID}"
    
    try:
        # Execute the query
        query_job = client.query(query)
        result = query_job.result()
        # Convert the result to a DataFrame
        data = result.to_dataframe()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

#Get demographic data
demo_data = pull_dataframes(DATASET_ID, DEMOGRAPHIC_TABLE_ID)
#st.write(demo_data)


# Function to plot pie chart using Plotly
def plot_pie_chart(breakdown, df):
    filtered_df = df[df['breakdown'] == breakdown]
    aggregated_df = filtered_df.groupby('value')['followers'].sum().reset_index()

    # Create pie chart using Plotly
    fig = go.Figure(
        data=[
            go.Pie(
                labels=aggregated_df['value'],
                values=aggregated_df['followers'],
                hole=0.3,  # Use 0 for a full pie chart, >0 for a donut chart
                marker=dict(colors=['#636EFA', '#EF553B', '#00CC96', '#AB63FA']),
                textinfo='label+percent'
            )
        ]
    )
    fig.update_layout(
        title_text=f"Distribution of Followers by {breakdown}",
        legend_title="Categories",
        margin=dict(l=20, r=20, t=50, b=20),
    )

    st.plotly_chart(fig)

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
    st.title("Social Buddy 🚀 - Harborview Social Report")

    # Define links to other pages
    PAGES = {
        "📊 Account Overview": "https://hv-bizbuddyv2-home.streamlit.app/",
        "📱 Posts Overview": "https://bizbuddyv2-hv-postoverview.streamlit.app/",
        "🔬 Posts Deepdive": "https://bizbuddyv2-hv-postdeepdive.streamlit.app/",
        "🗓️ Scheduler / Idea Generator": "https://bizbuddyv2-hv-postscheduler.streamlit.app/",
        "💡 Inspiration Upload": "https://hv-bizbuddyv2-inspiration.streamlit.app/",
        "📝 Report" : "https://hv-bizbuddyv2-report.streamlit.app/"
    }
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    for page, url in PAGES.items():
        st.sidebar.markdown(f"[**{page}**]({url})", unsafe_allow_html=True)# Filtering Options

    #Pull account data
    account_data = pull_dataframes(DATASET_ID, ACCOUNT_TABLE_ID)

    account_data = account_data.drop_duplicates()
    account_data = account_data.sort_values(by='date', ascending=True)
                     
    account_data.rename(columns={"total_followers": "Total Followers", "follower_count" : "Followers Gained", "reach": "Reach", "impressions": "Impressions"}, inplace=True)
    
    # Hardcoded metric
    selected_metric = 'Total Followers'

    col_sum, col_follows = st.columns(2)

    with col_sum:
        st.subheader("Account Overview")
        st.write("Your account has grown significantly over the last 90 days, posting around 30 times and gaining 1,500+ followers (a 150% increase).")
        st.write("Here are the highlights:")
        st.markdown("""
        - Your account has turned a corner, with posts receiving a 10x increase in average engagement.
        - A new style has emerged, using reels to deliver informative and engaging content that resonates with your audience.
        - New followers bring new customer demographics for your business to reach.
        """)
        st.write("Below is a detailed analysis of your account data, which we'll use to refine your social strategy and convert engagement into customers.")
    with col_follows:
        # Line chart for total followers over time using Plotly
        if account_data is not None and not account_data.empty:
            account_data['date'] = pd.to_datetime(account_data['date'])
            account_data = account_data.sort_values(by='date', ascending=True)
        
            # Create a complete date range from the first to the last day in account_data
            full_date_range = pd.date_range(start=account_data['date'].min(), end=account_data['date'].max())
        
            # Reindex account_data to include all dates in the range
            account_data = account_data.set_index('date').reindex(full_date_range).reset_index()
            account_data.rename(columns={'index': 'date'}, inplace=True)
        
            # Fill missing values for the selected metric
            account_data[selected_metric] = account_data[selected_metric].fillna(method='ffill')
        
            # Initialize a Plotly figure
            fig = go.Figure()
        
            # Add the main line chart for the selected metric
            fig.add_trace(go.Scatter(
                x=account_data['date'],
                y=account_data[selected_metric],
                mode='lines',
                name=selected_metric,
                line=dict(color='royalblue', width=2)
            ))
        
            # Customize layout
            fig.update_layout(
                xaxis=dict(
                    title='Date',
                    title_font=dict(size=12),
                    tickformat='%b %d',
                    tickangle=45
                ),
                yaxis=dict(title=selected_metric, title_font=dict(size=12)),
                title=f'{selected_metric} Over Time',
                title_font=dict(size=18, family='Arial'),
                hovermode='x unified',
                showlegend=False
            )
        
            # Add gridlines
            fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
            fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
        
            # Display the figure
            st.plotly_chart(fig)

    st.divider()

    ### What are we posting section ###
    st.subheader(f"*What* are we posting?")
    st.write("Let's take a look at your recent content to see what types of posts are working, which aren't, and where there may be untapped opportunities.")
    st.markdown("""
    - Content: Most posts focus on the appeal of Port Washington, where the Harborview is located. Other content highlights hotel facilities with visuals of rooms, dining areas, and common spaces.
    - Themes: Posts generally carry a positive tone, encouraging visitors to use the Harborview as a weekend escape from nearby cities. Other themes include collaboration and giveaways, which drive more direct engagement from users.
    """)
    st.write("Now let’s dive into the data to see how this content is performing.")        

    with st.expander("See Full Content Analysis"):
        # SECTION: Theme Performance
        col_theme, col_wc = st.columns(2)
    
        with col_theme:
    
            theme_data = (
                merged_data.groupby("main_theme")["reach"]
                .mean()
                .reset_index()
                .sort_values("reach", ascending=False)
            )
    
            fig_theme = px.bar(
                theme_data,
                x="reach",
                y="main_theme",
                title="Average Reach by Theme",
                orientation="h",
                labels={"theme": "Theme", "reach": "Average Reach"},
                template="plotly_white"
            )
    
            st.plotly_chart(fig_theme)

        with col_wc:
            st.write("Most Commonly Used Words in Posts")
            # Preprocess: fill in NaNs
            merged_data["processed_speech"] = merged_data["processed_speech"].fillna("")
            merged_data["caption"] = merged_data["caption"].fillna("")
    
            # Combine both text sources
            text_blob = " ".join(merged_data["processed_speech"].astype(str) + " " + merged_data["caption"].astype(str)).strip()
    
            # Generate word cloud
            wordcloud = WordCloud(width=800, height=400, background_color="white", colormap="viridis").generate(text_blob)
    
            # Display word cloud
            fig_wc, ax_wc = plt.subplots(figsize=(10, 5))
            ax_wc.imshow(wordcloud, interpolation="bilinear")
            ax_wc.axis("off")
            st.pyplot(fig_wc)
        st.markdown("Explanation:")
        st.markdown("""
                    - Left: shows post themes (a summary of what you are posting about) and their respective performance by average reach.
                    - Right: shows the repition of words and nouns with their degree of repition shown by size.
                    """)
    st.divider()

    
    ### How we are posting section ###
    st.subheader(f"*How* are we posting?")
    st.write("Now let's take a look at how we're posting and how it's performing.")
    st.markdown("""
    - Format: Most recent posts are multi-shot reels featuring the hotel and surrounding areas.
    - Visuals: The primary visual focus is the hotel, with recent content using drone footage. Videos typically start with the Port Washington bay and transition into hotel accommodations and amenities.
    - Timing: Posts go out at various times of day and across different days of the week. While no clear pattern appears by day, time of day is emerging as a key factor in performance.
    """)
    st.write("Let’s take a deeper look at the data to evaluate the effectiveness of these methods.")


    with st.expander("See Full Visual Analysis"):
        col_time, col_visuals = st.columns(2)  # Wider column for visuals

        with col_time:
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
    
        with col_visuals:
            visualfocus_data = (
                merged_data.groupby("main_focus")["reach"]
                .mean()
                .reset_index()
                .sort_values("reach", ascending=True)
            )
    
            fig_visualfocus = px.bar(
                visualfocus_data,
                x="reach",
                y="main_focus",
                orientation="h",
                title="Average Reach by Primary Visual",
                labels={"main_focus": "Primary Visual", "reach": "Average Reach"},
                template="plotly_white"
            )
            st.plotly_chart(fig_visualfocus)

        st.markdown("Explanation:")
        st.markdown("""
                    - Left: shows average reach for posts grouped the time of day that they were posted.
                    - Right: shows aveage reach for primary visuals for each post (the main imagery in each posts shown in the first 3 seconds of a video.
                    """)
    
    
        # SECTION 2: CTA Analysis
        col_shotlen, col_objcount = st.columns(2)
    
        with col_shotlen:
            video_analysis = merged_data.groupby("video_len").agg({"reach": "mean", "like_count": "mean"}).reset_index()
            
            fig_video = px.scatter(
            video_analysis,
            x="video_len",
            y="reach",
            title="Video Length vs Engagement",
            labels={"video_len": "Video Length", "reach": "Average Reach"},
            template="plotly_white"
            )

            # Set the marker size statically
            fig_video.update_traces(marker=dict(size=10))
            
            st.plotly_chart(fig_video)
    
        with col_objcount:
            object_analysis = merged_data.groupby("object_count").agg({"reach": "mean", "like_count": "mean"}).reset_index()
            
            fig_obj = px.scatter(
            object_analysis,
            x="object_count",
            y="reach",
            title="Object Count vs Engagement",
            labels={"object_count": "Object Count", "reach": "Average Reach"},
            template="plotly_white"
            )

            # Set the marker size statically
            fig_obj.update_traces(marker=dict(size=10))
            
            st.plotly_chart(fig_obj)

        st.markdown("Explanation:")
        st.markdown("""
                    - Left: shows average reach by the video length.
                    - Right: shows average reach by the object count (often used as a measure of how busy a video is).
                    """)
    st.divider()
    
    ### What are we posting section ###
    st.subheader(f"*Who* is seeing our posts?")
    st.write("Finally, let's see who’s following your account:")
    st.markdown("""
    - Age/Gender: The majority of your followers are female (about two-thirds of your follower base), primarily between the ages of 25–55.
    - Locations: Most followers are from Wisconsin, with a smaller segment from Illinois—primarily Chicago. Within Wisconsin, many are from Milwaukee, followed by Port Madison, and smaller surrounding towns.
    """)
    st.write("Let’s take a deeper look at the data to see exactly who’s following you.")


    with st.expander("See Full Demographic Analysis"):
        col_text, col_demo = st.columns(2)  # Wider column for visuals

        with col_text:
            # Prepare data (already categorized earlier)
            st.subheader("Demographic Breakdown")
            st.write("Use the dropdown menu to change demograpahic groups")
    
        with col_demo:
            # Dropdown for selecting breakdown
            selected_breakdown = st.selectbox("Select Breakdown", demo_data['breakdown'].unique())
    
            # Display the pie chart based on selected breakdown
            plot_pie_chart(selected_breakdown, demo_data)

    st.divider()

    st.subheader("Recommendations from Analysis")
    st.write("Your account is gaining serious momentum, creating strong potential for your business. Using insights from BizBuddy, you can refine your content and strategy to drive conversions.")
    
    #Content Analysis
    st.write("Content:")
    st.markdown("""
    - You’ve had great success posting content about the appeal of Port Washington, giving followers another reason to book a stay at the Harborview.
    - Some content and themes are becoming repetitive. You don’t need to stray far from your core message, but try presenting it in new ways. Audiences respond better when the same idea is delivered through varied messaging.
    - Use the word cloud to identify which words and themes are being used most often.
    - Your most successful posts are those that directly engage users (e.g., free room giveaways). Use rewards to drive interaction, then follow up with posts that highlight the benefits of staying.
    - This strategy works well in advertising, and with Instagram, it's free—or very affordable if boosted.
    """)
    
    #Style Analysis
    st.write("Style:")
    st.markdown("""
    - A clear insight from the data: posting later in the day leads to greater reach via Instagram’s algorithm. This may be due to user behavior or less competition in evening hours.
    - Picture your target user finishing their day and scrolling in the evening—this is your window. Use it.
    - Users are responding well to imagery featuring the marina and water in Port Madison. Lean into this visual theme.
    - Consider mixing in nature shots like parks and hikes from nearby areas. Start with nature to draw attention, then transition to hotel amenities to position the Harborview as a peaceful escape.
    - For shot style, longer videos with more visual elements tend to perform better. Aim for 20–30 seconds—long enough to offer substance, short enough to keep attention.
    """)
    
    #Demographic Analysis
    st.write("Demographic:")
    st.markdown("""
    - Your primary audience is women aged 25–54 from Milwaukee and Chicago.
    - These users are likely looking for a nearby getaway—using the Harborview as a home base for escaping the city and enjoying peaceful Port Washington.
    - Now that we have a defined customer profile, tailor your content to convert followers into bookings. Show families at the hotel or couples enjoying a quiet dinner by the water.
    - You’re also missing certain customer types. Use Instagram as a top-of-funnel tool to reach new audiences—business travelers, solo adventurers, or boaters exploring Lake Michigan could all be targeted.
    """)

    st.write("Summary:")
    st.write("Your content strategy is resonating well when emphasizing the appeal of Port Washington, particularly with engaging posts like giveaways, but risks becoming repetitive—so focus on refreshing the message while keeping the theme consistent. To improve performance, post later in the day, lean into marina and nature imagery, and tailor content toward your primary demographic of women aged 25–54 from nearby cities, while exploring new audiences like solo travelers or business visitors.")
    
    st.divider()

    #Appendix see the rest of the posts
    with st.expander("See More Visuals"):

        # WORD CHOICE IMPACT
        col_text4, col_viz4 = st.columns([1, 2])
        with col_text4:
            st.subheader("Impact of Word Choice")
            st.markdown("Analyze how the most common words and their frequency relate to engagement metrics.")

        with col_viz4:
            fig_words = px.scatter(
                merged_data,
                x="common_word_count",
                y="most_common_word",
                size="reach",
                color="theme_repetition",
                title="Impact of Word Choice on Engagement",
                labels={"most_common_word": "Most Common Word", "common_word_count": "Word Frequency"},
                template="plotly_white"
            )
            st.plotly_chart(fig_words)

        # SHOT COUNT ANALYSIS
        col_text5, col_viz5 = st.columns([1, 2])
        with col_text5:
            st.subheader("Shot Count and Performance")
            st.markdown("Explore how the number of shots in a video correlates with post performance.")

        with col_viz5:
            shot_data = (
                merged_data.groupby("shot_count")["reach"]
                .mean()
                .reset_index()
                .sort_values("shot_count")
            )
            fig_shot = px.bar(
                shot_data,
                x="shot_count",
                y="reach",
                title="Average Reach by Shot Count",
                labels={"shot_count": "Shot Count", "reach": "Average Reach"},
                template="plotly_white"
            )
            st.plotly_chart(fig_shot)

        # POLARITY ANALYSIS
        col_text6, col_viz6 = st.columns([1, 2])
        with col_text6:
            st.subheader("Polarity and Engagement")
            st.markdown("Investigate how positive or negative language affects reach.")

        with col_viz6:
            polarity_data = (
                merged_data.groupby("polarity")["reach"]
                .mean()
                .reset_index()
                .sort_values("polarity")
            )
            fig_polarity = px.bar(
                polarity_data,
                x="polarity",
                y="reach",
                title="Average Reach by Polarity",
                labels={"polarity": "Polarity (Negative to Positive)", "reach": "Average Reach"},
                template="plotly_white"
            )
            st.plotly_chart(fig_polarity)


if __name__ == "__main__":
    main()
