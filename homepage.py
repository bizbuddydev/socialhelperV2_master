import streamlit as st
from streamlit_calendar import calendar
import openai
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, date, timedelta
import json
# from styles import *

#For Viz
import plotly.graph_objects as go

st.set_page_config(page_title="Social Overview", layout="wide", page_icon="📊")

# Define links to other pages
PAGES = {
    "📊 Overview": "https://smp-bizbuddyv2-homepage.streamlit.app/",
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

# Set env variables
ACCOUNT_NAME = config["ACCOUNT_NAME"]
PROJECT_ID = config["PROJECT_ID"]
DATASET_ID = config["DATASET_ID"]
ACCOUNT_TABLE_ID = config["ACCOUNT_TABLE_ID"]
POST_TABLE_ID = config["POST_TABLE_ID"]
ACCOUNT_DATASET_ID = config["ACCOUNT_DATASET_ID"]
BUSINESS_TABLE_ID = config["BUSINESS_TABLE_ID"]
IDEAS_TABLE_ID = config["IDEAS_TABLE_ID"]
SUMMARY_TABLE_ID = config["SUMMARY_TABLE_ID"]
PAGE_ID = config["PAGE_ID"]


# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# OpenAI key
openai.api_key = st.secrets["openai"]["api_key"]

# Initialize OpenAI client
AI_client = openai

# Get Business Description
def pull_busdescritpion(dataset_id, table_id):
    
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    # Query to fetch all data from the table
    query = f"SELECT `Description of Business and Instagram Goals` FROM `{table_ref}` LIMIT 1"
    
    try:
        # Execute the query
        query_job = client.query(query)
        result = query_job.result()
        # Convert the result to a DataFrame
        data = result.to_dataframe()
        return data.iloc[0][0]
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

bus_description = pull_busdescritpion(ACCOUNT_DATASET_ID, BUSINESS_TABLE_ID)

# Get Post Idea Data
def pull_postideas(dataset_id, table_id):
    
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` LIMIT 3"
    
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

# Function to pull data from BigQuery
def pull_dataframes(table_id):
    
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"

    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}`"
    
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

# Function to pull data from BigQuery
def pull_accountsummary():
    
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{ACCOUNT_DATASET_ID}.{SUMMARY_TABLE_ID}"

    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` WHERE page_id = '{PAGE_ID}' ORDER BY date DESC LIMIT 1"
    
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

def get_daily_post_counts(post_data, account_data):
    # Ensure created_time is in datetime format
    post_data['date'] = pd.to_datetime(post_data['created_time'])
    account_data['date'] = pd.to_datetime(account_data['date']).dt.date

    # Generate the last 30 days as a date range
    yesterday = datetime.today() - timedelta(days=1)
    date_range = [yesterday - timedelta(days=i) for i in range(31)]
    date_range = sorted(date_range)  # Ensure dates are in ascending order

    # Initialize an empty list to store daily counts
    daily_counts = []

    # Count posts for each day
    for day in date_range:
        # Filter posts matching the current day
        day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day.replace(hour=23, minute=59, second=59, microsecond=999999)

        post_count = post_data[(post_data['created_time'] >= day_start) & 
                               (post_data['created_time'] <= day_end)].shape[0]
        daily_counts.append({
            'date': day.date(),
            'post_count': post_count
        })

    # Convert to DataFrame
    daily_post_counts_df = pd.DataFrame(daily_counts)

    # Merge with account_data on the Date column
    merged_df = pd.merge(account_data, daily_post_counts_df, how="left", on="date")

    return merged_df


def generate_ig_metrics(time_frame, account_data, post_data):    
    #Generate a DataFrame of Instagram metrics for a given time frame and the previous period.

    # Define date ranges
    today = datetime.today()
    current_period_start = today - timedelta(days=time_frame)
    previous_period_start = current_period_start - timedelta(days=time_frame)
    previous_period_end = current_period_start - timedelta(days=1)

    # Filter data for the current period
    current_account_data = account_data[
        (account_data['date'] >= current_period_start.date()) & 
        (account_data['date'] <= today.date())
    ]
    current_post_data = post_data[
        (post_data['created_time'] >= current_period_start) & 
        (post_data['created_time'] <= today)
    ]

    # Filter data for the previous period
    previous_account_data = account_data[
        (account_data['date'] >= previous_period_start.date()) & 
        (account_data['date'] <= previous_period_end.date())
    ]
    previous_post_data = post_data[
        (post_data['created_time'] >= previous_period_start) & 
        (post_data['created_time'] <= previous_period_end)
    ]

    # Calculate metrics for a given dataset
    def calculate_metrics(account_data, post_data):
        total_posts = len(post_data)
        followers_gained = account_data['follower_count'].sum() if 'follower_count' in account_data else 0
        total_reach = account_data['reach'].sum() if 'reach' in account_data else 0
        total_likes = post_data['like_count'].sum() if 'like_count' in post_data else 0
        total_comments = post_data['comments_count'].sum() if 'comments_count' in post_data else 0
        like_rate = total_likes / total_reach if total_reach > 0 else 0
        average_reach = total_reach / total_posts if total_posts > 0 else 0
        average_likes = total_likes / total_posts if total_posts > 0 else 0

        return {
            'Total Posts': total_posts,
            'Followers Gained': followers_gained,
            'Total Reach': total_reach,
            'Total Likes': total_likes,
            'Total Comments': total_comments,
            'Like Rate': like_rate,
            'Average Reach': average_reach,
            'Average Likes': average_likes,
        }

    # Create dataframes for current and previous periods
    current_metrics = calculate_metrics(current_account_data, current_post_data)
    previous_metrics = calculate_metrics(previous_account_data, previous_post_data)

    current_period_df = pd.DataFrame([current_metrics])
    previous_period_df = pd.DataFrame([previous_metrics])

    return current_period_df, previous_period_df

def calculate_percentage_diff_df(current_df, previous_df):
    
    #Calculate the percentage difference between two DataFrames.

    # Ensure the two DataFrames have the same structure
    if not current_df.columns.equals(previous_df.columns):
        raise ValueError("Both DataFrames must have the same columns.")

    # Convert all columns to numeric, coercing errors to NaN
    current_df = current_df.apply(pd.to_numeric, errors='coerce')
    previous_df = previous_df.apply(pd.to_numeric, errors='coerce')

    # Initialize an empty DataFrame for percentage differences
    percentage_diff_df = pd.DataFrame(columns=current_df.columns)

    # Calculate percentage differences for each column
    for column in current_df.columns:
        current_values = current_df[column]
        previous_values = previous_df[column]

        # Compute the percentage difference
        percentage_diff = []
        for current, previous in zip(current_values, previous_values):
            if pd.isna(current) or pd.isna(previous):
                diff = None  # Handle missing values
            elif current == previous:
                diff = 0  # Return 0 if the values are the same
            elif previous == 0:
                diff = None  # Handle division by zero (no valid percentage diff)
            else:
                diff = ((current - previous) / previous) * 100
                diff = round(diff, 2)  # Round to 2 decimal places
            percentage_diff.append(diff)

        # Add the percentage difference as a column
        percentage_diff_df[column] = percentage_diff

    return percentage_diff_df

def generate_static_summary(last_period_df, percentage_diff_df):
        
    #Generate a static summary string from the last period data and percentage differences.
    summary_lines = []

    for column in last_period_df.columns:
        # Get the last period value and percentage difference
        last_period_value = last_period_df[column].iloc[0]  # Assuming one row
        percentage_diff = percentage_diff_df[column].iloc[0]

        # Format the percentage difference with a "+" for positive values
        diff_string = f"{percentage_diff:+.2f}%" if percentage_diff is not None else "N/A"

        # Create a description line
        summary_lines.append(
            f"{column}: {last_period_value:,} ({diff_string} from the previous period)"
        )

    # Combine all lines into a single string
    return "\n".join(summary_lines)

def generate_gpt_summary(static_summary, business_description):

    #Generate a short performance summary using ChatGPT.
    # Create the prompt for ChatGPT
    prompt = (
        f"Here is the business context: {business_description}\n"
        f"Here is a summary of recent performance: {static_summary}\n"
        "Generate a concise two-sentence summary of the recent performance. Return this summary in bullets. The first sentence should describe overal perfromance and the next should be a set of suggestions centered around the idea that more posts will enhance the account and its engagement."
    )

    try:
        # Call ChatGPT using the updated syntax
        response = AI_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a social media manager specializing in providing actionable performance summaries. Use the summary of last weeks performance compared to the previous weeks performance."
                },
                {"role": "user", "content": prompt}
            ]
        )
        # Extract the content from the Choice object
        content = response.choices[0].message.content
        return content.strip()
    except Exception as e:
        return f"Error generating summary: {e}"

def split_bullet_points(response_text):
    #Split the ChatGPT response into two strings based on bullet points.
    
    # Split the response on the bullet point character (•)
    bullets = response_text.split('•')

    # Filter out empty strings caused by leading/trailing whitespace
    bullets = [bullet.strip() for bullet in bullets if bullet.strip()]

    # Return the two bullet points as separate strings
    if len(bullets) >= 2:
        return bullets[0], bullets[1]
    elif len(bullets) == 1:
        return bullets[0], ""
    else:
        return "", ""

# Add CSS for the metric card
st.markdown(
    """
    <style>
    .metric-card {
        border: 2px solid #000000; /* Green border */
        border-radius: 10px; /* Rounded corners */
        padding: 10px; /* Space inside the card */
        margin: 7px; /* Space around the card */
        background-color: #f9f9f9; /* Light background color */
        height: 115px; /* Adjusted height for percentage diff */
        box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1); /* Subtle shadow */
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        text-align: center;
    }
    .metric-card h3 {
        margin: 0;
        font-size: 16px;
        color: #333333; /* Dark text color */
    }
    .metric-card p {
        margin: 5px 0 0 0;
        font-size: 18px;
        color: #000000; /* Match the border color */
        font-weight: bold;
    }
    .metric-card .percentage-diff {
        font-size: 14px;
        font-weight: bold;
        margin-bottom: 5px;
    }
    .metric-card .percentage-diff.positive {
        color: #4CAF50; /* Green for positive values */
    }
    .metric-card .percentage-diff.negative {
        color: #FF5733; /* Red for negative values */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Function to display a metric card with an optional percentage difference
def display_metric(label: str, value: str, percentage_diff: float = None):
    # Determine the HTML for the percentage difference only if provided
    percentage_html = (
        f"<div class='percentage-diff {'positive' if percentage_diff > 0 else 'negative'}'>"
        f"{percentage_diff:+.2f}%</div>"
        if percentage_diff is not None
        else ""
    )
    
    # Render the metric card
    st.markdown(
        f"""
        <div class="metric-card">
            <h3>{label}</h3>
            <p>{value}</p>
            {percentage_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# Main function to display data and visuals
def main():

    st.markdown(f"<h1 style='text-align: center;'>{ACCOUNT_NAME}</h1>", unsafe_allow_html=True)

    # Pull data using the function
    account_data = pull_dataframes(ACCOUNT_TABLE_ID)
    post_data = pull_dataframes(POST_TABLE_ID)
    post_data = post_data.sort_values(by='created_time', ascending=True)


    # Get daily posts
    account_data = get_daily_post_counts(post_data, account_data)
    account_data = account_data.sort_values(by='date', ascending=True)

    #Get Post Metrics
    time_frame = 7
    l7_igmetrics, p7_igmetrics = generate_ig_metrics(time_frame, account_data, post_data)
    l7_perdiff = calculate_percentage_diff_df(l7_igmetrics, p7_igmetrics)

    # Generate summaries
    performance_summary = generate_static_summary(l7_igmetrics, l7_perdiff)

    #Get Scheduled Posts
    post_ideas = pull_postideas(ACCOUNT_DATASET_ID, IDEAS_TABLE_ID)
    
    # Create layout with two columns
    top_col_left, top_col_right = st.columns(2)

    with top_col_left:
        st.subheader("KPI Performance")
        st.write("All Time")
        # Columns for scorecards
        coll1, coll2, coll3, coll4 = st.columns(4) 
        
        # Calculate metrics
        if account_data is not None and not account_data.empty:
            total_followers = account_data.iloc[-1]['total_followers']  # Most recent day
        else:
            total_followers = 0

        total_posts = len(post_data) if post_data is not None else 0
        avg_reach = post_data['reach'].mean() if post_data is not None and not post_data.empty else 0
        avg_likes = post_data['like_count'].mean() if post_data is not None and not post_data.empty else 0

        
        # Layout for scorecards
        row1 = st.columns(4)
        
        # Row 1
        with row1[0]:
            display_metric("Followers", f"{total_followers:,}")
        
        with row1[1]:
            display_metric("Posts", f"{total_posts:,}")
        
        with row1[2]:
            display_metric("Avg Reach", f"{avg_reach:,.2f}")
            
        with row1[3]:
            display_metric("Avg Likes", f"{avg_likes:.2f}")
        

        st.write("Last 7 days")
         # Columns for scorecards
        coll5, coll6, coll7, coll8  = st.columns(4) 

        with coll5:
            display_metric("New Follows", f"{l7_igmetrics.iloc[0]["Followers Gained"]:,.0f}", l7_perdiff.iloc[0]["Followers Gained"])
        with coll6:
            display_metric("Posts", f"{l7_igmetrics.iloc[0]["Total Posts"]:,.0f}", l7_perdiff.iloc[0]["Total Posts"])
        with coll7:
            display_metric("Avg Reach", f"{l7_igmetrics.iloc[0]["Average Reach"]:,.2f}", l7_perdiff.iloc[0]["Average Reach"])
        with coll8:
            display_metric("Avg Likes", f"{l7_igmetrics.iloc[0]["Average Likes"]:,.2f}", l7_perdiff.iloc[0]["Average Likes"])
    

    with top_col_right:
        st.subheader("Account Insights from AI")
        account_summary_data = pull_accountsummary()
        account_summary = account_summary_data.iloc[0][1]
        #response_text = generate_gpt_summary(bus_description, performance_summary)
        bullet1, bullet2 = split_bullet_points(account_summary)
        st.write(bullet1)
        st.write(bullet2)
        
    ###Col info, bottom left
    bot_col_left, bot_col_right = st.columns(2)

    with bot_col_left:
        
        st.subheader("Performance Over Time")
                     
        account_data.rename(columns={"total_followers": "Total Followers", "follower_count" : "Followers Gained", "reach": "Reach", "impressions": "Impressions"}, inplace=True)
        
        # Dropdown for selecting metric
        metric_options = ['Total Followers', 'Followers Gained', 'Reach', 'Impressions']
        selected_metric = st.selectbox("Select metric for chart", metric_options)
        
        # Line chart for total followers over time using Plotly
        if account_data is not None and not account_data.empty:
            account_data['date'] = pd.to_datetime(account_data['date'])
            account_data = account_data.sort_values(by='date', ascending=True)
        
            # Create a complete date range from the first to the last day in account_data
            full_date_range = pd.date_range(start=account_data['date'].min(), end=account_data['date'].max())
        
            # Reindex account_data to include all dates in the range
            account_data = account_data.set_index('date').reindex(full_date_range).reset_index()
            account_data.rename(columns={'index': 'date'}, inplace=True)
        
            # Fill missing values for the selected metric with NaN or a default value
            account_data[selected_metric] = account_data[selected_metric].fillna(method='ffill')  # Example: forward-fill
        
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
        
            # Add vertical lines for each post date
            post_dates = pd.to_datetime(post_data['created_time']).dt.date.unique()  # Extract unique post dates
            post_dates = [pd.Timestamp(post_date) for post_date in post_dates if post_date >= account_data['date'].min().date()]  # Filter post dates
            
            for post_date in post_dates:
                fig.add_trace(go.Scatter(
                    x=[post_date, post_date],  # Draw a vertical line
                    y=[account_data[selected_metric].min(), account_data[selected_metric].max()],
                    mode='lines',
                    name='Post',
                    line=dict(color='gray', dash='dash'),
                    hoverinfo='text',
                    text=f"Post on {post_date.date()}"
                ))
        
            # Customize layout
            fig.update_layout(
                xaxis=dict(
                    title='Date',
                    title_font=dict(size=12),
                    tickformat='%b %d',  # Format ticks as "MMM DD"
                    tickangle=45
                ),
                yaxis=dict(title=selected_metric, title_font=dict(size=12)),
                title=f'{selected_metric} Over Time',
                title_font=dict(size=18, family='Arial', color='black'),
                plot_bgcolor='white',
                hovermode='x unified',
                showlegend=False  # Turn off the legend if desired
            )
        
            # Add gridlines for cleaner visuals
            fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
            fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
        
            # Display the Plotly figure in Streamlit
            st.plotly_chart(fig)

    with bot_col_right:
        # Only execute calendar logic in its own container
        if "calendar_events" not in st.session_state:
            st.session_state["calendar_events"] = [
                {"title": "Past Post", "start": "2025-01-01", "end": "2025-01-01", "color": "#FF6C6C"},
                {"title": "Upcoming Post", "start": "2025-01-13", "end": "2025-01-13", "color": "#f1c232"},
                {"title": "Upcoming Post", "start": "2025-01-16", "end": "2025-01-16", "color": "#f1c232"},
            ]
        
        # Create a container for the calendar widget
        calendar_container = st.container()
        
        with calendar_container:
            st.subheader("Upcoming Scheduled Posts")
            state = calendar(
                events=st.session_state["calendar_events"],
                options={
                    "headerToolbar": {
                        "left": "today prev,next",
                        "center": "title",
                        "right": "dayGridDay,dayGridWeek,dayGridMonth",
                    },
                    "initialDate": "2025-01-01",
                    "initialView": "dayGridMonth",
                    "editable": True,
                    "navLinks": True,
                    "selectable": True,
                },
                key="calendar",
            )
        
            # Update session state only when the calendar's state changes
            if state.get("eventsSet") and state["eventsSet"] != st.session_state["calendar_events"]:
                st.session_state["calendar_events"] = state["eventsSet"]
        

# Run the app
if __name__ == "__main__":
    main()
