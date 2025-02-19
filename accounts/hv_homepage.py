import streamlit as st
import openai
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import plotly.graph_objects as go
import json

st.set_page_config(page_title="Social Overview", layout="wide", page_icon="📊")

# Load the configuration file
def load_config(file_path="config.json"):
    with open(file_path, "r") as f:
        return json.load(f)

config = load_config()

# Set environment variables
ACCOUNT_NAME = "The Harborview"
PROJECT_ID = config["PROJECT_ID"]
DATASET_ID = config["DATASET_ID"]
ACCOUNT_TABLE_ID = config["ACCOUNT_TABLE_ID"]
POST_TABLE_ID = config["POST_TABLE_ID"]

# Load credentials and project ID from Streamlit secrets
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Pull account data from BigQuery
def pull_dataframes(dataset_id, table_id):
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_ref}`"
    
    try:
        query_job = client.query(query)
        result = query_job.result()
        return result.to_dataframe()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

# Function to display a metric card
def display_metric(label: str, value: str):
    st.markdown(
        f"""
        <div style="
            border: 2px solid #000000;
            border-radius: 10px;
            padding: 10px;
            margin: 7px;
            background-color: #f9f9f9;
            height: 100px;
            box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
        ">
            <h3 style="margin: 0; font-size: 16px; color: #333;">{label}</h3>
            <p style="margin: 5px 0 0 0; font-size: 18px; font-weight: bold;">{value}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Main function to display data and visuals
def main():
    st.markdown(f"<h1 style='text-align: center;'>{ACCOUNT_NAME}</h1>", unsafe_allow_html=True)

    # Pull account and post data
    account_data = pull_dataframes(DATASET_ID, ACCOUNT_TABLE_ID)
    post_data = pull_dataframes(DATASET_ID, POST_TABLE_ID)

    # Display KPI Performance
    top_col_left, top_col_right = st.columns(2)

    with top_col_left:
        st.subheader("KPI Performance")
        
        if account_data is not None and not account_data.empty:
            total_followers = account_data.iloc[-1]['total_followers']
        else:
            total_followers = 0

        total_posts = len(post_data) if post_data is not None else 0
        avg_reach = post_data['reach'].mean() if post_data is not None and not post_data.empty else 0
        avg_likes = post_data['like_count'].mean() if post_data is not None and not post_data.empty else 0

        # Display metric cards
        row1 = st.columns(4)
        with row1[0]:
            display_metric("Followers", f"{total_followers:,}")
        with row1[1]:
            display_metric("Posts", f"{total_posts:,}")
        with row1[2]:
            display_metric("Avg Reach", f"{avg_reach:,.2f}")
        with row1[3]:
            display_metric("Avg Likes", f"{avg_likes:.2f}")

    # Display Time-Series Chart
    mid_col_left, mid_col_right = st.columns(2)

    with mid_col_left:
        if account_data is not None and not account_data.empty:
            account_data['date'] = pd.to_datetime(account_data['date'])
            account_data = account_data.sort_values(by='date', ascending=True)
        
            # Aggregate duplicate dates before reindexing
            account_data = account_data.groupby('date', as_index=False).sum()
        
            # Create a complete date range
            full_date_range = pd.date_range(start=account_data['date'].min(), end=account_data['date'].max())
        
            # Reindex account_data
            account_data = account_data.set_index('date').reindex(full_date_range).reset_index()
            account_data.rename(columns={'index': 'date'}, inplace=True)
        
            # Fill missing values with forward fill
            selected_metric = "reach"
            account_data[selected_metric] = account_data[selected_metric].fillna(method='ffill')  

            # Initialize Plotly figure
            fig = go.Figure()
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
                    tickformat='%b %d',
                    tickangle=45
                ),
                yaxis=dict(title=selected_metric),
                title=f'{selected_metric} Over Time',
                plot_bgcolor='white',
                hovermode='x unified',
                showlegend=False
            )

            # Display visualization
            st.plotly_chart(fig)

# Run the app
if __name__ == "__main__":
    main()
