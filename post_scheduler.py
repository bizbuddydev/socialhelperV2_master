import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
import openai
import json

st.set_page_config(page_title="Post Scheduler", layout="wide", page_icon = "🗓️")

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

# Set env variables
ACCOUNT_NAME = config["ACCOUNT_NAME"]
PROJECT_ID = config["PROJECT_ID"]
ACCOUNT_DATASET_ID = config["ACCOUNT_DATASET_ID"]
IDEAS_TABLE_ID = config["IDEAS_TABLE_ID"]

# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Load BQ Client
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

openai.api_key = st.secrets["openai"]["api_key"]

# Initialize OpenAI client
client = openai


# Function to fetch the latest date and calculate the next post date
def fetch_latest_date():
    """
    Fetch the latest date from the smp_postideas table and return 3 days after it.

    Returns:
        datetime: The calculated next post date.
    """
    query = """
        SELECT MAX(date) as latest_date
        FROM `bizbuddydemo-v1.strategy_data.smp_postideas`
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    latest_date = result.to_dataframe().iloc[0]["latest_date"]
    return latest_date + timedelta(days=3)

# Function to generate a single post idea
def generate_post_idea(strategy):
    """
    Generate a single post idea using the provided strategy.

    Args:
        strategy (dict): A dictionary containing the social media strategy.

    Returns:
        pd.DataFrame: A dataframe containing the generated post idea.
    """
    prompt = (
        f"Based on this social media strategy: {strategy}, generate 1 post idea. "
        "Each idea should include the post Date, caption content, post type (e.g., Reel, Story, Static Post), "
        "themes (from the strategy), and tone. Ensure that the returned JSON object only has these columns with the exact names: 'Date', 'caption', 'post_type', 'themes', 'tone', 'source'. Ensure the idea aligns with the strategy and introduces a mix of concepts. "
        "Format as a JSON object."
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a social media manager with expertise in creating engaging content."},
            {"role": "user", "content": prompt}
        ]
    )

    idea_json = response.choices[0].message.content.strip()

    # Convert the JSON idea to a DataFrame
    idea_df = pd.read_json(idea_json, typ="series").to_frame().T

    # Assign a date to the post
    idea_df["Date"] = fetch_latest_date()

    return idea_df

# Function to manually add a post idea in the Streamlit app
def manually_add_post():
    """
    Allow the user to manually input data for a post idea.
    """
    st.subheader("Manually Add Post")

    # Input fields for the post
    date = st.date_input("Date", datetime.now())
    caption = st.text_area("Caption")
    post_type = st.selectbox("Post Type", ["Reel", "Story", "Static Post"])
    themes = st.text_area("Themes (comma-separated)")
    tone = st.text_area("Tone")
    source = "User"

    if st.button("Add Post", key="manual_add_post"):
        # Create a DataFrame for the new post
        post_df = pd.DataFrame({
            "date": [date],
            "caption": [caption],
            "post_type": [post_type],
            "themes": [themes.split(",")],
            "tone": [tone],
            "source": [source]
        })

        # Add the post to BigQuery
        try:
            add_post_to_bigquery(post_df)
            st.success("Post successfully added!")
        except Exception as e:
            st.error(f"Failed to add post: {e}")

# Function to add a row to the smp_postideas table in BigQuery
def add_post_to_bigquery(post_df):
    """
    Add a generated post idea to the smp_postideas table in BigQuery.

    Args:
        post_df (pd.DataFrame): The dataframe containing the post idea to be added.
    """
    table_id = "bizbuddydemo-v1.strategy_data.smp_postideas"

    # Convert list-type columns to JSON-serializable strings
    for column in post_df.columns:
        if post_df[column].apply(lambda x: isinstance(x, list)).any():
            post_df[column] = post_df[column].apply(json.dumps)

    # Insert the DataFrame row directly into BigQuery
    job = bq_client.load_table_from_dataframe(post_df, table_id)
    job.result()  # Wait for the load job to complete

    if job.errors:
        raise Exception(f"Failed to insert row into BigQuery: {job.errors}")

# Function to delete a post idea from BigQuery
def delete_post_by_caption(caption):
    """
    Delete a post idea from the smp_postideas table based on the caption.

    Args:
        caption (str): The caption of the post to delete.
    """
    query = f"""
        DELETE FROM `bizbuddydemo-v1.strategy_data.smp_postideas`
        WHERE caption = @caption
    """
    query_job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("caption", "STRING", caption)
        ]
    ))
    query_job.result()  # Wait for the query to complete


def fetch_post_data():
    """Fetch post data from BigQuery."""
    query = f"""
        SELECT date, caption, post_type, themes, tone, source
        FROM `{PROJECT_ID}.{ACCOUNT_DATASET_ID}.{IDEAS_TABLE_ID}`
        ORDER BY date ASC
    """
    query_job = bq_client.query(query)
    return query_job.to_dataframe()

def main():
    st.markdown(
        """<h1 style='text-align: center;'>Post Scheduler and Idea Generator</h1>""",
        unsafe_allow_html=True
    )

    # Add functionality to generate and add a post
    if st.button("Add AI Generated Post", key="generate_post_id"):
        with st.spinner("Generating and adding post..."):
            # Load strategy data (placeholder example)
            strategy = {
                "content_plan": [
                    "Testimonials from clients who have improved their performance through your services.",
                    "Short videos or animated infographics explaining different concepts in sports psychology.",
                    "Case studies showing how mental performance can affect sports outcomes.",
                    "Behind-the-scenes content showing what a 1 on 1 session may look like.",
                    "Inspirational quotes about mental resilience and strength.",
                    "Regular Q&A's or AMA (Ask Me Anything) sessions to address common questions or misconceptions about sports psychology."
                ],
                "tone": ["Inspirational", "Educational", "Casual"],
                "post_types": ["Reel", "Story", "Static Post"],
                "past_posts_summary": """Final Summary: This Instagram account primarily focuses on mental performance coaching in sports, offering insights, strategies, and examples of successful athletes who utilize these techniques. Posts often delve into specific mental strategies like visualization, self-talk, positive affirmations, and maintaining focus on the present moment or process rather than the outcome. The account also emphasizes the importance of resilience, confidence, body language, and optimal arousal levels for peak performance. The strategists also discuss the value of reframing negative experiences as learning opportunities and the role of good sleep habits in cognitive function. Teamwork in sports is frequently highlighted, with a focus on football and volleyball. Engagement with followers is encouraged through calls to action, such as following the page or sending direct messages for additional information or inquiries about one-on-one coaching sessions."""
            }

            # Generate a post idea
            post_df = generate_post_idea(strategy)

            # Add the post to BigQuery
            add_post_to_bigquery(post_df)

        st.success("Post successfully added!")

    with st.expander("Manually Add a Post:"):
        manually_add_post()

    # Fetch data from BigQuery
    posts = fetch_post_data()

    # Display posts
    st.subheader("Upcoming Posts")

    for index, row in posts.iterrows():
        with st.expander(f"{row['date']}, {row['post_type']}: {row['caption'][:50]}..."):
            st.markdown(f"**Date:** {row['date']}")
            st.markdown(f"**Caption:** {row['caption']}")
            st.markdown(f"**Post Type:** {row['post_type']}")
            st.markdown(f"**Themes:** {row['themes']}")
            st.markdown(f"**Tone:** {row['tone']}")
            st.markdown(f"**Source:** {row['source']}")
            
            if st.button("Delete Post", key=f"delete_{index}"):
                try:
                    delete_post_by_caption(row['caption'])
                    st.success("Post successfully deleted! Refresh the page to see updates.")
                except Exception as e:
                    st.error(f"Failed to delete post: {e}")

if __name__ == "__main__":
    main()
