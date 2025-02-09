import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
import openai
import json
import re

st.set_page_config(page_title="Post Scheduler", layout="wide", page_icon = "🗓️")

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

# Set env variables
ACCOUNT_NAME = config["ACCOUNT_NAME"]
PROJECT_ID = config["PROJECT_ID"]
ACCOUNT_DATASET_ID = config["ACCOUNT_DATASET_ID"]
IDEAS_TABLE_ID = config["IDEAS_TABLE_ID"]
PAGE_ID = config["PAGE_ID"]

# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Load BQ Client
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

openai.api_key = st.secrets["openai"]["api_key"]

# Initialize OpenAI client
client = openai


def fetch_latest_date(page_id):
    query = """
        SELECT MAX(date) as latest_date
        FROM `bizbuddydemo-v2.strategy_data.postideas`
        WHERE page_id = @page_id
    """
    
    query_job = bq_client.query(
        query, 
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("page_id", "INT64", page_id)]
        )
    )

    result_df = query_job.to_dataframe()

    # Handle case where there are no posts for the given page_id
    if result_df.empty or pd.isna(result_df.iloc[0]["latest_date"]):
        return pd.Timestamp(datetime.now().date() + timedelta(days=3))  # Return as datetime64[ns]

    latest_date = result_df.iloc[0]["latest_date"]
    return pd.Timestamp(latest_date + timedelta(days=3))

def generate_post_idea(strategy, past_posts, account_inspiration, past_post_ideas, account_insights):
    """
    Generate a single post idea using the provided strategy and additional context.

    Args:
        strategy (dict): A dictionary containing the social media strategy.
        past_posts (str): A formatted string of past posts.
        account_inspiration (str): A formatted string of inspiration for the account.
        past_post_ideas (str): A formatted string of past post concepts.
        account_insights (str): A formatted string of account insights.

    Returns:
        pd.DataFrame: A dataframe containing the generated post idea.
    """
    prompt = (
        f"You are a social media manager creating a post for an Instagram account based on the following context:\n\n"
        f"** Here is their Social Media Strategy:** {strategy}\n\n"
        f"** Here is past posts themes and types. Try to recommend similar ideas but avoid direct overlap:**\n{past_posts}\n\n"
        f"** Here is ideas about post structure / ideas that the user finds inspirational, factor this in:**\n{account_inspiration}\n\n"
        f"** Here are the Past Post Ideas, don't recommend the same things:**\n{past_post_ideas}\n\n"
        f"** Here is some Account Insights about what types of ideas and concepts have worked well for this account in the past. This should be weighted heavily as you decide which post to suggest:**\n{account_insights}\n\n"
        "**Generate 1 new post idea** based on this context. Ensure the idea aligns with the strategy but also introduces a mix of concepts.\n"
        "Each idea should include:\n"
        "- **post_summary** - (e.g., Summarize this post)\n"
        "- **caption**\n"
        "- **post_type** (e.g., Reel, Story, Static Post)\n"
        "- **themes**\n"
        "- **tone**\n"
        "**Output the response as a JSON object** with the **exact** keys: 'post_summary', 'caption', 'post_type', 'themes', 'tone'."
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an experienced social media manager with expertise in creating engaging content."},
            {"role": "user", "content": prompt}
        ]
    )

    idea_json = response.choices[0].message.content.strip()

    # ✅ Extract only the JSON using regex
    json_match = re.search(r"\{.*\}", idea_json, re.DOTALL)
    if json_match:
        idea_json = json_match.group(0)

    # ✅ Validate JSON before loading
    try:
        idea_dict = json.loads(idea_json)  # Convert JSON string to dictionary
        idea_df = pd.DataFrame([idea_dict])  # Convert dictionary to DataFrame
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse AI-generated post idea. Response was not valid JSON.\nError: {e}")
        return pd.DataFrame()  # Return an empty DataFrame to prevent breaking the app

    # ✅ Assign date directly from fetch_latest_date(), ensuring it's datetime64[ns]
    idea_df["date"] = fetch_latest_date(PAGE_ID)
    
    # ✅ Convert 'date' to string if required by BigQuery
    idea_df["date"] = idea_df["date"].astype(str)

    # ✅ Ensure BigQuery-compatible types
    idea_df["source"] = "ChatGPT"
    idea_df["page_id"] = PAGE_ID

    st.write("Final DataFrame Before Uploading:", idea_df.dtypes)  # Debugging: Show data types

    return idea_df


# Get past post ideas
def fetch_past_post_ideas(page_id):
    query = f"""
        SELECT themes, post_type 
        FROM `bizbuddydemo-v2.strategy_data.postideas` 
        WHERE page_id = @page_id 
        ORDER BY date 
        LIMIT 5
    """
    
    query_job = bq_client.query(
        query, 
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("page_id", "INT64", page_id)]
        )
    )
    
    results_df = query_job.to_dataframe()

    if results_df.empty:
        return "No past posts found."

    # Format the output as a numbered list
    past_posts_list = [
        f"{i+1}. {row['themes']} ({row['post_type']})"
        for i, row in results_df.iterrows()
    ]
    
    return "\n".join(past_posts_list)

# Get account inspo
def fetch_account_inspiration(page_id):
    query = f"""
        SELECT post_structure, post_ideas 
        FROM `bizbuddydemo-v2.strategy_data.accountinspiration` 
        WHERE page_id = @page_id 
        ORDER BY update_date 
        LIMIT 1
    """
    
    query_job = bq_client.query(
        query, 
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("page_id", "INT64", page_id)]
        )
    )
    
    results_df = query_job.to_dataframe()

    if results_df.empty:
        return "No inspiration data found."

    post_structure = results_df.iloc[0]["post_structure"]
    post_ideas = results_df.iloc[0]["post_ideas"]

    return f"Post structure: {post_structure}. Post ideas: {post_ideas}."

# Pull insights and past concepts
def fetch_past_post_concepts(page_id):
    query = f"""
        SELECT past_ideas 
        FROM `bizbuddydemo-v2.strategy_data.accountpastconcepts` 
        WHERE page_id = @page_id 
        ORDER BY update_date 
        LIMIT 1
    """
    
    query_job = bq_client.query(
        query, 
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("page_id", "INT64", page_id)]
        )
    )
    
    results_df = query_job.to_dataframe()

    if results_df.empty:
        return "No past post ideas found."

    return results_df.iloc[0]["past_ideas"]

# Get account insights
def fetch_account_insights(page_id):
    query = f"""
        SELECT notes 
        FROM `bizbuddydemo-v2.strategy_data.accountinsights` 
        WHERE page_id = @page_id 
        ORDER BY update_date 
        LIMIT 1
    """
    
    query_job = bq_client.query(
        query, 
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("page_id", "INT64", page_id)]
        )
    )
    
    results_df = query_job.to_dataframe()

    if results_df.empty:
        return "No account insights found."

    return results_df.iloc[0]["notes"]

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

def add_post_to_bigquery(post_df):
    table_id = "bizbuddydemo-v2.strategy_data.postideas"

    # ✅ Ensure 'date' is in string format before uploading
    post_df["date"] = post_df["date"].astype(str)

    # ✅ Convert list-type columns to JSON-serializable strings
    for column in post_df.columns:
        if post_df[column].apply(lambda x: isinstance(x, list)).any():
            post_df[column] = post_df[column].apply(json.dumps)

    # ✅ Debugging check before upload
    print("Data Types Before Upload:\n", post_df.dtypes)
    print("Data Preview:\n", post_df.head())

    # Insert the DataFrame row into BigQuery
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
        DELETE FROM `bizbuddydemo-v2.strategy_data.postideas`
        WHERE caption = @caption
    """
    query_job = bq_client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("caption", "STRING", caption)
        ]
    ))
    query_job.result()  # Wait for the query to complete


def fetch_post_data(page_id):
    """Fetch post data from BigQuery for a specific page ID."""
    query = """
        SELECT date, caption, post_type, themes, tone, source
        FROM `bizbuddydemo-v2.strategy_data.postideas`
        WHERE page_id = @page_id
        ORDER BY date ASC
    """

    query_job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("page_id", "INT64", page_id)]
        ),
    )

    return query_job.to_dataframe()

def main():
    past_post_ideas = fetch_past_post_ideas(PAGE_ID)
    account_inspiration = fetch_account_inspiration(PAGE_ID)
    past_posts = fetch_past_post_concepts(PAGE_ID)
    account_insights = fetch_account_insights(PAGE_ID)
    
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
                    "The content shared on The Harborview's Instagram should incorporate a blend of their services, location, and unique factors. Here are some suggestions: Showcase the hotel rooms and amenities like the gym, pool, and dining areas in high-quality photos or short videos. Highlight the beautiful views of Lake Washington from different points of the hotel. Promote deals and packages available for weekend getaways. Share customer testimonials and stories. Post behind-the-scenes content of the staff preparing for guests. Highlight local attractions and events in Port Washington."
                ],
                "tone": ["Inspirational", "Educational", "Casual"],
                "post_types": ["Reel", "Story", "Static Post"],
            }

            # Generate a post idea
            post_df = generate_post_idea(strategy, past_posts, account_inspiration, past_post_ideas, account_insights)

            # Add the post to BigQuery
            add_post_to_bigquery(post_df)

        st.success("Post successfully added!")

    with st.expander("Manually Add a Post:"):
        manually_add_post()

    # Fetch data from BigQuery
    posts = fetch_post_data(PAGE_ID)

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
