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

    # ✅ Convert latest_date to datetime if it's a string
    if isinstance(latest_date, str):
        latest_date = pd.to_datetime(latest_date).date()  # Convert to datetime.date

    return pd.Timestamp(latest_date) + pd.Timedelta(days=3)

def generate_post_idea(strategy, past_posts, account_inspiration, past_post_ideas, account_insights, user_context):
    
    prompt = (
        f"You are a social media manager creating a post for an Instagram account based on the following context:\n\n"
        f"** Here is their Social Media Strategy:** {strategy}\n\n"
        f"** Here is past posts themes and types. Try to recommend similar ideas but avoid direct overlap:**\n{past_posts}\n\n"
        f"** Here is ideas about post structure / ideas that the user finds inspirational, factor this in:**\n{account_inspiration}\n\n"
        f"** Here are the Past Post Ideas, don't recommend the same things:**\n{past_post_ideas}\n\n"
        f"** Here is some Account Insights about what types of ideas and concepts have worked well for this account in the past. This should be weighted heavily as you decide which post to suggest:**\n{account_insights}\n\n"
    )
    
    if user_context:
        prompt += f"**Additional user-provided context for this post:** {user_context}\n\n"
    
    prompt += (
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
        temperature=1.1,  # 🔥 Increased for more variation 
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
    
    return idea_df


def tweak_post_idea(existing_post, user_tweaks):
    """
    Generate a new version of a post idea based on user-provided tweaks.

    Args:
        existing_post (dict): The original post data including 'post_summary', 'caption', 'post_type', 'themes', and 'tone'.
        user_tweaks (str): The modifications the user wants to make to the post.

    Returns:
        dict: A new post idea with the applied tweaks.
    """
    prompt = (
        f"You are a social media manager improving an existing Instagram post based on user feedback.\n\n"
        f"**Here is the original post:**\n"
        f"Post Summary: {existing_post['post_summary']}\n"
        f"Caption: {existing_post['caption']}\n"
        f"Post Type: {existing_post['post_type']}\n"
        f"Themes: {existing_post['themes']}\n"
        f"Tone: {existing_post['tone']}\n\n"
        f"**User Feedback on Changes:** {user_tweaks}\n\n"
        "Please generate an improved version of this post while retaining its core structure. Ensure the updated post aligns with the requested tweaks.\n"
        "Return the output as a JSON object with the exact keys: 'post_summary', 'caption', 'post_type', 'themes', 'tone'."
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=1.0,  # Keeps responses creative but consistent
        messages=[
            {"role": "system", "content": "You are a social media manager refining an Instagram post based on user input."},
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
        new_post = json.loads(idea_json)  # Convert JSON string to dictionary
        return new_post  # Return updated post idea
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse AI-generated post idea. Response was not valid JSON.\nError: {e}")
        return None


# Get past post ideas
def fetch_past_post_ideas(page_id):
    query = f"""
        SELECT post_summary
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
        f"{i+1}. {row['post_summary']}"
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
    post_summary = st.text_area("Post Summary")
    caption = st.text_area("Caption")
    post_type = st.selectbox("Post Type", ["Reel", "Story", "Static Post"])
    themes = st.text_area("Themes (comma-separated)")
    tone = st.text_area("Tone")
    source = "User"
    page_id = PAGE_ID

    if st.button("Add Post", key="manual_add_post"):
        # Create a DataFrame for the new post
        post_df = pd.DataFrame({
            "date": [date],
            "post_summary": [post_summary],
            "caption": [caption],
            "post_type": [post_type],
            "themes": [themes.split(",")],
            "tone": [tone],
            "source": [source],
            "page_id": [page_id]
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

def display_posts_with_tweak_option(posts_df):
    """
    Display each post with a 'Tweak Post' option.

    Args:
        posts_df (pd.DataFrame): DataFrame containing posts with necessary fields.
    """
    for index, row in posts_df.iterrows():
        with st.expander(f"📢 {row['post_summary']}"):
            st.write(f"**Caption:** {row['caption']}")
            st.write(f"**Post Type:** {row['post_type']}")
            st.write(f"**Themes:** {', '.join(row['themes'])}")
            st.write(f"**Tone:** {', '.join(row['tone'])}")

            # Provide tweak option
            user_tweak = st.text_area(f"Enter what you want to tweak about this post (Required)", key=f"tweak_{index}")

            if st.button("Tweak Post", key=f"tweak_button_{index}"):
                if not user_tweak.strip():
                    st.error("You must enter something to tweak before submitting.")
                else:
                    with st.spinner("Updating post..."):
                        updated_post = tweak_post_idea(row.to_dict(), user_tweak)

                        if updated_post:
                            update_post_in_bigquery(row["page_id"], row["caption"], updated_post)


def update_post_in_bigquery(page_id, previous_caption, updated_post):
    """
    Update an existing post in BigQuery based on the previous caption.

    Args:
        page_id (str): The ID of the page where the post belongs.
        previous_caption (str): The caption of the original post (used as an identifier for the update).
        updated_post (dict): The updated post data including 'post_summary', 'caption', 'post_type', 'themes', and 'tone'.
    """
    client = bigquery.Client()

    query = f"""
    UPDATE `your_project.your_dataset.posts`
    SET post_summary = @post_summary,
        caption = @caption,
        post_type = @post_type,
        themes = @themes,
        tone = @tone,
        last_updated = CURRENT_TIMESTAMP()
    WHERE page_id = @page_id AND caption = @previous_caption
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("post_summary", "STRING", updated_post["post_summary"]),
            bigquery.ScalarQueryParameter("caption", "STRING", updated_post["caption"]),
            bigquery.ScalarQueryParameter("post_type", "STRING", updated_post["post_type"]),
            bigquery.ScalarQueryParameter("themes", "STRING", ",".join(updated_post["themes"])),
            bigquery.ScalarQueryParameter("tone", "STRING", ",".join(updated_post["tone"])),
            bigquery.ScalarQueryParameter("page_id", "STRING", page_id),
            bigquery.ScalarQueryParameter("previous_caption", "STRING", previous_caption),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for job to complete

    st.success("Post successfully updated in BigQuery!")

def fetch_post_data(page_id):
    """Fetch post data from BigQuery for a specific page ID."""
    query = """
        SELECT date, post_summary, caption, post_type, themes, tone, source
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

    with st.expander("Add an AI generated Post"):
        # User input for additional context
        user_context = st.text_area("Optional: Add context for this post idea (e.g., seasonal theme, specific campaign focus, etc.)", "")
    
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

                # Generate a post idea with optional user context
                post_df = generate_post_idea(strategy, past_posts, account_inspiration, past_post_ideas, account_insights, user_context)

                # Add the post to BigQuery
                if not post_df.empty:
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
            st.markdown(f"**Post Summary:** {row['post_summary']}")
            st.markdown(f"**Caption:** {row['caption']}")
            st.markdown(f"**Post Type:** {row['post_type']}")
            st.markdown(f"**Themes:** {row['themes']}")
            st.markdown(f"**Tone:** {row['tone']}")
            st.markdown(f"**Source:** {row['source']}")

            col1, col2 = st.columns(2)
            
            # Split update and delete
            with col1:
                # --- NEW: Tweak Post Feature ---
                user_tweak = st.text_area(f"Enter what you want to tweak about this post (Required)", key=f"tweak_{index}")
    
                if st.button("Tweak Post", key=f"tweak_button_{index}"):
                    if not user_tweak.strip():
                        st.error("You must enter something to tweak before submitting.")
                    else:
                        with st.spinner("Updating post..."):
                            updated_post = tweak_post_idea(row.to_dict(), user_tweak)
    
                            if updated_post:
                                update_post_in_bigquery(row["page_id"], row["caption"], updated_post)
                                st.success("Post successfully updated! Refresh the page to see changes.")

            with col2:
                # Delete Post Option
                if st.button("Delete Post", key=f"delete_{index}"):
                    try:
                        delete_post_by_caption(row['caption'])
                        st.success("Post successfully deleted! Refresh the page to see updates.")
                    except Exception as e:
                        st.error(f"Failed to delete post: {e}")

if __name__ == "__main__":
    main()

