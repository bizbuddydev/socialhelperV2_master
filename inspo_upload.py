import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import json
import os
from datetime import datetime, timedelta
import uuid

st.set_page_config(page_title="Post Analyzer", layout="wide", page_icon="🚝")

# Define links to other pages
PAGES = {
    "📊 Overview": "https://smp-bizbuddyv2-homepage.streamlit.app/",
    "📱 Posts": "https://smp-bizbuddyv2-postoverview.streamlit.app/",
    "📡 Deep Dive": "https://bizbuddy-postdd-smp.streamlit.app/",
    "🚝 Inspiration Upload": "https://smp-bizbuddyv2-inspoupload.streamlit.app/",
    "💡 Brainstorm": "https://smp-bizbuddy-v1-brainstorm.streamlit.app/"
}

# Sidebar navigation
st.sidebar.title("Navigation")
for page, url in PAGES.items():
    st.sidebar.markdown(f"[**{page}**]({url})", unsafe_allow_html=True)

# Load the account configuration
def load_config(file_path="config.json"):
    with open(file_path, "r") as f:
        return json.load(f)

config = load_config()

# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
project_id = st.secrets["gcp_service_account"]["project_id"]
PAGE_ID = 17841467554159158

# Initialize Google Cloud Storage client
storage_client = storage.Client(credentials=credentials)
bucket_name = "bizbuddyfiles_inspiration"  # All file types go to the same bucket for testing

bq_client = bigquery.Client(credentials=credentials, project=project_id)

def process_article(uploaded_file):
    return "In progress"


def insert_into_bq(page_id, inspiration_context):
    """Inserts a new row into BigQuery and returns the generated video_id."""
    
    # Define your BigQuery table
    table_id = "bizbuddydemo-v2.inspo_data.inpsoextract"

    # Generate a unique video ID
    video_id = str(uuid.uuid4())

    # Get today's date in standard format
    upload_date = datetime.today().strftime('%Y-%m-%d')

    # Create the row to insert
    rows_to_insert = [
        {
            "page_id": page_id,
            "video_id": video_id,
            "inspiration_context": inspiration_context,
            "upload_date": upload_date,
            "file_type": "video"
        }
    ]

    # Insert row into BigQuery
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    
    if errors:
        st.write(f"BigQuery insertion errors: {errors}")
    else:
        st.write(f"Inserted row with video_id: {video_id}")

    return video_id  # Return generated ID to use for file naming
    

def upload_to_gcs(uploaded_file, file_type, page_id, inspiration_context):
    """Uploads file to Google Cloud Storage with a generated video_id and returns a signed URL."""
    bucket = storage_client.bucket(bucket_name)
    
    # Generate a unique ID and insert the data into BigQuery
    video_id = insert_into_bq(page_id, inspiration_context)

    # Extract file extension
    file_ext = uploaded_file.name.split('.')[-1]
    
    # Create new filename (video_id + file extension)
    new_filename = f"{video_id}.{file_ext}"
    
    blob = bucket.blob(new_filename)

    # Upload file
    blob.upload_from_file(uploaded_file, content_type=uploaded_file.type)

    # Generate a signed URL (valid for 24 hours)
    signed_url = blob.generate_signed_url(
        expiration=timedelta(hours=24),  # Change expiration time as needed
        method="GET"
    )

    return signed_url
    

def main():
    st.title("📱 Post Inspiration Uploader")

    # Step 1: Select content type
    content_type = st.selectbox(
        "What type of content are you uploading?",
        ["Select an option", "Video", "Image", "Article"]
    )

    inspiration_reason, caption_text, uploaded_file = None, None, None

    if content_type in ["Video", "Image"]:
        inspiration_reason = st.radio(
            "Why did you upload this?",
            ["Aesthetic/Post Structure", "Content"]
        )

        caption_text = st.text_area(
            "Tell us more about this inspiration (e.g., what you like about it, key takeaways):", 
            height=100
        )

        uploaded_file = st.file_uploader(
            f"Upload a {content_type.lower()} file", 
            type=["mp4", "mov", "avi"] if content_type == "Video" else ["png", "jpg", "jpeg"]
        )

    elif content_type == "Article":
        article_choice = st.radio("How do you want to add your article?", ["Upload a file", "Paste text"])

        if article_choice == "Upload a file":
            uploaded_file = st.file_uploader("Upload a text file", type=["txt", "pdf", "docx"])
        else:
            article_text = st.text_area("Paste your article text here", height=200)

    # If a file is uploaded, process based on type
    if uploaded_file:
        file_type = uploaded_file.type.split('/')[0]  # Extract file type (image, video, text)
    
        if file_type == "video":
            # Get page_id and inspiration context from user input
            page_id = PAGE_ID
            inspiration_context = caption_text
            
            if page_id and inspiration_context:
                public_url = upload_to_gcs(uploaded_file, file_type, page_id, inspiration_context)
                st.success(f"Uploaded Video: {uploaded_file.name}")
                st.markdown(f"[View Video in Cloud Storage]({public_url})")
            else:
                st.warning("Please enter Page ID and Inspiration Context before uploading.")
        
        elif file_type in ["text", "application"]:  # 'application' covers PDFs, Word docs, etc.
            process_article(uploaded_file)
            st.success(f"Article Processed: {uploaded_file.name}")
        
        else:
            st.warning(f"Unsupported file type: {uploaded_file.type}")
            
if __name__ == "__main__":
    main()
