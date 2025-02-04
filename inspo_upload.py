import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
import json
import os

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

# Initialize Google Cloud Storage client
storage_client = storage.Client(credentials=credentials)
bucket_name = "bizbuddy-testbucketimg"  # All file types go to the same bucket for testing

def upload_to_gcs(uploaded_file, file_type):
    """Uploads file to Google Cloud Storage and returns the public URL."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(uploaded_file.name)
    
    # Upload file
    blob.upload_from_file(uploaded_file, content_type=uploaded_file.type)
    
    # Make the file publicly accessible (optional)
    blob.make_public()
    
    return blob.public_url

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

    # If a file is uploaded, process upload to GCS
    if uploaded_file:
        file_type = uploaded_file.type.split('/')[0]  # Extract file type (image, video, text)
        st.success(f"Uploaded: {uploaded_file.name}")

if __name__ == "__main__":
    main()
