import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import json

st.set_page_config(page_title="Post Analyzer", layout="wide", page_icon="🚝")

# Define links to other pages
PAGES = {
    "📊 Overview": "https://smp-bizbuddyv2-homepage.streamlit.app/",
    "📱 Posts": "https://smp-bizbuddyv2-postoverview.streamlit.app/",
    # "🗓️ Scheduler": "https://smp-bizbuddy-postscheduler.streamlit.app/",
    "📡 Deep Dive": "https://smp-bizbuddyv2-postoverview.streamlit.app/",
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

def main():
    st.title("📱 Post Inspiration Uploader")

    # Step 1: Select content type
    content_type = st.selectbox(
        "What type of content are you uploading?",
        ["Select an option", "Video", "Image", "Article"]
    )

    # Step 2: Ask why they are uploading this before file upload (for Video & Image)
    inspiration_reason, caption_text = None, None

    if content_type in ["Video", "Image"]:
        inspiration_reason = st.radio(
            "Why did you upload this?",
            ["Aesthetic/Post Structure", "Content"]
        )

        # Step 3: Caption or Notes before file upload
        caption_text = st.text_area(
            "Tell us more about this inspiration (e.g., what you like about it, key takeaways):", 
            height=100
        )

        # Step 4: Now allow file upload
        uploaded_file = st.file_uploader(
            f"Upload a {content_type.lower()} file", 
            type=["mp4", "mov", "avi"] if content_type == "Video" else ["png", "jpg", "jpeg"]
        )

        if uploaded_file:
            st.success(f"Uploaded: {uploaded_file.name}")

    elif content_type == "Article":
        article_choice = st.radio("How do you want to add your article?", ["Upload a file", "Paste text"])

        if article_choice == "Upload a file":
            uploaded_file = st.file_uploader("Upload a text file", type=["txt", "pdf", "docx"])
        else:
            article_text = st.text_area("Paste your article text here", height=200)

        if uploaded_file:
            st.success(f"Uploaded: {uploaded_file.name}")
        elif article_text:
            st.success("Text added successfully")

if __name__ == "__main__":
    main()
