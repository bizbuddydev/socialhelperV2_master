import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from datetime import date, timedelta
import json

st.set_page_config(page_title="Post Analyzer", layout="wide", page_icon="📱")

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

    # Step 2: Show relevant upload option
    uploaded_file = None
    article_text = ""

    if content_type == "Video":
        uploaded_file = st.file_uploader("Upload a video file", type=["mp4", "mov", "avi"])
    elif content_type == "Image":
        uploaded_file = st.file_uploader("Upload an image file", type=["png", "jpg", "jpeg"])
    elif content_type == "Article":
        article_choice = st.radio("How do you want to add your article?", ["Upload a file", "Paste text"])

        if article_choice == "Upload a file":
            uploaded_file = st.file_uploader("Upload a text file", type=["txt", "pdf", "docx"])
        else:
            article_text = st.text_area("Paste your article text here", height=200)

    # Step 3: Display file name or text preview
    if uploaded_file:
        st.success(f"Uploaded: {uploaded_file.name}")
    elif article_text:
        st.success("Text added successfully")
if __name__ == "__main__":
    main()
