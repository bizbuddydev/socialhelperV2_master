from openai import OpenAI
import streamlit as st

st.set_page_config(page_title="Post Brainstormer", layout="wide", page_icon = "💡")

openai_api_key = st.secrets["openai"]["api_key"]

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

st.title("💬BizBuddy Chatbot")
st.caption("🚀 A BizBuddy chatbot that understands your business, powered by OpenAI")
if "messages" not in st.session_state:
    # Prepopulate the chat with a system message containing business context
    st.session_state["messages"] = [
        {
            "role": "system",
            "content": (
                "You are an experienced social media manager working with a small business"
                "They are going to chat with you in here and would like your expertise to help brainstorm"
                "The business is a Sports Psychologist located in Boise Idaho. They work with teams and individuals to  optimize their mental ability in various areas to optimize performance during competition. Their instagram goals are to gain a following around my content which then turns into customers for my business. "
                "They like to post voice over content about different concepts or applications of sports psychology. "
                "Key goals include increasing audience engagement, optimizing post performance, "
                "and improving overall brand visibility. Assume that the user may have questions "
                "about strategy, content planning, analytics, or scheduling."
            ),
        },
        {"role": "assistant", "content": "How can I help you today?"}
    ]

# Display all previous messages, excluding system messages
for msg in st.session_state.messages:
    if msg["role"] != "system":  # Skip displaying system messages
        st.chat_message(msg["role"]).write(msg["content"])

# Handle new user inputs
if prompt := st.chat_input():
    client = OpenAI(api_key=openai_api_key)
    # Append the user's message
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    # Send the full conversation history, including business context, to OpenAI
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=st.session_state.messages
    )
    msg = response.choices[0].message.content
    # Append the assistant's response
    st.session_state.messages.append({"role": "assistant", "content": msg})
    st.chat_message("assistant").write(msg)
