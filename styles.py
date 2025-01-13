import streamlit as st

# Add CSS for the metric card
st.markdown(
    """
    <style>
    .metric-card {
        border: 2px solid #4CAF50; /* Green border */
        border-radius: 10px; /* Rounded corners */
        padding: 15px; /* Space inside the card */
        margin: 10px; /* Space around the card */
        background-color: #f9f9f9; /* Light background color */
        height: 120px; /* Fixed height */
        box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1); /* Subtle shadow */
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        text-align: center;
    }
    .metric-card h3 {
        margin: 0;
        font-size: 20px;
        color: #333333; /* Dark text color */
    }
    .metric-card p {
        margin: 5px 0 0 0;
        font-size: 18px;
        color: #4CAF50; /* Match the border color */
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Function to display a metric card
def display_metric(label: str, value: str):
    st.markdown(
        f"""
        <div class="metric-card">
            <h3>{label}</h3>
            <p>{value}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
